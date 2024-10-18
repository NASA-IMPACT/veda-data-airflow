import base64
from argparse import ArgumentParser
import boto3
import os
import subprocess
import json
import smart_open
from urllib.parse import urlparse
import psycopg2
import geopandas as gpd
from shapely import wkb
from geoalchemy2 import Geometry
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, inspect
import concurrent.futures
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, INTEGER, VARCHAR, TIMESTAMP


def download_file(file_uri: str):
    sts = boto3.client("sts")
    print(f'Assuming role: {os.environ.get("EXTERNAL_ROLE_ARN")}')
    role_arn = os.environ.get("EXTERNAL_ROLE_ARN")
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName="airflow_vector_ingest",
    )
    new_session = boto3.Session(
        aws_access_key_id=response["Credentials"]["AccessKeyId"],
        aws_secret_access_key=response["Credentials"]["SecretAccessKey"],
        aws_session_token=response["Credentials"]["SessionToken"],
    )
    s3 = new_session.client("s3")

    url_parse = urlparse(file_uri)

    bucket = url_parse.netloc
    path = url_parse.path[1:]
    filename = url_parse.path.split("/")[-1]
    target_filepath = os.path.join("/tmp", filename)

    s3.download_file(bucket, path, target_filepath)

    print(f"downloaded {target_filepath}")

    sts.close()
    return target_filepath


def get_connection_string(secret: dict, as_uri: bool = False) -> str:
    if as_uri:
        return f"postgresql://{secret['username']}:{secret['password']}@{secret['host']}:5432/{secret['dbname']}"
    else:
        return f"PG:host={secret['host']} dbname={secret['dbname']} user={secret['username']} password={secret['password']}"


def get_gdf_schema(gdf, target_projection):
    """map GeoDataFrame columns into a table schema

    :param gdf:  GeoDataFrame from geopandas
    :param target_projection: srid for the target table geometry column
    :return:
    """
    # map geodatafrome dtypes to sqlalchemy types
    dtype_map = {
        "int64": INTEGER,
        "float64": DOUBLE_PRECISION,
        "object": VARCHAR,
        "datetime64": TIMESTAMP,
    }
    schema = []
    for column, dtype in zip(gdf.columns, gdf.dtypes):
        if str(dtype) == "geometry":
            # do not inpsect to retrieve geom type, just use generic GEOMETRY
            # geom_type = str(gdf[column].geom_type.unique()[0]).upper()
            geom_type = str(dtype).upper()
            # do not taKe SRID from existing file for target table
            # we always want to transform from file EPSG to Table EPSG(<target_projection>)
            column_type = Geometry(geometry_type=geom_type, srid=target_projection)
        else:
            dtype_str = str(dtype)
            column_type = dtype_map.get(dtype_str.split("[")[0], VARCHAR)

        if column == "primarykey":
            schema.append(Column(column.lower(), column_type, unique=True))
        else:
            schema.append(Column(column.lower(), column_type))
    return schema


def ensure_table_exists(
    db_metadata: MetaData, gpkg_file: str, target_projection: int, table_name: str
):
    """create a table if it doesn't exist or just
    validate GeoDataFrame columns against existing table

    :param db_metadata: instance of sqlalchemy.MetaData
    :param gpkg_file: file path to GPKG
    :param target_projection: srid for target DB table geometry column
    :param table_name: name of table to create
    :return: None
    """
    gdf = gpd.read_file(gpkg_file)
    gdf_schema = get_gdf_schema(gdf, target_projection)
    engine = db_metadata.bind
    try:
        Table(table_name, db_metadata, autoload_with=engine)
    except sqlalchemy.exc.NoSuchTableError:
        Table(table_name, db_metadata, *gdf_schema)
        db_metadata.create_all(engine)

    # validate gdf schema against existing table schema
    insp = inspect(engine)
    existing_columns = insp.get_columns(table_name)
    existing_column_names = [col["name"] for col in existing_columns]
    for column in gdf_schema:
        if column.name not in existing_column_names:
            raise ValueError(
                f"your .gpkg seems to have a column={column.name} that does not exist in the existing table columns={existing_column_names}"
            )


def delete_region(
    engine,
    gpkg_path: str,
    table_name: str,
):
    gdf = gpd.read_file(gpkg_path)
    if 'region' in gdf.columns:
        region_name = gdf["region"].iloc[0]
        with engine.connect() as conn:
            with conn.begin():
                delete_sql = sqlalchemy.text(
                    f"""
                    DELETE FROM {table_name} WHERE region=:region_name
                    """
                )
                conn.execute(delete_sql, {'region_name': region_name})
    else:
        print(f"'region' column not found in {gpkg_path}. No records deleted.")


def upsert_to_postgis(
    engine,
    gpkg_path: str,
    target_projection: int,
    table_name: str,
    batch_size: int = 10000,
):
    """batch the GPKG file and upsert via threads

    :param engine: instance of sqlalchemy.Engine
    :param gpkg_path: file path to GPKG
    :param table_name: name of the target table
    :param batch_size: upper limit of batch size
    :return:
    """
    gdf = gpd.read_file(gpkg_path)
    source_epsg_code = gdf.crs.to_epsg()
    if not source_epsg_code:
        # assume NAD27 Equal Area for now :shrug:
        # since that's what the default is for Fire Atlas team exports
        # that's what PROJ4 does under the hood for 9311 :wethinksmirk:
        source_epsg_code = 2163

    # convert the `t` column to something suitable for sql insertion otherwise we get 'Timestamp(<value>)'
    gdf["t"] = gdf["t"].dt.strftime("%Y-%m-%d %H:%M:%S")
    # convert to WKB
    gdf["geometry"] = gdf["geometry"].apply(lambda geom: wkb.dumps(geom, hex=True))

    def upsert_batch(batch):
        with engine.connect() as conn:
            with conn.begin():
                for row in batch.to_dict(orient="records"):
                    # make sure all column names are lower case for keys and values
                    row = {k.lower(): v for k, v in row.items()}
                    columns = [col.lower() for col in batch.columns]

                    non_geom_placeholders = ", ".join(
                        [f":{col}" for col in columns[:-1]]
                    )
                    # NOTE: we need to escape `::geometry` so parameterized statements don't try to replace it
                    # because parametrized statements in sqlalchemy are `:<variable-name>`
                    geom_placeholder = f"ST_Transform(ST_SetSRID(ST_GeomFromWKB(:geometry\:\:geometry), {source_epsg_code}), {target_projection})"  # noqa: W605
                    upsert_sql = sqlalchemy.text(
                        f"""
                            INSERT INTO {table_name} ({', '.join([col for col in columns])})
                            VALUES ({non_geom_placeholders},{geom_placeholder})
                            ON CONFLICT (primarykey)
                            DO UPDATE SET {', '.join(f"{col}=EXCLUDED.{col}" for col in columns if col != 'primarykey')}
                        """
                    )

                    # logging.debug(f"[ UPSERT SQL ]:\n{str(upsert_sql)}")
                    conn.execute(upsert_sql, row)

    batches = [gdf.iloc[i : i + batch_size] for i in range(0, len(gdf), batch_size)]
    # set `max_workers` to something below max concurrent connections for postgresql
    # https://www.postgresql.org/docs/14/runtime-config-connection.html
    with concurrent.futures.ThreadPoolExecutor(max_workers=75) as executor:
        executor.map(upsert_batch, batches)


def get_secret(secret_name: str) -> None:
    """Retrieve secrets from AWS Secrets Manager

    Args:
        secret_name (str): name of aws secrets manager secret containing database connection secrets

    Returns:
        secrets (dict): decrypted secrets in dict
    """

    # Create a Secrets Manager client
    session = boto3.session.Session(region_name=os.environ.get("AWS_REGION"))
    client = session.client(service_name="secretsmanager")

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    # Decrypts secret using the associated KMS key.
    # Depending on whether the secret is a string or binary, one of these fields will be populated.
    if "SecretString" in get_secret_value_response:
        return json.loads(get_secret_value_response["SecretString"])
    else:
        return json.loads(base64.b64decode(get_secret_value_response["SecretBinary"]))


def load_to_featuresdb(
    filename: str,
    collection: str,
    extra_flags: list = None,
    target_projection: str = "EPSG:4326",
):
    if extra_flags is None:
        extra_flags = ["-overwrite", "-progress"]

    secret_name = os.environ.get("VECTOR_SECRET_NAME")

    con_secrets = get_secret(secret_name)
    connection = get_connection_string(con_secrets)

    print(f"running ogr2ogr import for collection: {collection}")
    options = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        connection,
        "-t_srs",
        target_projection,
        filename,
        "-nln",
        collection,
        *extra_flags,
    ]
    out = subprocess.run(
        options,
        check=False,
        capture_output=True,
    )

    if out.stderr:
        error_description = f"Error: {out.stderr}"
        print(error_description)
        return {"status": "failure", "reason": error_description}

    return {"status": "success"}


def load_to_featuresdb_eis(
    filename: str,
    collection: str,
    target_projection: int = 4326,
):
    """create table if not exists and upload GPKG

    :param filename: the file path to the downloaded GPKG
    :param collection: the name of the collection
    :param target_projection: srid for the target table
    :return: None
    """
    secret_name = os.environ.get("VECTOR_SECRET_NAME")
    conn_secrets = get_secret(secret_name)
    connection_string = get_connection_string(conn_secrets, as_uri=True)

    # NOTE: about `collection.rsplit` below:
    #
    # EIS Fire team naming convention for outputs
    #     Snapshots: "snapshot_{layer_name}_nrt_{region_name}.gpkg"
    #     Lf_archive: "lf_{layer_name}_archive_{region_name}.gpkg"
    #     Lf_nrt: "lf_{layer_name}_nrt_{region_name}.gpkg"
    #
    # Insert/Alter on table call everything except the region name:
    #     e.g. `snapshot_perimeter_nrt_conus` this gets inserted into the table `eis_fire_snapshot_perimeter_nrt`
    collection = collection.rsplit("_", 1)[0]
    target_table_name = f"eis_fire_{collection}"

    engine = create_engine(connection_string)
    metadata = MetaData()
    metadata.bind = engine

    ensure_table_exists(metadata, filename, target_projection, target_table_name)
    delete_region(engine, filename, target_table_name)
    upsert_to_postgis(engine, filename, target_projection, target_table_name)
    return {"status": "success"}


def alter_datetime_add_indexes_eis(collection: str):
    # NOTE: about `collection.rsplit` below:
    #
    # EIS Fire team naming convention for outputs
    #     Snapshots: "snapshot_{layer_name}_nrt_{region_name}.gpkg"
    #     Lf_archive: "lf_{layer_name}_archive_{region_name}.gpkg"
    #     Lf_nrt: "lf_{layer_name}_nrt_{region_name}.gpkg"
    #
    # Insert/Alter on table call everything except the region name:
    #     e.g. `snapshot_perimeter_nrt_conus` this gets inserted into the table `eis_fire_snapshot_perimeter_nrt`
    collection = collection.rsplit("_", 1)[0]

    secret_name = os.environ.get("VECTOR_SECRET_NAME")
    conn_secrets = get_secret(secret_name)
    conn = psycopg2.connect(
        host=conn_secrets["host"],
        dbname=conn_secrets["dbname"],
        user=conn_secrets["username"],
        password=conn_secrets["password"],
    )

    cur = conn.cursor()
    cur.execute(
        f"ALTER table eis_fire_{collection} "
        f"ALTER COLUMN t TYPE TIMESTAMP USING t::timestamp without time zone; "
        f"CREATE INDEX IF NOT EXISTS idx_eis_fire_{collection}_datetime ON eis_fire_{collection}(t);"
        f"CREATE INDEX IF NOT EXISTS idx_eis_fire_{collection}_primarykey ON eis_fire_{collection}(primarykey);"
        f"CREATE INDEX IF NOT EXISTS idx_eis_fire_{collection}_region ON eis_fire_{collection}(region);"
    )
    conn.commit()


def handler():
    print("Vector ingest started")
    parser = ArgumentParser(
        prog="vector_ingest",
        description="Ingest Vector",
        epilog="Running the code as ECS task",
    )
    parser.add_argument(
        "--payload", dest="payload", help="event passed to stac_handler function"
    )
    args = parser.parse_args()

    payload_event = json.loads(args.payload)
    s3_event = payload_event.pop("payload")
    with smart_open.open(s3_event, "r") as _file:
        s3_event_read = _file.read()
    event_received = json.loads(s3_event_read)
    s3_objects = event_received["objects"]
    status = list()
    for s3_object in s3_objects:
        href = s3_object["assets"]["default"]["href"]
        collection = s3_object["collection"]
        downloaded_filepath = download_file(href)
        print(f"[ DOWNLOAD FILEPATH ]: {downloaded_filepath}")
        print(f"[ COLLECTION ]: {collection}")

        s3_object_prefix = event_received["prefix"]
        if s3_object_prefix.startswith("EIS/"):
            coll_status = load_to_featuresdb_eis(downloaded_filepath, collection)
        else:
            coll_status = load_to_featuresdb(downloaded_filepath, collection)

        status.append(coll_status)
        # delete file after ingest
        os.remove(downloaded_filepath)

        if coll_status["status"] == "success" and s3_object_prefix.startswith("EIS/"):
            alter_datetime_add_indexes_eis(collection)
        elif coll_status["status"] != "success":
            # bubble exception so Airflow shows it as a failure
            raise Exception(coll_status["reason"])
    print(status)


if __name__ == "__main__":
    handler()
