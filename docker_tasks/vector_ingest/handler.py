import base64
from argparse import ArgumentParser
import boto3
import os
import ast
import subprocess
import json
import smart_open
from urllib.parse import urlparse
import psycopg2


def download_file(file_uri: str):
    sts = boto3.client("sts")
    response = sts.assume_role(
        RoleArn=os.environ.get("EXTERNAL_ROLE_ARN"),
        RoleSessionName="sts-assume-114506680961",
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


def alter_datetime_add_indexes(collection: str):
    secret_name = os.environ.get("VECTOR_SECRET_NAME")

    con_secrets = get_secret(secret_name)

    conn = psycopg2.connect(
        host=con_secrets["host"],
        dbname=con_secrets["dbname"],
        user=con_secrets["username"],
        password=con_secrets["password"],
    )

    cur = conn.cursor()
    cur.execute(
        f"ALTER table eis_fire_{collection} "
        f"ALTER COLUMN t TYPE TIMESTAMP USING t::timestamp without time zone; "
        f"CREATE INDEX IF NOT EXISTS idx_eis_fire_{collection}_datetime ON eis_fire_{collection}(t);"
    )
    conn.commit()


def load_to_featuresdb(filename: str, collection: str):
    secret_name = os.environ.get("VECTOR_SECRET_NAME")

    con_secrets = get_secret(secret_name)
    connection = get_connection_string(con_secrets)

    print(f"running ogr2ogr import for collection: {collection}")
    if collection in ["snapshot_fireline_nrt", "snapshot_newfirepix_nrt"]:
        # it seems `.fgb`(s) get encoded with a name when written to disk
        # since we are changing the name during an `s3.copy` operation from the algorithm
        # the original name is still needed in `-sql` statements to read the file
        encoded_filename = collection.split("_")[1]
        out = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "PostgreSQL",
                connection,
                "-t_srs",
                "EPSG:4326",
                filename,
                "-nln",
                f"eis_fire_{collection}",
                "-overwrite",
                "-sql",
                f"SELECT fireID, mergeid, t_ed as t from {encoded_filename}",
                "-progress",
            ],
            check=False,
            capture_output=True,
        )
    elif collection == "snapshot_perimeter_nrt":
        out = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "PostgreSQL",
                connection,
                "-t_srs",
                "EPSG:4326",
                filename,
                "-nln",
                f"eis_fire_{collection}",
                "-overwrite",
                "-sql",
                "SELECT n_pixels, n_newpixels, farea, fperim, flinelen, duration, pixden, meanFRP, isactive, t_ed as t, fireID from perimeter",
                "-progress",
            ],
            check=False,
            capture_output=True,
        )
    elif collection in [
        "lf_nfplist_archive",
        "lf_nfplist_nrt",
    ]:
        out = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "PostgreSQL",
                connection,
                "-t_srs",
                "EPSG:4326",
                filename,
                "-nln",
                f"eis_fire_{collection}",
                "-overwrite",
                "-sql",
                f"SELECT x, y, frp, DS, DT, ampm, datetime as t, sat, id as fireID from {collection}",
                "-progress",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    elif collection in [
        "lf_newfirepix_nrt",
        "lf_fireline_nrt",
        "lf_newfirepix_archive",
        "lf_fireline_archive",
    ]:
        out = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "PostgreSQL",
                connection,
                "-t_srs",
                "EPSG:4326",
                filename,
                "-nln",
                f"eis_fire_{collection}",
                "-overwrite",
                "-sql",
                f"SELECT id as fireID, t from {collection}",
                "-progress",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    elif collection in [
        "lf_perimeter_archive",
        "lf_perimeter_nrt",
    ]:
        out = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "PostgreSQL",
                connection,
                "-t_srs",
                "EPSG:4326",
                filename,
                "-nln",
                f"eis_fire_{collection}",
                "-overwrite",
                "-sql",
                f"SELECT n_pixels, n_newpixels, farea, fperim, flinelen, duration, pixden, meanFRP, t, id as fireID from {collection}",
                "-progress",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    else:
        print("Not a valid fireline collection")
        return {"status": "failure"}

    if out.stderr:
        error_description = f"Error: {out.stderr}"
        print(error_description)
        return {"status": "failure", "reason": error_description}

    return {"status": "success"}


def handler(event, context):
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

    payload_event = ast.literal_eval(args.payload)
    s3_event = payload_event.pop("payload")
    with smart_open.open(s3_event, "r") as _file:
        s3_event_read = _file.read()
    event_received = json.loads(s3_event_read)
    s3_objects = event_received["objects"]
    status = list()
    for s3_object in s3_objects:
        href = s3_object["s3_filename"]
        collection = s3_object["collection"]
        downloaded_filepath = download_file(href)
        print(f"[ DOWNLOAD FILEPATH ]: {downloaded_filepath}")
        print(f"[ COLLECTION ]: {collection}")
        coll_status = load_to_featuresdb(downloaded_filepath, collection)
        status.append(coll_status)
        # delete file after ingest
        os.remove(downloaded_filepath)
        if coll_status["status"] == "success":
            alter_datetime_add_indexes(collection)
        else:
            # bubble exception so Airflow shows it as a failure
            raise Exception(coll_status["reason"])
    print(status)


if __name__ == "__main__":
    sample_event = {
        "collection": "eis_fire_newfirepix_2",
        "href": "s3://covid-eo-data/fireline/newfirepix.fgb",
    }
    handler(sample_event, {})
