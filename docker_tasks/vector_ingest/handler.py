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

# Just to test locally
def download_file2(file_uri: str):
    s3 = boto3.client("s3")


    url_parse = urlparse(file_uri)
    print("url_parsed: ", url_parse)

    bucket = url_parse.netloc
    path = url_parse.path[1:]
    filename = url_parse.path.split("/")[-1]
    print(bucket, path, filename)
    target_filepath = os.path.join("/tmp", filename)

    s3.download_file(bucket, path, target_filepath)

    print(f"downloaded {target_filepath}")

    s3.close()
    return target_filepath


def get_connection_string(secret: dict, as_uri: bool = False) -> str:
    if as_uri:
        return f"postgresql://{secret['username']}:{secret['password']}@{secret['host']}:5432/{secret['dbname']}"
    else:
        #return f"PG:host=localhost port=5432 dbname=postgis user=username password=password"
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



def load_to_featuresdb(
    filename: str,
    collection: str,
    x_possible: str = "longitude",
    y_possible: str = "latitude",
    source_projection : str ="EPSG:4326",
    target_projection : str ="EPSG:4326"
):
    secret_name = os.environ.get("VECTOR_SECRET_NAME")
    con_secrets = get_secret(secret_name)
    connection = get_connection_string(con_secrets)

    print(f"running ogr2ogr import for collection: {collection}")
    options = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        connection,
        filename,
        "-oo",
        f"X_POSSIBLE_NAMES={x_possible}",
        "-oo",
        f"Y_POSSIBLE_NAMES={y_possible}",
        "-nln",
        collection, # Or could be the actual filename 
        "-s_srs",
        source_projection,
        "-t_srs",
        target_projection,
        "-overwrite"
    ]
    out = subprocess.run(
        options,
        check=False,
        capture_output=True,
    )
    #print("db connection ", options)

    if out.stderr:
        error_description = f"Error: {out.stderr}"
        print(error_description)
        return {"status": "failure", "reason": error_description}

    return {"status": "success"}



def handler(event, context):
    print("Generic Vector ingest started")
    parser = ArgumentParser(
        prog="generic_vector_ingest",
        description="Ingest Vector- Generic",
        epilog="Running the code as ECS task",
    )
    parser.add_argument(
        "--payload", dest="payload", help="event passed to stac_handler function"
    )
    args = parser.parse_args()

    payload_event = ast.literal_eval(args.payload)
    print("*********** payload", payload_event)
    s3_event = payload_event.pop("payload")
    # These will be later extracted from the json file. Need to see if the json file put x_possible in the json file after the dag is triggered with x_possibel in it
    x_possible = payload_event["x_possible"]
    y_possible = payload_event["y_possible"]
    source_projection = payload_event["source_projection"]
    target_projection = payload_event["target_projection"]

    # extract the actual link of the json file and read
    with smart_open.open(s3_event, "r") as _file:
        s3_event_read = _file.read()
    print("file read done")
    event_received = json.loads(s3_event_read)
    s3_objects = event_received["objects"]
    status = list()
    for s3_object in s3_objects:
        href = s3_object["assets"]["default"]["href"] #s3://ghgc-data-store-develop/transformed_csv/NIST_Urban_Testbed/NEB-ch4.csv
        #collection = s3_object["collection"]
        #collection = href.split("/")[-1].split(".")[0]
        # or it could be 
        collection = href.split("/")[-2] + '_' + href.split("/")[-1].split(".")[0]

        downloaded_filepath = download_file(href)
        print("-----------------------------------------------------\n")
        print(f"[ DOWNLOAD FILEPATH ]: {downloaded_filepath}")
        print(f"[ COLLECTION ]: {collection}")
        coll_status = load_to_featuresdb(downloaded_filepath, collection, x_possible, y_possible, source_projection, target_projection)
        status.append(coll_status)

        # delete file after ingest
        os.remove(downloaded_filepath)

        # Not sure if we need it
        # if coll_status["status"] == "success":
        #     alter_datetime_add_indexes(collection)
        # else:
        #     # bubble exception so Airflow shows it as a failure
        #     raise Exception(coll_status["reason"])
    print("\n **********Overall Status*********\n", f"Done for {len(status)} csv files",status)


if __name__ == "__main__":
    # It has nothing to do
    sample_event = {
        "collection": "eis_fire_newfirepix_2",
        "href": "s3://covid-eo-data/fireline/newfirepix.fgb",
    }
    handler(sample_event, {})
