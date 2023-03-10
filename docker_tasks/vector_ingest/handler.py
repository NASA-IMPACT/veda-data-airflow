import time

import boto3
import os
import subprocess
import json
from urllib.parse import urlparse

s3 = boto3.client(
    "s3",
)


def download_file(file_uri: str):

    url_parse = urlparse(file_uri)

    bucket = url_parse.netloc
    path = url_parse.path[1:]
    filename = url_parse.path.split("/")[-1]
    target_filepath = os.path.join("/tmp", filename)

    s3.download_file(bucket, path, target_filepath)

    print(f"downloaded {target_filepath}")

    return target_filepath


def get_connection_string(secret: dict) -> str:
    return f"PG:host={secret['host']} dbname={secret['dbname']} user={secret['username']} password={secret['password']}"


def get_secret(secret_name: str) -> None:
    """Retrieve secrets from AWS Secrets Manager

    Args:
        secret_name (str): name of aws secrets manager secret containing database connection secrets
        profile_name (str, optional): optional name of aws profile for use in debugger only

    Returns:
        secrets (dict): decrypted secrets in dict
    """

    # Create a Secrets Manager client
    session = boto3.session.Session(region_name="us-west-1")
    client = session.client(service_name="secretsmanager")

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    # Decrypts secret using the associated KMS key.
    # Depending on whether the secret is a string or binary, one of these fields will be populated.
    if "SecretString" in get_secret_value_response:
        return json.loads(json.loads(get_secret_value_response["SecretString"]))
    else:
        return json.loads(base64.b64decode(get_secret_value_response["SecretBinary"]))


def load_to_featuresdb(filename: str, collection: str):
    secret_name = os.environ.get("VECTOR_SECRET_NAME")

    con_secrets = get_secret(secret_name)
    connection = get_connection_string(con_secrets)

    print(f"running ogr2ogr import for collection: {collection}")

    subprocess.run(
        [
            "ogr2ogr",
            "-f",
            "PostgreSQL",
            connection,
            "-t_srs",
            "EPSG:4326",
            filename,
            "-nln",
            collection,
            "-append",
            "-update",
            "-progress",
        ]
    )


def handler(event, context):
    print("Statr vector")
    time.sleep(30)
    print("wakeup vector")
    href = event["s3_filename"]

    collection = event["collection"]

    downloaded_filepath = download_file(href)

    load_to_featuresdb(downloaded_filepath, collection)


if __name__ == "__main__":
    sample_event = {
        "collection": "eis_fire_newfirepix_2",
        "href": "s3://covid-eo-data/fireline/newfirepix.fgb",
    }
    handler(sample_event, {})
