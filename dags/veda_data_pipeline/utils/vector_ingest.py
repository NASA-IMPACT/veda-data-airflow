import base64
import boto3
import os
import subprocess
import json
import smart_open
from urllib.parse import urlparse


def download_file(file_uri: str):
    """Downloads file from s3

    Args:
        file_uri (str): s3 URL of the file to be downloaded

    Returns:
        target_filepath (str): filepath of the downloaded file
    """
    role_arn = os.environ.get("EXTERNAL_ROLE_ARN")
    kwargs = assume_role(role_arn=role_arn) if role_arn else {}

    s3 = boto3.client("s3", **kwargs)
    url_parse = urlparse(file_uri)

    bucket = url_parse.netloc
    path = url_parse.path[1:]
    filename = url_parse.path.split("/")[-1]
    target_filepath = os.path.join("/tmp", filename)

    s3.download_file(bucket, path, target_filepath)

    s3.close()
    return target_filepath


def assume_role(role_arn, session_name="veda-data-pipelines_vector-ingest"):
    """Assumes an AWS IAM role and returns temporary credentials.

    Args:
        role_arn (str): The ARN of the role to assume.
        session_name (str): A name for the assumed session.

    Returns:
        dict: Temporary AWS credentials.
    """
    sts = boto3.client("sts")
    credentials = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
    )
    creds = credentials["Credentials"]
    return {
        "aws_access_key_id": creds["AccessKeyId"],
        "aws_secret_access_key": creds.get("SecretAccessKey"),
        "aws_session_token": creds.get("SessionToken"),
    }


def get_connection_string(secret: dict, as_uri: bool = False) -> str:
    if as_uri:
        return f"postgresql://{secret['username']}:{secret['password']}@{secret['host']}:5432/{secret['dbname']}"
    else:
        # return f"PG:host=localhost port=5432 dbname=postgis user=username password=password"
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
        secret_name: str,
        filename: str,
        layer_name: str,
        x_possible: str = "longitude",
        y_possible: str = "latitude",
        source_projection: str = "EPSG:4326",
        target_projection: str = "EPSG:4326",
        extra_flags: list = ["-overwrite", "-progress"]
):
    con_secrets = get_secret(secret_name)
    connection = get_connection_string(con_secrets)

    print(f"running ogr2ogr import for collection/file: {layer_name}")
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
        layer_name,
        "-s_srs",
        source_projection,
        "-t_srs",
        target_projection,
        *extra_flags
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


def handler(secret_name, payload_event):
    print("------Vector ingestion for Features API started------")

    s3_event = payload_event.pop("payload")
    # Extracting configs for ingestion
    x_possible = payload_event["x_possible"]
    y_possible = payload_event["y_possible"]
    source_projection = payload_event["source_projection"]
    target_projection = payload_event["target_projection"]
    extra_flags = payload_event["extra_flags"]

    layer_name = payload_event["collection"]
    collection_not_provided = layer_name == ""

    # Read the json to extract the discovered file paths
    with smart_open.open(s3_event, "r") as _file:
        s3_event_read = _file.read()

    event_received = json.loads(s3_event_read)
    s3_objects = event_received["objects"]
    status = list()

    for s3_object in s3_objects:
        href = s3_object["assets"]["default"]["href"]
        filename = href.split("/")[-1].split(".")[0]

        # Use id template when collection is not provided in the conf
        if collection_not_provided:
            layer_name = payload_event.get("id_template", "{}").format(filename)

        downloaded_filepath = download_file(href)
        print(f"[ COLLECTION ]: {layer_name}, [ DOWNLOAD FILEPATH ]: {downloaded_filepath}")

        coll_status = load_to_featuresdb(secret_name, downloaded_filepath, layer_name,
                                         x_possible, y_possible,
                                         source_projection, target_projection,
                                         extra_flags)
        status.append(coll_status)

        # Delete file after ingest
        os.remove(downloaded_filepath)

        if coll_status["status"] != "success":
            # Bubble exception so Airflow shows it as a failure
            raise Exception(coll_status["reason"])

    print("------Overall Status------\n", f"Done for {len(status)} discovered files\n", status)
    return status

