import base64
import json
import functools
import glob
import os
from sys import argv

import boto3

DATA_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")


sts = boto3.client("sts")
ACCOUNT_ID = sts.get_caller_identity().get("Account")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ENV = os.environ.get("STAGE")
PREFIX = os.environ.get("PREFIX")
MWAA_NAME = f"{PREFIX}-mwaa"
MWAA_ARN = f"arn:aws:airflow:{REGION}:{ACCOUNT_ID}:environment/{MWAA_NAME}"


def arguments():
    if len(argv) <= 1:
        print("No collection provided")
        return
    return argv[1:]


def data_files(data, data_path):
    files = []
    files.extend(glob.glob(os.path.join(data_path, f"{data}*.json")))
    return files


def args_handler(func):
    @functools.wraps(func)
    def prep_args(*args, **kwargs):
        internal_args = arguments()
        func(internal_args)

    return prep_args


def get_secret(secret_name: str) -> None:
    """Retrieve secrets from AWS Secrets Manager

    Args:
        secret_name (str): name of aws secrets manager secret containing database connection secrets
        profile_name (str, optional): optional name of aws profile for use in debugger only

    Returns:
        secrets (dict): decrypted secrets in dict
    """

    # Create a Secrets Manager client
    session = boto3.session.Session(region_name="us-west-2")
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
