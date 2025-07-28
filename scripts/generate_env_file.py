import boto3
import json
from argparse import ArgumentParser


def get_secrets_as_env(secret_id, out_file):
    sm_client = boto3.client("secretsmanager")
    response = sm_client.get_secret_value(SecretId=secret_id)
    secrets = json.loads(response["SecretString"])
    with open(out_file, "w") as _env:
        for out_key in secrets:
            out_value = secrets[out_key]
            _env.write(f"{out_key}={out_value}\n")


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Generate_env_file",
        description="Generate dot env file for deployment",
        epilog="Contact Marouane for extra help",
    )
    parser.add_argument(
        "--secret-id",
        dest="secret_id",
        help="AWS secret id",
        required=True,
    )
    parser.add_argument(
        "--env-file",
        dest="env_file",
        help=".env file to write to",
        required=False,
        default=".env",
    )

    args = parser.parse_args()

    get_secrets_as_env(secret_id=args.secret_id, out_file=args.env_file)
