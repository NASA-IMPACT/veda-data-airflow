import os
import re
import tempfile

import boto3
from rio_cogeo.cogeo import cog_translate


def assume_role(role_arn, session_name="veda-airflow-pipelines_transfer_files"):
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


def get_matching_files(s3_client, bucket, prefix, regex_pattern):
    matching_files = []

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    while True:
        for obj in response["Contents"]:
            file_key = obj["Key"]
            if re.match(regex_pattern, file_key):
                matching_files.append(file_key)

        if "NextContinuationToken" in response:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=response["NextContinuationToken"],
            )
        else:
            break

    return matching_files


def transfer_file(s3_client, file_key, local_file_path, destination_bucket, collection):
    filename = file_key.split("/")[-1]
    target_key = f"{collection}/{filename}"
    s3_client.upload_file(local_file_path, destination_bucket, target_key)


def cogify_transfer_handler(event, context):
    kwargs = {}
    if external_role_arn := os.environ["EXTERNAL_ROLE_ARN"]:
        creds = assume_role(external_role_arn, "veda-data-pipelines_data-transfer")
        kwargs = {
            "aws_access_key_id": creds["AccessKeyId"],
            "aws_secret_access_key": creds["SecretAccessKey"],
            "aws_session_token": creds["SessionToken"],
        }
    source_s3 = boto3.client("s3")
    target_s3 = boto3.client("s3", **kwargs)

    origin_bucket = event.get("origin_bucket")
    origin_prefix = event.get("origin_prefix")
    regex_pattern = event.get("filename_regex")
    target_bucket = event.get("target_bucket", "veda-data-store-staging")
    collection = event.get("collection")

    matching_files = get_matching_files(
        source_s3, origin_bucket, origin_prefix, regex_pattern
    )
    if not event.get("dry_run"):
        for origin_key in matching_files:
            with tempfile.NamedTemporaryFile() as local_tif, tempfile.NamedTemporaryFile() as local_cog:
                local_tif_path = local_tif.name
                local_cog_path = local_cog.name
                source_s3.download_file(origin_bucket, origin_key, local_tif_path)
                cog_translate(local_tif_path, local_cog_path, quiet=True)
                filename = origin_key.split("/")[-1]
                destination_key = f"{collection}/{filename}"
                target_s3.upload_file(local_cog_path, target_bucket, destination_key)
    else:
        print(
            f"Would have copied {len(matching_files)} files from {origin_bucket} to {target_bucket}"
        )
        print(f"Files matched: {matching_files}")
