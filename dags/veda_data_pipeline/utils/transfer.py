import os
import re

import boto3
from botocore.exceptions import ClientError


def assume_role(role_arn, session_name="veda-data-airflow_s3-discovery"):
    sts = boto3.client("sts")
    print(f"Assuming role: {role_arn}")
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


def transfer_files_within_s3(
    s3_client, origin_bucket, matching_files, destination_bucket, collection
):
    for file_key in matching_files:
        filename = file_key.split("/")[-1]
        target_key = f"{collection}/{filename}"
        copy_source = {"Bucket": origin_bucket, "Key": file_key}

        # We can use the etag to check if the file has already been copied and avoid duplication of effort
        # by using the CopySourceIfNoneMatch parameter below.
        try:
            target_metadata = s3_client.head_object(
                Bucket=destination_bucket, Key=target_key
            )
            target_etag = target_metadata["ETag"]
        except (s3_client.exceptions.NoSuchKey, ClientError):
            target_etag = ""

        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=destination_bucket,
            Key=target_key,
            CopySourceIfNoneMatch=target_etag,
        )


def data_transfer_handler(event, role_arn=None, bucket_output=None):
    origin_bucket = event.get("origin_bucket")
    origin_prefix = event.get("origin_prefix")
    filename_regex = event.get("filename_regex")
    target_bucket = event.get("target_bucket")
    collection = event.get("collection")

    role_arn = os.environ.get("ASSUME_ROLE_ARN", role_arn)
    kwargs = assume_role(role_arn=role_arn) if role_arn else {}
    s3client = boto3.client("s3", **kwargs)
    matching_files = get_matching_files(
        s3_client=s3client,
        bucket=origin_bucket,
        prefix=origin_prefix,
        regex_pattern=filename_regex,
    )
    if not event.get("dry_run"):
        transfer_files_within_s3(
            s3_client=s3client,
            origin_bucket=origin_bucket,
            matching_files=matching_files,
            destination_bucket=target_bucket,
            collection=collection,
        )
    else:
        print(
            f"Would have copied {len(matching_files)} files from {origin_bucket} to {target_bucket}"
        )
        print(f"Files matched: {matching_files}")

    return {**event}
