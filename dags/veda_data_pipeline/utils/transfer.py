import csv
from io import StringIO
import os
import re

import boto3


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
        except s3_client.exceptions.NoSuchKey:
            target_etag = None

        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=destination_bucket,
            Key=target_key,
            CopySourceIfNoneMatch=target_etag,
        )

def get_inventory_file_content(s3_client, bucket, collection):
    inventory_key = f"veda_collection_inventories/{collection}.csv"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=inventory_key)
        content = response["Body"].read().decode("utf-8")
        return content
    except s3_client.exceptions.NoSuchKey:
        return None

def update_inventory_csv(s3_client, bucket, collection, matching_files_metadata):
    inventory_content = get_inventory_file_content(s3_client, bucket, collection)
    
    # Convert existing CSV content to dictionary for easy updates
    existing_data = {}
    if inventory_content:
        reader = csv.DictReader(StringIO(inventory_content))
        for row in reader:
            existing_data[row["object_key"]] = row

    # Update the dictionary with new data
    for obj_metadata in matching_files_metadata:
        existing_data[obj_metadata["Key"]] = {
            "last_modified": obj_metadata["LastModified"].strftime('%Y-%m-%d %H:%M:%S'),
            "etag": obj_metadata["ETag"].replace('"', ''),
            "object_key": obj_metadata["Key"]
        }

    # Convert dictionary back to CSV
    output = StringIO()
    fieldnames = ["last_modified", "etag", "object_key"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for item in existing_data.values():
        writer.writerow(item)
    
    # Upload updated CSV to S3
    s3_client.put_object(Body=output.getvalue(), Bucket=bucket, Key=f"veda_collection_inventories/{collection}.csv")

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

    matching_files_metadata = [s3client.head_object(Bucket=origin_bucket, Key=file_key) for file_key in matching_files]
    update_inventory_csv(s3client, origin_bucket, collection, matching_files_metadata)
    
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
