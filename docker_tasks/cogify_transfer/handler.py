import ast
import json
import os
import re
import tempfile
from argparse import ArgumentParser
from time import sleep, time

import boto3
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles


def assume_role(role_arn, session_name="veda-airflow-pipelines_transfer_files"):
    sts = boto3.client("sts")
    credentials = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
    )
    return credentials["Credentials"]


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


def cogify_transfer_handler(event):
    origin_bucket = event.get("origin_bucket")
    origin_prefix = event.get("origin_prefix")
    regex_pattern = event.get("filename_regex")
    target_bucket = event.get("target_bucket", "veda-data-store-staging")
    collection = event.get("collection")
    cog_profile = event.get("cog_profile", "deflate")
    dry_run = event.get("dry_run", False)

    source_s3 = boto3.client("s3")
    if target_bucket == "veda-data-store-staging":
        external_role_arn = os.environ["EXTERNAL_ROLE_ARN"]
        creds = assume_role(external_role_arn, "veda-data-pipelines_data-transfer")
        kwargs = {
            "aws_access_key_id": creds["AccessKeyId"],
            "aws_secret_access_key": creds["SecretAccessKey"],
            "aws_session_token": creds["SessionToken"],
        }
        target_s3 = boto3.client("s3", **kwargs)
    else:
        target_s3 = boto3.client("s3")

    dst_profile = cog_profiles.get(cog_profile)

    matching_files = get_matching_files(
        source_s3, origin_bucket, origin_prefix, regex_pattern
    )
    if not dry_run:
        for origin_key in matching_files:
            with tempfile.NamedTemporaryFile(delete=False) as local_tif, tempfile.NamedTemporaryFile(delete=False) as local_cog:
                local_tif_path = local_tif.name
                local_cog_path = local_cog.name
                source_s3.download_file(origin_bucket, origin_key, local_tif_path)
                local_tif.close()
                cog_translate(local_tif_path, local_cog_path, dst_profile, quiet=True)
                local_cog.close()
                filename = origin_key.split("/")[-1]
                destination_key = f"{collection}/{filename}"
                target_s3.upload_file(local_cog_path, target_bucket, destination_key)

            # Manually delete the temporary files
            os.remove(local_tif_path)
            os.remove(local_cog_path)

    return {"matching_files": matching_files, "dry_run": dry_run}


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="cogify_transfer",
        description="Cogify and transfer files on S3",
    )
    parser.add_argument(
        "--payload", dest="payload", help="event passed to stac_handler function"
    )
    args = parser.parse_args()
    # For cloud watch log to work the task should stay alive for at least 30 s
    start = time()
    print(f"Start at {start}")

    payload_event = ast.literal_eval(args.payload)
    cogify_transfer_response = cogify_transfer_handler(payload_event)
    response = json.dumps({**payload_event, **cogify_transfer_response})
    end = time() - start
    print(f"Actual processing took {end:.2f} seconds")
    # Check if it took less than 50 seconds
    if end - start < 50:
        sleep(50)
    print(response)
