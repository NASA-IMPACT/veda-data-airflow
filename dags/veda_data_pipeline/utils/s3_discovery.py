import itertools
import json
import os
import re
from typing import List
from uuid import uuid4

import boto3
from smart_open import open as smrt_open


def assume_role(role_arn, session_name="veda-data-pipelines_s3-discovery"):
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


def get_s3_resp_iterator(bucket_name, prefix, s3_client, page_size=1000):
    """
    Returns an s3 paginator.
    :param bucket_name: The bucket.
    :param prefix: The path for the s3 granules.
    :param s3_client: Initialized boto3 S3 client
    :param page_size: Number of records returned
    """
    s3_paginator = s3_client.get_paginator("list_objects")
    print(f"Getting S3 response iterator for bucket: {bucket_name}, prefix: {prefix}")
    return s3_paginator.paginate(
        Bucket=bucket_name, Prefix=prefix, PaginationConfig={"page_size": page_size}
    )


def discover_from_s3(response_iterator, filename_regex: str) -> dict:
    """Iterate through pages of S3 objects returned by a ListObjectsV2 operation.
    The discover_from_s3 function takes in an iterator over the pages of S3 objects returned
    by a ListObjectsV2 operation. It iterates through the pages and yields each S3 object in the page as a dictionary.
    This function can be used to iterate through a large number of S3 objects returned by a ListObjectsV2 operation
    without having to load all the objects into memory at once.

    Parameters:
    response_iterator (iter): An iterator over the pages of S3 objects returned by a ListObjectsV2 operation.
    filename_regex (str): A regular expression used to filter the S3 objects returned by the ListObjectsV2 operation.

    Yields:
    dict: A dictionary representing an S3 object.
    """
    for page in response_iterator:
        for s3_object in page.get("Contents", {}):
            key = s3_object["Key"]
            if re.match(filename_regex, key):
                yield s3_object


def group_by_item(discovered_files: List[str], id_regex: str, assets: dict) -> dict:
    """Group assets by matching regex patterns against discovered files."""
    grouped_files = []
    for uri in discovered_files:
        # Each file gets its matched asset type and id
        filename = uri.split("/")[-1]
        prefix = "/".join(uri.split("/")[:-1])
        if match := re.match(id_regex, filename):
            # At least one match; can use the match here to construct an ID (match groups separated by '-')
            item_id = "-".join(match.groups())
            for asset_name, asset_definition in assets.items():
                regex = asset_definition["regex"]
                if re.match(regex, filename):
                    asset_type = asset_name
                    break
            grouped_files.append(
                {
                    "prefix": prefix,
                    "filename": filename,
                    "asset_type": asset_type,
                    "item_id": item_id,
                }
            )
        else:
            print(f"Warning: skipping file. No id match found: {filename}")
    # At this point, files are labeled with type and id. Now, group them by id
    sorted_list = sorted(grouped_files, key=lambda x: x["item_id"])
    grouped_data = [
        {"item_id": key, "data": list(group)}
        for key, group in itertools.groupby(sorted_list, key=lambda x: x["item_id"])
    ]
    items_with_assets = []
    # Produce a dictionary in which each record is keyed by an item ID and contains a list of associated asset hrefs
    for group in grouped_data:
        item = {"item_id": group["item_id"], "assets": {}}
        for file in group["data"]:
            asset_type = file["asset_type"]
            filename = file["filename"]
            # Copy the asset definition and update the href
            updated_asset = assets[file["asset_type"]].copy()
            updated_asset["href"] = f"{file['prefix']}/{file['filename']}"
            item["assets"][asset_type] = updated_asset
        items_with_assets.append(item)
    return items_with_assets


def generate_payload(s3_prefix_key: str, payload: dict):
    """Generate a payload and write it to an S3 file.
    This function takes in a prefix for an S3 key and a dictionary containing a payload.
    The function then writes the payload to an S3 file using the provided prefix and a randomly
    generated UUID as the key. The key of the output file is then returned.
    Parameters:
    s3_prefix_key (str): The prefix for the S3 key where the output file will be written.
    payload (dict): A dictionary containing the payload to be written to the output file.

    Returns:
    str: The S3 key of the output file.
    """
    output_key = f"{s3_prefix_key}/s3_discover_output_{uuid4()}.json"
    with smrt_open(output_key, "w") as file:
        file.write(json.dumps(payload))
    return output_key


def propagate_forward_datetime_args(event):
    """
    This function extracts datetime-related arguments from the input event dictionary.
    The purpose is to forward these datetime arguments to other functions that may require them.

    The function looks for the keys "single_datetime", "start_datetime", "end_datetime",
    and "datetime_range" in the event dictionary. If any of these keys are present,
    it includes them in the output dictionary.

    Parameters:
    event (dict): Input dictionary potentially containing datetime arguments.

    Returns:
    dict: A new dictionary containing only the datetime-related keys from the input
    that were present. If none of the specified keys are in the event,
    the function returns an empty dictionary.
    """
    keys = ["single_datetime", "start_datetime", "end_datetime", "datetime_range"]
    return {key: event[key] for key in keys if key in event}


def s3_discovery_handler(event, chunk_size=2800, role_arn=None, bucket_output=None):
    bucket = event.get("bucket")
    prefix = event.get("prefix", "")
    filename_regex = event.get("filename_regex", None)
    collection = event.get("collection", prefix.rstrip("/"))
    properties = event.get("properties", {})
    assets = event.get("assets")
    id_regex = event.get("id_regex")
    id_template = event.get("id_template", collection + "-{}")
    date_fields = propagate_forward_datetime_args(event)
    dry_run = event.get("dry_run", False)

    payload = {**event, "objects": []}
    slice = event.get("slice")

    bucket_output = os.environ.get("EVENT_BUCKET", bucket_output)
    key = f"s3://{bucket_output}/events/{collection}"
    records = 0
    out_keys = []
    discovered = 0

    kwargs = assume_role(role_arn=role_arn) if role_arn else {}
    s3client = boto3.client("s3", **kwargs)
    s3_iterator = get_s3_resp_iterator(
        bucket_name=bucket, prefix=prefix, s3_client=s3client
    )
    file_uris = [
        f"s3://{bucket}/{obj['Key']}"
        for obj in discover_from_s3(s3_iterator, filename_regex)
    ]
    items_with_assets = group_by_item(file_uris, id_regex, assets)
    # Update IDs using id_template
    for item in items_with_assets:
        item["item_id"] = id_template.format(item["item_id"])

    if dry_run:
        print(f"-DRYRUN- Discovered {len(items_with_assets)} items")
        for idx in range(0, min(10, len(items_with_assets))):
            print("-DRYRUN- Example item")
            print(json.dumps(items_with_assets[idx]))

    item_count = 0
    for item in items_with_assets:
        item_count += 1
        # Logic to ingest a 'slice' of data
        if slice:
            if item_count < slice[0]:  # Skip until we reach the start of the slice
                continue
            if (
                item_count >= slice[1]
            ):  # Stop once we reach the end of the slice, while saving progress
                break
        file_obj = {
            "collection": collection,
            "item_id": item["item_id"],
            "assets": item["assets"],
            "properties": properties,
            **date_fields,
        }

        payload["objects"].append(file_obj)
        if records == chunk_size:
            out_keys.append(generate_payload(s3_prefix_key=key, payload=payload))
            records = 0
            discovered += len(payload["objects"])
            payload["objects"] = []
        records += 1

    if payload["objects"]:
        out_keys.append(generate_payload(s3_prefix_key=key, payload=payload))
        discovered += len(payload["objects"])
    # We need to make sure the payload isn't too large for ECS overrides
    try:
        del event["assets"]
    except KeyError:
        pass
    return {**event, "payload": out_keys, "discovered": discovered}
