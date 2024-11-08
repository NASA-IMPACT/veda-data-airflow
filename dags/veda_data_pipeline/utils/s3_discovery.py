import itertools
import json
import os
import re
from typing import List, Dict
from uuid import uuid4
from pathlib import Path

from datetime import datetime
from dateutil.tz import tzlocal
import boto3
from smart_open import open as smrt_open


# Adding a custom exception for empty list
class EmptyFileListError(Exception):
    def __init__(self, error_message):
        self.error_message = error_message
        super().__init__(self.error_message)


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


def discover_from_s3(
        response_iterator, filename_regex: str, last_execution: datetime
) -> dict:
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
            conditionals = [re.match(filename_regex, key)]
            if last_execution:
                last_modified = s3_object["LastModified"]
                conditionals.append(last_modified > last_execution)
            if all(conditionals):
                yield s3_object


def group_by_item(discovered_files: List[str], id_regex: str, assets: dict) -> dict:
    """Group assets by matching regex patterns against discovered files."""
    grouped_files = []
    for uri in discovered_files:
        # Each file gets its matched asset type and id
        filename = uri.split("/")[-1]
        prefix = "/".join(uri.split("/")[:-1])
        asset_type = None
        if match := re.match(id_regex, filename):
            # At least one match; can use the match here to construct an ID (match groups separated by '-')
            item_id = "-".join(match.groups())
            for asset_name, asset_definition in assets.items():
                regex = asset_definition["regex"]
                if re.match(regex, filename):
                    asset_type = asset_name
                    break
            if asset_type:
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


def construct_single_asset_items(discovered_files: List[str], assets: dict|None) -> dict:
    items_with_assets = []
    asset_key = "default"
    asset_value = {}
    if assets:
        asset_key = list(assets.keys())[0]
        asset_value = assets[asset_key]
    for uri in discovered_files:
        # Each file gets its matched asset type and id
        filename = uri.split("/")[-1]
        filename_without_extension = Path(filename).stem
        prefix = "/".join(uri.split("/")[:-1])
        item = {
            "item_id": filename_without_extension,
            "assets": {
                asset_key: {
                    "title": "Default COG Layer",
                    "description": "Cloud optimized default layer to display on map",
                    "href": f"{prefix}/{filename}",
                    **asset_value
                }
            },
        }
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
    id_template = event.get("id_template", "{}")
    date_fields = propagate_forward_datetime_args(event)
    dry_run = event.get("dry_run", False)
    if process_from := event.get("process_from_yyyy_mm_dd"):
        process_from = datetime.strptime(process_from, "%Y-%m-%d").replace(
            tzinfo=tzlocal()
        )
    if last_execution := event.get("last_successful_execution"):
        last_execution = datetime.fromisoformat(last_execution)
    if dry_run:
        print("Running discovery in dry run mode")

    payload = {**event, "objects": []}
    slice = event.get("slice")

    bucket_output = os.environ.get("EVENT_BUCKET", bucket_output)
    key = f"s3://{bucket_output}/events/{collection}"
    records = 0
    out_keys = []
    discovered = []

    kwargs = assume_role(role_arn=role_arn) if role_arn else {}
    s3client = boto3.client("s3", **kwargs)
    s3_iterator = get_s3_resp_iterator(
        bucket_name=bucket, prefix=prefix, s3_client=s3client
    )
    file_uris = [
        f"s3://{bucket}/{obj['Key']}"
        for obj in discover_from_s3(
            s3_iterator, filename_regex, last_execution=process_from or last_execution
        )
    ]

    if len(file_uris) == 0:
        raise EmptyFileListError(f"No files discovered at bucket: {bucket}, prefix: {prefix}")

    # group only if more than 1 assets
    if assets and len(assets.keys()) > 1:
        items_with_assets = group_by_item(file_uris, id_regex, assets)
    else:
        # out of convenience, we might not always want to explicitly define assets
        # or if only a single asset is defined, follow default flow
        items_with_assets = construct_single_asset_items(file_uris, assets)

    if len(items_with_assets) == 0:
        raise EmptyFileListError(
            f"No items could be constructed for files at bucket: {bucket}, prefix: {prefix}"
        )

    # Update IDs using id_template
    for item in items_with_assets:
        item["item_id"] = id_template.format(item["item_id"])

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

        if dry_run and item_count < 10:
            print("-DRYRUN- Example item")
            print(json.dumps(file_obj))

        payload["objects"].append(file_obj)
        if records == chunk_size:
            out_keys.append(generate_payload(s3_prefix_key=key, payload=payload))
            records = 0
            discovered.append(len(payload["objects"]))
            payload["objects"] = []
        records += 1

    if payload["objects"]:
        out_keys.append(generate_payload(s3_prefix_key=key, payload=payload))
        discovered.append(len(payload["objects"]))
    # We need to make sure the payload isn't too large for ECS overrides
    try:
        del event["assets"]
    except KeyError:
        pass
    return {**event, "payload": out_keys, "discovered": discovered}

def generate_dates_from_datetime_args(date_fields: Dict[str, str], frequency: str) -> List[str]:
    """
    Generate a list of date strings based on date_fields and a frequency.
    :param date_fields: Dictionary with datetime-related fields, e.g., 'start_datetime', 'end_datetime'.
    :param frequency: The frequency of dates ('daily', 'monthly', 'yearly').
    :return: List of dates in 'YYYYMM' or 'YYYYMMDD' format based on frequency.
    """
    if "start_datetime" in date_fields and "end_datetime" in date_fields:
        start_date = datetime.strptime(date_fields["start_datetime"], "%Y%m%d")
        end_date = datetime.strptime(date_fields["end_datetime"], "%Y%m%d")
    else:
        raise ValueError("Datetime range with 'start_datetime' and 'end_datetime' must be provided.")

    current_date = start_date
    dates = []

    while current_date <= end_date:
        if frequency == "day":
            dates.append(current_date.strftime("%Y%m%d"))
            current_date += timedelta(days=1)
        elif frequency == "month":
            dates.append(current_date.strftime("%Y%m"))
            # Move to the first day of the next month
            month = current_date.month + 1 if current_date.month < 12 else 1
            year = current_date.year if month > 1 else current_date.year + 1
            current_date = current_date.replace(year=year, month=month, day=1)
        elif frequency == "year":
            dates.append(current_date.strftime("%Y"))
            current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
        else:
            raise ValueError(f"Unsupported frequency: {frequency}")

    print(f"Generated {len(dates)} dates with frequency '{frequency}' from datetime args {date_fields}.")
    return dates

# Function to generate item IDs using the id template
def generate_item_ids(dates: List[str], id_template: str) -> List[str]:
    item_ids = [id_template.format(date) for date in dates]
    print(f"Generated {len(item_ids)} item IDs using template '{id_template}'.")
    return item_ids

# Function to generate asset metadata for each item ID
def generate_asset_metadata(variable: str, date: str, model:str,  asset_template: str, asset_definitions: Dict[str, dict]) -> Dict[str, dict]:
    expected_href = asset_template.format(variable, variable, model, date)
    assets = {}

    asset_info = asset_definitions.get(variable, {})
    asset_metadata = asset_info.copy()
    asset_metadata["href"] = expected_href
    assets[variable] = asset_metadata
    # title and description must be provided, but can be autofilled if needed
    asset_metadata["title"] = asset_info.get("title", variable)
    asset_metadata["description"] = asset_info.get("description", f"{variable} asset (default description)")

    return assets

def asset_exists(s3_client, bucket_name: str, asset_href: str) -> bool:
    """Check if an asset exists in the specified S3 bucket."""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=asset_href.split(f"s3://{bucket_name}/", 1)[-1])
        print(f"Asset '{asset_href}' exists in S3 bucket '{bucket_name}'.")
        return True
    except s3_client.exceptions.ClientError as e:
        print(f"Asset '{asset_href}' does not exist in S3 bucket '{bucket_name}'.")
        print(f"Error: {e}")
        return False

# Reduced chunk size to reflect more assets per item
def cmip_discovery_handler(event, chunk_size=200, role_arn=None, bucket_output=None):
    variables = event.get("variables", ["rlds", "huss", "hurs"])
    date_fields = propagate_forward_datetime_args(event)
    frequency = date_fields.get("datetime_range", "month")  # Frequency (daily, monthly, yearly)
    assets = event.get("assets")
    id_template = event.get("id_template", "{}-{}")
    asset_template = event.get("asset_template", "s3://nex-gddp-cmip6-cog/monthly/CMIP6_ensemble/median/{}/{}_month_ensemble-median_{}_{}.tif")
    bucket = event.get("bucket", "nex-gddp-cmip6-cog")
    collection = event.get("collection", "cmip6-test")
    model = event.get("model", "ssp585")
    dry_run = event.get("dry_run", False)

    s3_client = boto3.client("s3")

    dates = generate_dates_from_datetime_args(date_fields, frequency)
    item_ids = generate_item_ids(dates, id_template)
    items_with_assets = []
    for item_id in item_ids:
        _, date = item_id.split("-")
        valid_assets = {}
        for variable in variables:
            assets_metadata = generate_asset_metadata(variable, date, model, asset_template, assets)
            for asset_type, asset_metadata in assets_metadata.items():
                print(f"Asset type: {asset_type}\nAsset metadata: {asset_metadata}\n")
                asset_key = asset_metadata["href"].split(f"s3://{bucket}/", 1)[-1]
                if asset_exists(s3_client, bucket, asset_key):
                    valid_assets[asset_type] = asset_metadata

        # Only include items with at least one valid asset
        if valid_assets:
            items_with_assets.append({"item_id": item_id, "assets": valid_assets})

    print(f"Initial discovery completed. {len(items_with_assets)} items with assets found.")

    payload = {**event, "objects": []}
    slice = event.get("slice")

    bucket_output = os.environ.get("EVENT_BUCKET", bucket_output)
    key = f"s3://{bucket_output}/events/{collection}"
    records = 0
    out_keys = []
    discovered = []

    item_count = 0
    for item in items_with_assets:
        item_count += 1
        if slice:
            if item_count < slice[0]:
                continue
            if (
                    item_count >= slice[1]
            ):
                break
        file_obj = {
            "collection": collection,
            "item_id": item["item_id"],
            "assets": item["assets"],
            #"properties": properties,
            "datetime_range": frequency,
        }

        if dry_run and item_count < 10:
            print("-DRYRUN- Example item")
            print(json.dumps(file_obj))

        payload["objects"].append(file_obj)
        if records == chunk_size:
            out_keys.append(generate_payload(s3_prefix_key=key, payload=payload))
            records = 0
            discovered.append(len(payload["objects"]))
            payload["objects"] = []
        records += 1

    if payload["objects"]:
        out_keys.append(generate_payload(s3_prefix_key=key, payload=payload))
        discovered.append(len(payload["objects"]))
    try:
        del event["assets"]
    except KeyError:
        pass
    return {**event, "payload": out_keys, "discovered": discovered}
