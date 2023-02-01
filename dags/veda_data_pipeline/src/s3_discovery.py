import re
import boto3
import json
from uuid import uuid4
from smart_open import open as smrt_open
from airflow.models.variable import Variable

MWAA_STAC_CONF = Variable.get("MWAA_STACK_CONF", deserialize_json=True)


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
    return s3_paginator.paginate(
        Bucket=bucket_name, Prefix=prefix, PaginationConfig={"page_size": page_size}
    )


def discover_from_s3(response_iterator):
    """Iterate through pages of S3 objects returned by a ListObjectsV2 operation.
    The discover_from_s3 function takes in an iterator over the pages of S3 objects returned
    by a ListObjectsV2 operation. It iterates through the pages and yields each S3 object in the page as a dictionary.
    This function can be used to iterate through a large number of S3 objects returned by a ListObjectsV2 operation
    without having to load all the objects into memory at once.

    Parameters:
    response_iterator (iter): An iterator over the pages of S3 objects returned by a ListObjectsV2 operation.

    Yields:
    dict: A dictionary representing an S3 object.
    """
    for page in response_iterator:
        for s3_object in page.get("Contents", {}):
            yield s3_object


def generate_payload(s3_prefix_key: str, payload: dict, limit: int = None):
    """Generate a payload and write it to an S3 file.
    This function takes in a prefix for an S3 key, a dictionary containing a payload,
    and an optional limit on the number of objects in the payload. If a limit is provided,
    the function will slice the objects list in the payload dictionary to include only the specified number
    of objects. The function then writes the payload to an S3 file using the provided prefix and a randomly
    generated UUID as the key. The key of the output file is then returned.
    Parameters:
    s3_prefix_key (str): The prefix for the S3 key where the output file will be written.
    payload (dict): A dictionary containing the payload to be written to the output file.
    limit (int, optional): The maximum number of objects to include in the payload. If not provided, all objects will be included.

    Returns:
    str: The S3 key of the output file.
    """
    if limit:
        payload["objects"] = payload["objects"][:limit]
    output_key = f"{s3_prefix_key}/s3_discover_output_{uuid4()}.json"
    with smrt_open(output_key, "w") as file:
        file.write(json.dumps(payload))
    return output_key


def s3_discovery_handler(event, chunk_size=2800):
    bucket = event.get("bucket")
    prefix = event.get("prefix", "")
    filename_regex = event.get("filename_regex", None)
    collection = event.get("collection", prefix.rstrip("/"))
    properties = event.get("properties", {})
    event["cogify"] = event.pop("cogify", False)
    payload = {**event, "objects": []}
    limit = event.get("limit")
    # Propagate forward optional datetime arguments
    date_fields = {}
    if "single_datetime" in event:
        date_fields["single_datetime"] = event["single_datetime"]
    if "start_datetime" in event:
        date_fields["start_datetime"] = event["start_datetime"]
    if "end_datetime" in event:
        date_fields["end_datetime"] = event["end_datetime"]
    if "datetime_range" in event:
        date_fields["datetime_range"] = event["datetime_range"]

    role_arn = MWAA_STAC_CONF.get("ASSUME_ROLE_ARN")
    kwargs = assume_role(role_arn=role_arn) if role_arn else {}
    s3client = boto3.client("s3", **kwargs)

    s3_iterator = get_s3_resp_iterator(
        bucket_name=bucket, prefix=prefix, s3_client=s3client
    )
    bucket_output = MWAA_STAC_CONF.get("EVENT_BUCKET")
    key = f"s3://{bucket_output}/events/{collection}"
    records = 0
    out_keys = []
    discovered = 0
    for s3_object in discover_from_s3(s3_iterator):
        filename = s3_object["Key"]
        if filename_regex and not re.match(filename_regex, filename):
            continue
        file_obj = {
            "collection": collection,
            "s3_filename": f"s3://{bucket}/{filename}",
            "upload": event.get("upload", False),
            "properties": properties,
            **date_fields,
        }

        payload["objects"].append(file_obj)
        if records == chunk_size:
            out_keys.append(
                generate_payload(s3_prefix_key=key, payload=payload, limit=limit)
            )
            records = 0
            discovered += len(payload["objects"])
            payload["objects"] = []
            if limit:
                return {**event, "payload": out_keys, "discovered": discovered}
        records += 1

    if payload["objects"]:
        out_keys.append(
            generate_payload(s3_prefix_key=key, payload=payload, limit=limit)
        )
        discovered += len(payload["objects"])
    return {**event, "payload": out_keys, "discovered": discovered}
