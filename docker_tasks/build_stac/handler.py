from argparse import ArgumentParser
import ast
from contextlib import closing
import json
from multiprocessing import Pool, cpu_count
import os
from typing import Any, Dict, TypedDict, Union
from uuid import uuid4

import smart_open

from utils import stac as stac, events


class S3LinkOutput(TypedDict):
    stac_file_url: str


class StacItemOutput(TypedDict):
    stac_item: Dict[str, Any]


def handler(event: Dict[str, Any]) -> Union[S3LinkOutput, StacItemOutput]:
    """
    Lambda handler for STAC Collection Item generation

    Arguments:
    event - object with event parameters to be provided in one of 2 formats.
        Format option 1 (with Granule ID defined to retrieve all metadata from CMR):
        {
            "collection": "OMDOAO3e",
            "s3_filename": "s3://climatedashboard-data/OMDOAO3e/OMI-Aura_L3-OMDOAO3e_2022m0120_v003-2022m0122t021759.he5.tif",
            "granule_id": "G2205784904-GES_DISC",
        }
        Format option 2 (with regex provided to parse datetime from the filename:
        {
            "collection": "OMDOAO3e",
            "s3_filename": "s3://climatedashboard-data/OMSO2PCA/OMSO2PCA_LUT_SCD_2005.tif",
        }

    """

    EventType = events.CmrEvent if event.get("granule_id") else events.RegexEvent
    parsed_event = EventType.parse_obj(event)
    try:
        stac_item = stac.generate_stac(parsed_event).to_dict()
    except Exception as ex:
        out_err: StacItemOutput = {"stac_item": {"error": f"{ex}", "event": event}}
        return out_err

    output: StacItemOutput = {"stac_item": stac_item}
    return output


def using_pool(objects, workers_count: int):
    returned_results = []
    with closing(Pool(processes=workers_count)) as pool:
        results = pool.imap_unordered(handler, objects)
        for result in results:
            try:
                returned_results.append(result)
            except:
                print("Error")
    return returned_results


def sequential_processing(objects):
    returned_results = []
    for _object in objects:
        result = handler(_object)
        returned_results.append(result)
    return returned_results


def write_outputs_to_s3(key, payload_success, payload_failures):
    success_key = f"{key}/build_stac_output_{uuid4()}.json"
    with smart_open.open(success_key, "w") as _file:
        _file.write(json.dumps(payload_success))
    dead_letter_key = ""
    if payload_failures:
        dead_letter_key = f"{key}/dead_letter_events/build_stac_failed_{uuid4()}.json"
        with smart_open.open(dead_letter_key, "w") as _file:
            _file.write(json.dumps(payload_failures))
    return [success_key, dead_letter_key]


def stac_handler(payload_event):
    s3_event = payload_event.pop("payload")
    collection = payload_event.get("collection", "not_provided")
    bucket_output = os.environ["EVENT_BUCKET"]
    key = f"s3://{bucket_output}/events/{collection}"
    use_multiprocessing = payload_event.get("use_multiprocessing", True)
    workers_count = payload_event.get("cpu_count", cpu_count())
    payload_success = []
    payload_failures = []
    with smart_open.open(s3_event, "r") as _file:
        s3_event_read = _file.read()
    event_received = json.loads(s3_event_read)
    objects = event_received["objects"]
    payloads = using_pool(objects, workers_count=workers_count) if use_multiprocessing else sequential_processing(objects)
    for payload in payloads:
        stac_item = payload["stac_item"]
        if "error" in stac_item:
            payload_failures.append(stac_item)
        else:
            payload_success.append(stac_item)
    success_key, dead_letter_key = write_outputs_to_s3(
        key=key, payload_success=payload_success, payload_failures=payload_failures
    )

    return {
        "payload": {
            "success_event_key": success_key,
            "failed_event_key": dead_letter_key,
            "status": {
                "successes": len(payload_success),
                "failures": len(payload_failures),
            },
        }
    }


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="build_stac",
        description="Build STAC",
        epilog="Contact Abdelhak Marouane for extra help",
    )
    parser.add_argument("--payload", dest="payload", help="Events to pass to ")
    args = parser.parse_args()

    payload_event = ast.literal_eval(args.payload)
    building_stac_response = stac_handler(payload_event)
    response = json.dumps({
        **payload_event,
        **building_stac_response
    })
    print(response)
