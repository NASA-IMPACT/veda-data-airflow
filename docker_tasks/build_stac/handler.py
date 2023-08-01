import ast
import json
import os
from argparse import ArgumentParser
from contextlib import closing
from multiprocessing import Pool, cpu_count
from time import sleep, time
from typing import Any, Dict, TypedDict, Union
from uuid import uuid4

import smart_open
from utils import events
from utils import stac as stac


class S3LinkOutput(TypedDict):
    stac_file_url: str


class StacItemOutput(TypedDict):
    stac_item: Dict[str, Any]


def handler(event: Dict[str, Any]) -> Union[S3LinkOutput, StacItemOutput]:
    """
    Handler for STAC Collection Item generation

    Arguments:
    event - object with event parameters
        {
            "collection": "OMDOAO3e",
            "id_regex": "_(.*).tif",
            "assets": {
                "OMDOAO3e_LUT": {
                    "title": "OMDOAO3e_LUT",
                    "description": "OMDOAO3e_LUT, described",
                    "href": "s3://climatedashboard-data/OMDOAO3e/OMDOAO3e_LUT.tif",
                },
                "OMDOAO3e_LUT": {
                    "title": "OMDOAO3e_LUT",
                    "description": "OMDOAO3e_LUT, described",
                    "href": "s3://climatedashboard-data/OMDOAO3e/OMDOAO3e_LUT.tif",
                }
            }
        }

    """

    parsed_event = events.RegexEvent.parse_obj(event)
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
            except Exception as nex:
                print(f"Error {nex}")
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
    payloads = (
        using_pool(objects, workers_count=workers_count)
        if use_multiprocessing
        else sequential_processing(objects)
    )
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
    parser.add_argument(
        "--payload", dest="payload", help="event passed to stac_handler function"
    )
    args = parser.parse_args()
    # For cloud watch log to work the task should stay alife for at least 30 s
    start = time()
    print(f"Start at {start}")

    payload_event = ast.literal_eval(args.payload)
    building_stac_response = stac_handler(payload_event)
    response = json.dumps({**payload_event, **building_stac_response})
    end = time() - start
    print(f"Actual processing took {end:.2f} seconds")
    # Check if it took less than 50 seconds
    if end - start < 50:
        sleep(50)
    print(response)
