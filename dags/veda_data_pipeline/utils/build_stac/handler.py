import logging
import json
from typing import Any, Dict, TypedDict, Union
from uuid import uuid4
import smart_open
from veda_data_pipeline.utils.build_stac.utils import events
from veda_data_pipeline.utils.build_stac.utils import stac
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.exceptions import AirflowException



class S3LinkOutput(TypedDict):
    stac_file_url: str


def using_pool(objects, workers_count: int):
    returned_results = []
    with ThreadPoolExecutor(max_workers=workers_count) as executor:
        # Submit tasks to the executor
        futures = {executor.submit(handler, obj): obj for obj in objects}

        for future in as_completed(futures):
            try:
                result = future.result()  # Get result from future
                returned_results.append(result)
            except Exception as nex:
                print(f"Error {nex} with object {futures[future]}")

    return returned_results


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



def stac_handler(payload_src: dict, bucket_output, ti=None):
    payload_event = payload_src.copy()
    s3_event = payload_event.pop("payload")
    collection = payload_event.get("collection", "not_provided")
    key = f"s3://{bucket_output}/events/{collection}"
    payload_success = []
    payload_failures = []

    try:
        logging.info(f"=== Starting build_stac for collection {collection} ===")
        with smart_open.open(s3_event, "r") as _file:
            s3_event_read = _file.read()
        event_received = json.loads(s3_event_read)
        objects = event_received["objects"]

        logging.info(f"Total items to process is: {len(objects)}")

        use_multithreading = payload_event.get("use_multithreading", True)
        payloads = (
            using_pool(objects, workers_count=4)
            if use_multithreading
            else sequential_processing(objects)
        )
        for index, payload in enumerate(payloads, 1):
            stac_item = payload["stac_item"]
            if "error" in stac_item:
                payload_failures.append(stac_item)
            else:
                payload_success.append(stac_item)

            if index % 100 == 0:
                logging.info(f"Processed {index} items of {len(objects)}")

        success_key, dead_letter_key = write_outputs_to_s3(
            key=key, payload_success=payload_success, payload_failures=payload_failures
        )

        total_processed = len(payload_success) + len(payload_failures)

        logging.info("\n=== Run Summary ===")
        logging.info(f"Collection: {collection}")
        logging.info(f"Total Processed: {total_processed}")
        logging.info(f"Successes: {len(payload_success)}")
        logging.info(f"Failures: {len(payload_failures)}")
        logging.info(f"Success Rate: {(len(payload_success) / total_processed) * 100:.2f}%" if total_processed > 0 else "0%")

        if payload_failures:
            logging.warning("\n=== Error Breakdown ===")
            error_breakdown = {}
            for failure in payload_failures:
                error_msg = failure.get('error', 'Unknown error')
                error_breakdown[error_msg] = error_breakdown.get(error_msg, 0) + 1

            for error, count in error_breakdown.items():
                logging.warning(f"  - {error}: {count} occurrences")

        result = {
            "payload": {
                "success_event_key": success_key,
                "failed_event_key": dead_letter_key,
                "status": {
                    "successes": len(payload_success),
                    "failures": len(payload_failures),
                }
            }
        }

        if len(payload_failures) != 0:
            logging.warning(
                f"Build STAC completed with {len(payload_failures)} failures. See logs for details {dead_letter_key}"
            )

        return result

    except Exception as e:
        logging.error(f"Unexpected error in build stac process: {str(e)}")
        raise
