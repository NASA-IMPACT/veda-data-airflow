import json
from uuid import uuid4
import smart_open


def write_xcom_to_s3(s3_uri, payload):
    xcom_s3_key = f"{s3_uri}/xcom_output_{uuid4()}.json"
    with smart_open.open(xcom_s3_key, "w") as _file:
        _file.write(json.dumps(payload))
    return xcom_s3_key


def read_xcom_from_s3(s3_uri):
    with smart_open.open(s3_uri, "r") as _file:
        s3_event_read = _file.read()
    return json.loads(s3_event_read)