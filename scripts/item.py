import base64
import json
import sys

import requests
from utils import get_items, get_mwaa_cli_token


def insert_items(files):
    mwaa_cli_token = get_mwaa_cli_token()
    mwaa_auth_token = "Bearer " + mwaa_cli_token["CliToken"]
    print("Inserting items:")
    for filename in files:
        print(filename)
        events = json.load(open(filename))
        if type(events) != list:
            events = [events]
        for event in events:
            raw_data = f"dags trigger veda_discover --conf '{json.dumps(event)}'"
            mwaa_response = requests.post(
                f"https://{mwaa_cli_token['WebServerHostname']}/aws_mwaa/cli",
                headers={
                    "Authorization": mwaa_auth_token,
                    "Content-Type": "application/json",
                },
                data=raw_data,
            )
            mwaa_std_err_message = base64.b64decode(
                mwaa_response.json()["stderr"]
            ).decode("utf8")
            mwaa_std_out_message = base64.b64decode(
                mwaa_response.json()["stdout"]
            ).decode("utf8")
            print(mwaa_response.status_code)
            print(f"stderr: {mwaa_std_err_message}")
            print(f"stdout: {mwaa_std_out_message}")


if __name__ == "__main__":
    file_regex = sys.argv[1]
    files = get_items(file_regex)
    print(file_regex, files)
    insert_items(files)
