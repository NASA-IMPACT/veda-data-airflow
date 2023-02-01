import base64
import json
import os
import sys

import boto3
import requests

from utils import args_handler, data_files, DATA_PATH, MWAA_NAME

dag_name = "veda_discover"
airflow_client = boto3.client("mwaa")
print(MWAA_NAME)
mwaa_cli_token = airflow_client.create_cli_token(
  Name=MWAA_NAME
)
mwaa_auth_token = 'Bearer ' + mwaa_cli_token['CliToken']
mwaa_webserver_hostname = 'https://{0}/aws_mwaa/cli'.format(mwaa_cli_token['WebServerHostname'])


items_path = os.path.join(DATA_PATH, "items")
def insert_items(files):
    print("Inserting items:")
    for filename in files:
        print(filename)
        events = json.load(open(filename))
        if type(events) != list:
            events = [events]
        for event in events:
            raw_data = "dags trigger {0} --conf '{1}'".format(dag_name, json.dumps(event))
            mwaa_response = requests.post(
                mwaa_webserver_hostname,
                headers={
                    'Authorization': mwaa_auth_token,
                    'Content-Type': 'application/json'
                    },
                data=raw_data
            )
            mwaa_std_err_message = base64.b64decode(mwaa_response.json()['stderr']).decode('utf8')
            mwaa_std_out_message = base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')
            print(mwaa_response.status_code)
            print(f"stderr: {mwaa_std_err_message}")
            print(f"stdout: {mwaa_std_out_message}")

if __name__ == "__main__":
    file_regex = sys.argv[1]
    files = data_files(file_regex, items_path)
    print(file_regex, files)
    insert_items(files)