import boto3
import http.client
import os
import base64
import ast
import json


#mwaa_env_name = 'veda-pipeline-dev-mwaa'
mwaa_env_name = os.getenv('TARGET_MWAA_ENV')
dag_name = os.getenv('TARGET_DAG_NAME')
mwaa_cli_command = os.getenv('TARGET_DAG_COMMAND')
client = boto3.client('mwaa')


def lambda_handler(event, context):
    for record in event['Records']:
        print(f"[ RECORD ]: {record}")
        s3_event_key = record['s3']['object']['key']
        print(f"[ S3 EVENT KEY ]: {s3_event_key}")
        s3_filename_target = os.path.split(s3_event_key)[-1]
        print(f"[ S3 FILENAME TARGET ]: {s3_filename_target}")
        s3_filename_no_ext = os.path.splitext(s3_filename_target)[0]
        print(f"[ S3 FILENAME NO EXT ]: {s3_filename_no_ext}")

        bucket_key_prefix = "EIS/FEDSoutput/Snapshot/"
        if s3_filename_no_ext.startswith("lf_"):
            bucket_key_prefix = "EIS/FEDSoutput/LFArchive/"

        # get web token
        mwaa_cli_token = client.create_cli_token(
            Name=mwaa_env_name
        )
        print(f"[ CLI TOKEN ]: {mwaa_cli_token}")
        serialized_args = json.dumps({
                    "discovery": "s3",
                    "collection": s3_filename_no_ext,
                    "prefix": bucket_key_prefix,
                    "bucket": "veda-data-store-staging",
                    "filename_regex": f"^(.*){s3_filename_target}$",
                    "vector": True
        })
        conn = http.client.HTTPSConnection(mwaa_cli_token['WebServerHostname'])
        payload = f"{mwaa_cli_command} {dag_name} --conf '{serialized_args}'"
        print(f"[ CLI PAYLOAD ]: {payload}")
        headers = {
          'Authorization': 'Bearer ' + mwaa_cli_token['CliToken'],
          'Content-Type': 'text/plain'
        }
        conn.request("POST", "/aws_mwaa/cli", payload, headers)
        res = conn.getresponse()
        data = res.read()
        dict_str = data.decode("UTF-8")
        mydata = ast.literal_eval(dict_str)
        print(f"[ DATA ]: {mydata}")
        print(f"[ STDOUT ]: {base64.b64decode(mydata['stdout'])}")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
