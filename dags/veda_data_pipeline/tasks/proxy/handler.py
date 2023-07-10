import json
import os

import boto3


def handler(event, context):
    STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]
    step_function_input = [json.loads(record["body"]) for record in event["Records"]]

    client = boto3.client("stepfunctions")
    client.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        input=json.dumps(step_function_input),
    )
    return
