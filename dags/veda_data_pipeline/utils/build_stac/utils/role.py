import boto3


def assume_role(role_arn, session_name):
    sts = boto3.client("sts")
    creds = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
    )
    return creds["Credentials"]
