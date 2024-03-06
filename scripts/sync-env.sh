#!/usr/bin/env bash
# Use this script to load environment variables for a deployment from AWS Secrets

for s in $(aws secretsmanager get-secret-value --secret-id $1 --query SecretString --output text | jq -r "to_entries|map(\"\(.key)=\(.value|tostring)\")|.[]" ); do
    echo "$s" >> $GITHUB_ENV
    echo "$s" >> .env
done
source .env
export PREFIX=veda-pipeline-${STAGE}

cat << EXPORT_ENVS >> .env
PREFIX=$PREFIX
AWS_REGION=us-west-2
STATE_BUCKET_NAME=${PREFIX}-tf-state-shared
STATE_BUCKET_KEY=veda-mwaa/${PREFIX}-mwaa/terraform.tfstate
STATE_DYNAMO_TABLE=${PREFIX}-shared-state-mwaa-lock-state
EXPORT_ENVS
