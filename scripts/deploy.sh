#! /bin/bash
# Check .env file


DOT_ENV=$1

if [ -f $DOT_ENV ]
then
  set -a; source $DOT_ENV; set +a
else
  echo "Run: ./scripts/deploy.sh <.env_file>"
  echo "Please create $DOT_ENV file first and try again"
  exit 1
fi

function create_state_bucket {
  # $1 region
  # $2 bucket_name

  aws s3 mb  s3://$2  --region $1
  aws s3api put-bucket-versioning \
    --bucket $2 \
    --versioning-configuration Status=Enabled
}

function create_dynamo_db {
  # $1 region
  # $2 table_name
  aws dynamodb create-table \
    --table-name $2 \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region $1

}
function generate_terraform_variables {
  tf_vars=(tf tfvars)
    for tf_var in "${tf_vars[@]}"; do
    (
      echo "cat <<EOF"
      cat terraform.${tf_var}.tmpl
      echo EOF
    ) | sh > terraform.${tf_var}
  done

}

function check_create_remote_state {
  # $1 aws_region
  # $2 bucket name
  # $3 dynamotable_name
  AWS_REGION=$1
  STATE_BUCKET_NAME=$2
  STATE_DYNAMO_TABLE=$3

  bucketstatus=$(aws s3api head-bucket --bucket $STATE_BUCKET_NAME  2>&1)
  table_exists=$(aws dynamodb describe-table --table-name  $STATE_DYNAMO_TABLE --region $AWS_REGION  2>&1)

  if echo "${table_exists}" | grep "An error";
  then
    echo "Creating dynamodb table for TF state"
    create_dynamo_db $AWS_REGION $STATE_DYNAMO_TABLE
  else
    echo "DynamoDB $STATE_DYNAMO_TABLE exists. Continue..."
  fi

  if echo "${bucketstatus}" | grep 'Not Found';
  then
        echo "Creating TF remote state"
        create_state_bucket $AWS_REGION $STATE_BUCKET_NAME
        create_dynamo_db $AWS_REGION $STATE_DYNAMO_TABLE
  elif echo "${bucketstatus}" | grep 'Forbidden';
  then
    echo "Bucket $STATE_BUCKET_NAME exists but not owned"
    exit 1
  elif echo "${bucketstatus}" | grep 'Bad Request';
  then
    echo "Bucket $STATE_BUCKET_NAME specified is less than 3 or greater than 63 characters"
    exit 1
  else
    echo "State Bucket $STATE_BUCKET_NAME owned and exists. Continue...";
    echo "State Dynamo table $STATE_DYNAMO_TABLE owned and exists. Continue...";
  fi
}


cd ./infrastructure
generate_terraform_variables
check_create_remote_state $AWS_REGION $STATE_BUCKET_NAME $STATE_DYNAMO_TABLE

read -rp 'action [init|plan|deploy]: ' ACTION
case $ACTION in
  init)
    terraform init
    ;;
  plan)
    terraform plan
    ;;

  deploy)
    terraform apply --auto-approve
    ;;
  *)
    echo "Chose from 'init', 'plan' or 'deploy'"
    exit 1
    ;;
esac

