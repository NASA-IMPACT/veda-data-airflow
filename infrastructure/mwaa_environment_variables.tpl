{
    "MWAA_STACK_CONF":{
        "PREFIX": "${prefix}",
        "EVENT_BUCKET": "${event_bucket}",
        "SECURITYGROUPS": ["${securitygroup_1}"],
        "SUBNETS": ["${subnet_1}", "${subnet_2}"],
        "ECS_CLUSTER_NAME": "${ecs_cluster_name}",
        "LOG_GROUP_NAME": "${log_group_name}",
        "STAGE": "${stage}",
        "MWAA_EXECUTION_ROLE_ARN": "${mwaa_execution_role_arn}",
        "ACCOUNT_ID": "${account_id}",
        "AWS_REGION": "${aws_region}"
    },
    "ASSUME_ROLE_READ_ARN": "${assume_role_read_arn}",
    "ASSUME_ROLE_WRITE_ARN": "${assume_role_write_arn}",
    "COGNITO_APP_SECRET": "${cognito_app_secret}",
    "STAC_INGESTOR_API_URL": "${stac_ingestor_api_url}"
}