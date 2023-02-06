{
    "MWAA_STACK_CONF":{
        "PREFIX": "${prefix}",
        "ASSUME_ROLE_ARN": "${assume_role_arn}",
        "EVENT_BUCKET": "${event_bucket}",
        "SECURITYGROUPS": ["${securitygroup_1}"],
        "SUBNETS": ["${subnet_1}", "${subnet_2}"]
         },
    "COGNITO_APP_SECRET": "${cognito_app_secret}",
    "STAC_INGESTOR_API_URL": "${stac_ingestor_api_url}",
    "STAGE": "${stage}"
}