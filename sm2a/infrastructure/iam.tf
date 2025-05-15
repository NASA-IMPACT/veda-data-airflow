locals {

  base_policies = [
    {
      Effect = "Allow"
      Action = [
        "sts:AssumeRole",
        "iam:PassRole",
        "logs:GetLogEvents"
      ]
      Resource = ["*"]
    },
    {
      Effect = "Allow"
      Action = ["glue:DeleteDatabase"]
      Resource = [
        "arn:aws:glue:us-west-2:*:catalog",
        "arn:aws:glue:us-west-2:*:database/*",
        "arn:aws:glue:us-west-2:*:table/*",
        "arn:aws:glue:us-west-2:*:userDefinedFunction/*"
      ]
    },
  ]

  # Disaster Recovery
  disaster_recovery_policy = [
    {
      Effect = "Allow"
      Action = [
        "rds:Describe*", 
        "rds:Start*", "kms:*", 
        "glue:Get*", 
        "glue:CreateCrawler", 
        "glue:StartCrawler", 
        "glue:UpdateCrawler"
      ]
      Resource = ["*"]
    }
  ]

  #Cloudfront Invalidation
  cloudfront_invalidation_policy = [
    {
      Effect = "Allow"
      Action = ["cloudfront:CreateInvalidation"]
      Resource = ["arn:aws:cloudfront::*:distribution/*"]
    }
  ]

  # Conditional Secrets Manager arn for ingest api keycloak client secret in different aws account
  secrets_manager_policy = var.ingest_api_keycloak_client_secret_arn != null ? [
    {
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = [var.ingest_api_keycloak_client_secret_arn]
    }
  ] : []

  # Conditional KMS policy statement
  kms_policy = var.kms_key_arn != null ? [
    {
      Effect   = "Allow"
      Action   = ["kms:Decrypt"]
      Resource = [var.kms_key_arn]
    }
  ] : []

  # Final list of all worker policy statements
  custom_worker_policy_statement = concat(
    local.base_policies,
    local.disaster_recovery_policy,
    local.cloudfront_invalidation_policy,
    local.secrets_manager_policy,
    local.kms_policy
  )
} 