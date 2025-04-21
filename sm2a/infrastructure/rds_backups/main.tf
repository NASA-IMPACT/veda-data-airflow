resource "aws_s3_bucket" "rds_backup_bucket" {
  bucket = var.snapshot_bucket_name
  lifecycle {
    prevent_destroy = true
  }
}


resource "aws_iam_role" "snapshot_export_role" {
  name                 = "${var.prefix}-s3-export-role"
  permissions_boundary = var.permission_boundaries_arn

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "export.rds.amazonaws.com"
        }
      }
    ]
  })
}


resource "aws_iam_role" "glue_crawler_role" {
  name                 = "${var.prefix}-glue-crawler-role"
  permissions_boundary = var.permission_boundaries_arn

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}


resource "aws_iam_policy" "s3_snapshot_export_policy" {
  name = "${var.prefix}-s3-export-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject*",
          "s3:ListBucket",
          "s3:GetObject*",
          "s3:DeleteObject*",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.snapshot_bucket_name}",
          "arn:aws:s3:::${var.snapshot_bucket_name}/*"
        ]
      }
    ]
  })
}



resource "aws_iam_policy" "glue_crawler_policy" {
  name = "${var.prefix}-glue-crawler-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.snapshot_bucket_name}",
          "arn:aws:s3:::${var.snapshot_bucket_name}/*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "kms:*"
        ],
        "Resource" : [
          "*"
        ]
      }

    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_s3_export_policy" {
  role       = aws_iam_role.snapshot_export_role.name
  policy_arn = aws_iam_policy.s3_snapshot_export_policy.arn
}

resource "aws_iam_role_policy_attachment" "attach_glue_crawler_policy" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = aws_iam_policy.glue_crawler_policy.arn
}
# Attach the AWSGlueServiceRole managed policy
resource "aws_iam_role_policy_attachment" "attach_glue_service_role" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}


# Create the KMS key
resource "aws_kms_key" "s3_export_kms_key" {
  description              = "KMS key for exporting RDS snapshots to S3"
  key_usage                = "ENCRYPT_DECRYPT"
  customer_master_key_spec = "SYMMETRIC_DEFAULT"

  tags = {
    Name    = "S3ExportKMS"
    Project = "VEDA"
  }
}

# Create an alias for the KMS key
resource "aws_kms_alias" "s3_export_kms_key_alias" {
  name          = "alias/${var.prefix}-s3-snapshot-export-key"
  target_key_id = aws_kms_key.s3_export_kms_key.id
}
