output "glue_crawler_role_arn" {
    value = aws_iam_role.glue_crawler_role.arn
}

output "s3_export_kms_key_id" {
    value = aws_kms_key.s3_export_kms_key.id
}

output "snapshot_export_role_arn" {
    value = aws_iam_role.snapshot_export_role.arn
}

output "snapshot_bucket_name" {
    value = aws_s3_bucket.rds_backup_bucket.bucket
}