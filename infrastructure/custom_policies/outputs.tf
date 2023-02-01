output "custom_policy_arns_map" {
  value = {
    "custom_policy_arns" = aws_iam_policy.read_data.arn
  }
}

output "custom_policy" {
  value = {
    "arn" = aws_iam_policy.read_data.arn
  }
}