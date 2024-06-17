#!/bin/bash

CLOUDFRONT_DIST_ID=$1
WORKFLOWS_API_ORIGIN_ID=$2
WORKFLOWS_API_DOMAIN_NAME=$3

WORKFLOWS_API_DOMAIN_NAME=${WORKFLOWS_API_DOMAIN_NAME#http://}
WORKFLOWS_API_DOMAIN_NAME=${WORKFLOWS_API_DOMAIN_NAME#https://}

# Step 1: Retrieve the current CloudFront distribution configuration
aws cloudfront get-distribution-config --id $CLOUDFRONT_DIST_ID > current-config.json

# Extract the ETag and DistributionConfig for later use
ETAG=$(jq -r '.ETag' current-config.json)
DISTRIBUTION_CONFIG=$(jq '.DistributionConfig' current-config.json)

# Step 2: Modify the configuration
# Add a new origin for the workflows API
MODIFIED_CONFIG=$(echo $DISTRIBUTION_CONFIG | jq --arg origin_id "$WORKFLOWS_API_ORIGIN_ID" --arg domain_name "$WORKFLOWS_API_DOMAIN_NAME" '.Origins.Items += [{"Id": $origin_id, "DomainName": $domain_name, "CustomOriginConfig": {"HTTPPort": 80, "HTTPSPort": 443, "OriginProtocolPolicy": "https-only", "OriginSslProtocols": {"Items": ["TLSv1.2"], "Quantity": 1}, "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5}, "OriginPath": "", "CustomHeaders": {"Quantity": 0}}] | .Origins.Quantity += 1')
# Add a new cache behavior for the workflows API
MODIFIED_CONFIG=$(echo $MODIFIED_CONFIG | jq --arg origin_id "$WORKFLOWS_API_ORIGIN_ID" '.CacheBehaviors.Items += [{"PathPattern": "/api/workflows*", "TargetOriginId": $origin_id, "ViewerProtocolPolicy": "https-only", "AllowedMethods": {"Items": ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"], "Quantity": 7, "CachedMethods": {"Items": ["GET", "HEAD"], "Quantity": 2}}, "SmoothStreaming": false, "CachePolicyId": "4135ea2d-6df8-44a3-9df3-4b5a84be39ad", "OriginRequestPolicyId": "b689b0a8-53d0-40ab-baf2-68738e2966ac", "LambdaFunctionAssociations": {"Quantity": 0},"FunctionAssociations": {"Quantity": 0}, "FieldLevelEncryptionId": "", "Compress": true}] | .CacheBehaviors.Quantity += 1')

# Step 3: Update the CloudFront distribution with the modified configuration
echo $MODIFIED_CONFIG | jq . > modified-config.json
aws cloudfront update-distribution --id $CLOUDFRONT_DIST_ID --if-match $ETAG --distribution-config file://modified-config.json

# Cleanup
rm current-config.json modified-config.json
