Module to query an `s3` bucket to discover COGs 
```bash
docker build -t s3-discovery.
# Currently runs an example for OMI Ozone
docker run s3-discovery python -m handler
```

To run this locally, you may need to pass your AWS credentials to the module: `docker run -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY s3-discovery python -m handler`

AWS Provisioning
This Lambda needs to list the contents of a S3 Bucket in order to discover files.
- Add `s3:ListBucket` to the Lambda's execution role
