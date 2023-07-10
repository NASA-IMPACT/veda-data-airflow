# DB Write

Test with python locally (uses example data in [hlss30_stac_example.ndjson](./hlss30_stac_example.ndjson))

```bash
python -m handler
```

Build and test the docker image:

```bash
# From the root of this repository
docker build -t db_write -f lambdas/db-write/Dockerfile .
export COGNITO_APP_SECRET=veda-auth-stack-dev/data-pipelines
# TODO (aimee): This isn't currently working because we are getting an import error for boto3
# docker run --env "COGNITO_APP_SECRET=$COGNITO_APP_SECRET" python -m handler
```
