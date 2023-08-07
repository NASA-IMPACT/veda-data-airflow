## Build STAC function

Code intended to receive an event which contains all the data necessary to build a STAC Item. This STAC item is generated and written to a JSON file and uploaded to s3.

```bash
docker build --platform=linux/amd64 -t build-stac .
# Runs an example in handler.py
docker run \
    -v $HOME/.aws/credentials:/root/.aws/credentials:ro \
    --env AWS_PROFILE=XXX \
    --env BUCKET=XXX \
    --rm -it \
    build-stac python -m handler
```
