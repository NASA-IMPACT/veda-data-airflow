# Operating Guide: Data Transformation and Ingest for VEDA

This guide provides information on how VEDA runs data ingest, transformation and metadata (STAC) publication workflows via AWS Services, such as step functions.

NOTE: Since collection ingest still requires calling the database from a local machine, users must add their IP to an inbound rule on the security group attached to the RDS instance.

## Data

### `collections/`

The `collections/` directory holds json files representing the data for VEDA collection metadata (STAC).

Should follow the following format:

```json
{
    "id": "<collection-id>",
    "type": "Collection",
    "links":[
    ],
    "title":"<collection-title>",
    "extent":{
        "spatial":{
            "bbox":[
                [
                    "<min-longitude>",
                    "<min-latitude>",
                    "<max-longitude>",
                    "<max-latitude>",
                ]
            ]
        },
        "temporal":{
            "interval":[
                [
                    "<start-date>",
                    "<end-date>",
                ]
            ]
        }
    },
    "license":"MIT",
    "description": "<collection-description>",
    "stac_version": "1.0.0",
    "dashboard:is_periodic": "<true/false>",
    "dashboard:time_density": "<month/>day/year>",
    "item_assets": {
        "cog_default": {
            "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            "roles": [
                "data",
                "layer"
            ],
            "title": "Default COG Layer",
            "description": "Cloud optimized default layer to display on map"
        }
    }
}

```

### `items/`

The `items/` directory holds json files representing the airflow inputs for initiating discovery, ingest and publication workflows for ingesting items.
Can either be a single input event or a list of input events.

Should follow the following format:

```json
{
    "collection": "<collection-id>",
    "discovery": "<s3/cmr>",

    ## for s3 discovery
    "prefix": "<s3-key-prefix>",
    "bucket": "<s3-bucket>",
    "filename_regex": "<filename-regex>",
    "datetime_range": "<month/day/year>",
    
    ## for cmr discovery
    "version": "<collection-version>",
    "temporal": ["<start-date>", "<end-date>"],
    "bounding_box": ["<bounding-box-as-comma-separated-LBRT>"],
    "include": "<filename-pattern>",
    
    ### misc
    "cogify": "<true/false>",
    "upload": "<true/false>",
    "dry_run": "<true/false>",
}
```

## Ingestion

Install dependencies:

```bash
poetry install
```

### Ingesting a collection

Create a collection json file in the `data/collections/` directory. For format, check the [data](#data) section.

```bash
./scripts/collection.sh .env <event-json-start-pattern>
```

### Ingesting items to a collection

Create an input json file in the `data/items/` directory. For format, check the [data](#data) section.

```bash
./scripts/item.sh .env <event-json-start-pattern>
```


### Updating collection summaries


## Glossary

### Tasks

#### 1. s3-discovery

Discovers all the files in an S3 bucket, based on the prefix and filename regex.

#### 2. cmr-query

Discovers all the files in a CMR collection, based on the version, temporal, bounding box, and include. Returns objects that follow the specified criteria.

#### 3. cogify

Converts the input file to a COG file, writes it to S3, and returns the S3 key.

#### 4. data-transfer

Copies the data to the VEDA MCP bucket if necessary.

#### 5. build-stac

Given an object received from the `STAC_READY_QUEUE`, builds a STAC Item, writes it to S3, and returns the S3 key.

#### 6. submit-stac

Submits STAC items to STAC Ingestor system via POST requests.

### MWAA Workflows

#### 1. Discover

Discovers all the files that need to be ingested either from s3 or cmr.

#### 2. Cogify

Converts the input files to COGs, runs in parallel.

#### 3. Publication

Publishes the item to the STAC database (and MCP bucket if necessary).
