# Ingestion Pipeline Overview

This pipeline is designed to handle the ingestion of both vector and raster data. The ingestion can be performed using the `veda-discover` DAG. Below are examples of configurations for both vector and raster data. These configurations may change over time - for the most up-to-date information, please refer to the `template_dag_run_conf` provided alongside each DAG. Previously-used configurations may also be found in the [veda-data repository](https://github.com/NASA-IMPACT/veda-data).

## Ingestion Configuration

### Vector Data Ingestion

```json
{
  "bucket": "ghgc-data-store-develop",
  "collection": "",
  "extra_flags": [
    "-overwrite",
    "-oo",
    "X_POSSIBLE_NAMES=longitude",
    "-oo",
    "Y_POSSIBLE_NAMES=latitude"
  ],
  "filename_regex": ".*metadata.*csv",
  "id_template": "any_prefix_{}",
  "prefix": "transformed_csv/NOAA/",
  "source_projection": "EPSG:4326",
  "target_projection": "EPSG:4326",
  "vector": true,
  "invalidate_cloudfront": true
}
```

### Raster Data Ingestion

```json
{
    "collection": "",
    "bucket": "",
    "prefix": "",
    "filename_regex": ".*.tif$",
    "datetime_range": "",
    "assets": {
        "co2": {
            "title": "",
            "description": ".",
            "regex": ".*.tif$"
        }
    },
    "id_regex": ".*_(.*).tif$",
    "id_template": "-{}"
}

```

## Configuration Fields Description

- `collection`: The collection_id of the raster or vector data.
- `bucket`: The name of the S3 bucket where the data is stored.
- `prefix`: The location within the bucket where the files are to be discovered.
- `filename_regex`: A regex expression used to filter files based on naming patterns.
- `id_template`: The format used to create item identifiers in the system.
- `vector`: Set to true to trigger the generic vector ingestion pipeline.
- `vector_eis`: Set to true to trigger the EIS Fire specific vector ingestion pipeline.

## Pipeline Behaviour

Since this pipeline can ingest both raster and vector data, the configuration can be modified accordingly. The `"vector": true` triggers the `generic_ingest_vector` dag. If the `collection` is provided, it uses the collection name as the table name for ingestion (recommended to use `append` extra_flag when the collection is provided). When no `collection` is provided, it uses the `id_template` and generates a table name by appending the actual ingested filename to the id_template (recommended to use `overwrite` extra flag).
