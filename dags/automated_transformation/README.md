# DAG Configuration Documentation

## DAG Overview

**DAG ID:** `automate-cog-transformation`

**Description:** This DAG automates the transformation of raw geospatial data into Cloud-Optimized GeoTIFFs (COGs). It
fetches transformation plugins, discovers files, processes them, and generates a report on the transformation results.

**Tags:** Transformation, Report

**Schedule:** Not scheduled (triggered manually)

**Catchup:** Disabled

**On Failure Callback:** Sends alerts via Slack (`slack_fail_alert`)

---

## DAG Parameters

The DAG accepts several parameters to configure its execution dynamically.

| Parameter                 | Type     | Default Value                                                                | Description                                                                      |
|---------------------------|----------|------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| `data_acquisition_method` | `enum`   | `s3`                                                                         | Defines the data acquisition method. Currently supports only `s3`.               |
| `plugins_uri`             | `string` | `https://raw.githubusercontent.com/US-GHG-Center/ghgc-docs/refs/heads/main/` | The base URI for fetching transformation plugins.                                |
| `raw_data_bucket`         | `string` | `ghgc-data-store-develop`                                                    | S3 bucket where raw data is stored.                                              |
| `raw_data_prefix`         | `string` | `delivery/gpw`                                                               | Prefix for locating raw data in the S3 bucket. Must not start or end with a `/`. |
| `dest_data_bucket`        | `string` | `ghgc-data-store-develop`                                                    | Destination S3 bucket where transformed COGs are stored.                         |
| `data_prefix`             | `string` | `transformed_cogs`                                                           | Prefix for storing transformed COGs. Must not start or end with a `/`.           |
| `collection_name`         | `string` | `gpw`                                                                        | The name of the data collection being processed.                                 |
| `nodata`                  | `number` | `-9999`                                                                      | No-data value used during transformation.                                        |
| `ext`                     | `string` | `.nc`                                                                        | File extension of input data files. Must start with a dot (`.`).                 |

---

## DAG Tasks

### 1. `start`

A dummy task marking the start of the DAG execution.

### 2. `check_function_exists`

- **Purpose:** Checks if the transformation plugin for the specified collection exists.
- **Process:**
    - Constructs the plugin URL based on `plugins_uri` and `collection_name`.
    - Attempts to download the file to verify its existence.
- **Failure Handling:** Raises an exception if the file is not found.

### 3. `discover_files`

- **Purpose:** Identifies raw data files in the S3 bucket matching the specified prefix and extension.
- **Process:**
    - Lists all files in `raw_data_bucket` with `raw_data_prefix` and `ext`.
    - Splits the list into chunks (max 900 per chunk).
- **Output:** Returns a list of file chunks to be processed in parallel.

### 4. `process_files`

- **Purpose:** Transforms each discovered file into a Cloud-Optimized GeoTIFF (COG).
- **Process:**
    - Retrieves DAG parameters.
    - Calls the `transform_cog` function with the appropriate inputs.
- **Concurrency Limit:** Only one task instance can be active at a time (`max_active_tis_per_dag=1`).
- **Output:** Returns the transformation status for each file (success or failure).

### 5. `generate_report`

- **Purpose:** Summarizes transformation results.
- **Process:**
    - Counts successful and failed transformations.
    - Raises an exception if any failures occurred.
- **Output:** Returns a summary dictionary with success and failure counts.

### 6. `end`

A dummy task marking the end of the DAG execution.

---

## DAG Flow

1. `start` → `check_function_exists`
2. `check_function_exists` → `discover_files`
3. `discover_files` → `process_files` (expands over discovered file chunks)
4. `process_files` → `generate_report`
5. `generate_report` → `end`

---

## Failure Handling

- If `check_function_exists` fails, the DAG stops execution.
- If `process_files` fails for any file, failures are collected in `generate_report`, which raises an exception if any
  exist.
- Slack notifications are triggered if the DAG encounters a failure.

---

## Notes

- The DAG is designed to be manually triggered and does not run on a schedule.
- The transformation logic is dependent on external plugins hosted in a repository.
- The DAG handles large file lists by chunking them to optimize processing.

