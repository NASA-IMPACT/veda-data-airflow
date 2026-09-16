# Vector Ingestion Pipeline

## Overview
The vector ingestion pipeline is designed to transform and store spatial vector datasets in an RDS (PostgreSQL) database. It accepts **CSV** or **GeoJSON** files and, based on the provided DAG configuration, ingests them into the database. Once the data is stored, the **Features API** will automatically include it in the list of collections if it is a spatial dataset.

**Code Repository:** [veda-data-airflow](https://github.com/NASA-IMPACT/veda-data-airflow/tree/dev)

---

## Pipeline Process

### 1. Discovering Files from S3
The transformed files are stored in a designated S3 location, specified as `prefix` in the DAG configuration. The DAG will:
- Discover files matching the `filename_regex` provided in the configuration.
- Pass the list of discovered files to the next task via **XCom**.

### 2. Ingesting Vector Data
This process retrieves the `collection` name from the DAG configuration. If the `collection` name is empty, the `id_template` is used as the collection name. The collection name is required to define the table/collection in the database or Features API.

#### **File Processing Methods**
Depending on how the data needs to be ingested, different approaches can be taken:

#### **Creating Separate Collections for Each File (Commonly Used)**
- If each discovered file should be stored as a **separate collection (table)**, leave the `collection` name **empty**.
- The `id_template` will be used to name the tables.
- The filename (excluding the extension) will be used to create a collection.
- If `append` is included in `extra_flags`, new data will be **appended** to the collection.
- If `overwrite` is included, it will **replace existing data** in the collection.

#### **Merging Multiple Files into One Collection**
- If multiple files are discovered and should be combined into a **single collection (one table)**, specify the `collection` in the DAG configuration.
- Add `append` to `extra_flags` to merge all discovered files into one collection.
- Using `overwrite` in this scenario does not make sense, as it would overwrite the collection with each file, retaining only the last executed file.

---

### 3. Configuring the Table (Optional)
After **every** discovered file has been ingested, an optional `table_config` block applies
index and statistics DDL to the collection. Omit the key entirely to skip this step.

```json
{
  "collection": "hms_smoke",
  "table_config": {
    "schema": "public",
    "indexes": [
      {"columns": ["datetime"], "method": "btree", "concurrently": true}
    ],
    "analyze": true
  }
}
```

| Field | Description |
|-------|-------------|
| `schema` | Table schema, default `public`. |
| `indexes` | List of indexes. Each needs `columns`; `method` (default `btree`), `concurrently` (default `true`) and `name` are optional. |
| `analyze` | Run `ANALYZE` afterwards, default `true`. Without statistics the planner will not use the new indexes. |

**Why this runs after ingest, not before.** Building an index in one bulk sort is cheaper
than maintaining it row by row while data loads. The task sits downstream of the mapped
ingest tasks, so it runs once on the finished table rather than once per file.

**Notes**

- Requires an explicit `collection`. With a per-file `id_template` there is no single
  table to configure and the step is skipped.
- `ogr2ogr` already creates the GiST index on the geometry column and has no option for
  an index on any other column; this covers the rest.
- `-overwrite` drops and recreates the table, destroying its indexes, so they are rebuilt
  on each ingest.
- `concurrently` defaults to `true` so the build does not hold an `ACCESS EXCLUSIVE` lock
  on a table the Features API is serving — a blocking build on a large table is a visible
  outage, not just a slow ingest.
- **Backfilling across several DAG runs:** omit `table_config` from the intermediate runs
  and set it only on the last one. Otherwise the index is created after the first run and
  every later chunk loads into an indexed table, which is exactly what the post-ingest
  ordering is meant to avoid. Within a *single* run this is handled automatically, since
  all files are ingested by mapped tasks before this step runs.

---

### 4. Internal Processing with ogr2ogr
Internally, the ingestion task uses the **ogr2ogr** command, a command-line tool from the **GDAL** library, to convert and process geospatial data between various formats. The data is imported into a **PostgreSQL** database with **PostGIS** extensions for spatial data.

#### **Supported Input Formats**
- GeoJSON
- CSV
- Other geospatial data formats

For more details, refer to the [GDAL ogr2ogr documentation](https://gdal.org/programs/ogr2ogr.html).

---

## Useful ogr2ogr Options
| Option | Description |
|--------|-------------|
| `<layer_name>` | Defines the layer name. |
| `-s_srs` | Specifies the source spatial reference system. |
| `-t_srs` | Specifies the target spatial reference system. |
| `-nln` | Sets the new layer name. |
| `-overwrite` | Replaces existing data in the target collection. |
| `-append` | Appends new data to an existing collection. |
| `-oo X_POSSIBLE_NAMES=longitude` | Specifies possible column names for the X (longitude) coordinate in CSV files. |
| `-oo Y_POSSIBLE_NAMES=latitude` | Specifies possible column names for the Y (latitude) coordinate in CSV files. |

---

## Example DAG Configurations

### **GeoJSON Ingestion**
```json
{
  "bucket": "veda-data-store-dev",
  "collection": "",
  "filename_regex": ".*geojson",
  "id_template": "{}",
  "prefix": "cyclone/ascatc_wind/",
  "vector": true,
  "invalidate_cloudfront": true
}
```
Note: We don't need X_POSSIBLE_NAMES and Y_POSSIBLE_NAMES here because GeoJSON inherently contains geometry that ogr2ogr can recognize.

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
Note: If you have `-` in the collection name or filenames, it will automatically convert that to `_`. It will also automatically convert all uppercases to lowercases in the table name.

Note: `"invalidate_cloudfront": true` is the default value, that will invalidate the cloudfront cache after the ingestion is done

