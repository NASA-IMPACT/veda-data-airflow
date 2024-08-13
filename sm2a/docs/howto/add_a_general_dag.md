# How to Add a General DAG
> A general DAG will be standalone or separate from the dynamic Vendor ETL Pipeline DAGs. General purposes may include data quality checks, data profiling, and other utility tasks. In the event that a DAG is vendor-specific but not a fit for the Vendor ETL Pipeline, let's consider it a general DAG.

## Steps
1. Copy the template DAG file from the `dags` directory
2. Rename the file adhering to the following naming conventions
3. Update the DAG file with the necessary configurations, including relevant Tag(s) and Owner Links
4. Configure the DAG with the necessary tasks


## Naming Conventions

### DAG File & Class Name
- `<noun/subject>_<verb/method>_<qualifier>_` - for general DAGs where:
  - `<noun/subject>` is the subject of the DAG
  - `<verb/method>` is the action the DAG performs
  - `<qualifier>` is an optional qualifier to differentiate DAGs with the same subject and verb (action)
  - Example: `metadata_monitor_`
- `v_<noun>_<verb>_<vendor>` - similar to the general DAG pattern, but for vendor-specific DAGs that don't qualify for the Dynamic Vendor ETL Pipeline (i.e. `v_data_unzip_maxar`)
- `util_` - for utility files that can be shared across multiple DAGs (e.g. `util_s3file_check_`) 

### Tags
- `<VendorName>` - for vendor-specific DAGs (e.g. `Maxar` or `Planet`)
- `AWS` - for interactions with AWS services 
- `ETL` - for ETL tasks outside of the Dynamic Vendor ETL Pipeline
- `QAQC` - for data quality checks
- `Template` - for templates

### General Principles
- **Keep things simple**. If a DAG is too complex, its scheduling performance may be impacted. This includes a DAG's structure: simple linear DAGs (A -> B -> C) are preferred over deeply nested DAGs that may incur delays in scheduling ([reference](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#reducing-dag-complexity)).
- **Write efficient Python code**.
- **Avoid Top-Level Code in the DAG file** to avoid scheduling delays, since the scheduler always executes top-level code as it parses a DAG file ([reference](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#best-practices-top-level-code)).
- **Use Airflow Variables or AWS Secrets Manager**. Airflow Variables can store configuration settings that may change over time ([reference](https://airflow.apache.org/docs/apache-airflow/stable/concepts/variables.html)); AWS Secrets Manager can also store variables, as well as sensitive information like passwords and API keys ([reference](https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html)).
- **Avoid storing files locally**. Instead, use XCom for small messages or S3/another cloud storage service to coordinate large messages or data files that are needed between Tasks ([reference](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#communication)).
- **Time and test your DAGs**. Make sure they run as expected and complete within an expected time frame ([reference](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing-a-dag)).

## Additional Resources
- [Apache Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Apache Airflow Concepts](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html)