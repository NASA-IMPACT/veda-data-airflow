# How to Add a DAG

## Steps
1. Copy the template DAG file from the `dags` directory
2. Rename the file adhering to the following naming conventions
3. Update the DAG file with the necessary configurations, including relevant Tag(s) and Owner Links
4. Configure the DAG with the necessary tasks

### Adding a DAG

The DAGs are defined in Python files located in the [dags](./dags/) directory. Each DAG should be defined as a Python module that defines a DAG object. The DAGs are scheduled by  the [Airflow Scheduler](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html#scheduler). Since we aim to keep the scheduler lightweight, every task-dependent library should be imported in the tasks and not at the DAG level.

Our preferred method of defining DAGs and tasks is to use Taskflow for all Python tasks. An example DAG is shown below:

```python
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@task
def foo_task():
    print("Hello World")
    return "Hello World"

@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def example_dag():
    foo = foo_task()
    bar = EmptyOperator(task_id='bar')
    foo >> bar
```


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
- `Operations` - DAGs that are used for operational purposed, not for ingesting new data (fr example, scheduling and restoring backups)
- `Collection` - DAGs that create a new collection in the targeted STAC catalog
- `Discovery` - DAGs that discover and ingest new assets and items
- `Automated` - DAGs that cannot be run manually, and are scheduled to run automatically

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