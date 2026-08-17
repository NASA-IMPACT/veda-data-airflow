# Airflow Task Design Guide

A concise reference for contributors designing, implementing, and reviewing tasks in SM2A projects.*

## Airflow Task Design Goals

| #  | Goals | What it means | Why it matters |
| - | - | - | - |
| 1 | **Explicit parameters** | Declare every input (e.g., `bucket: str`, `run_date: datetime`) as a named argument in the TaskFlow function signature. | Readers (and IDEs) know exactly what values the task needs; type hints support linting & autocompletion. |
| 2 | **Direct parameter access** | Pass scalar / simple objects directly—avoid wrapping them in catch‑all dicts or `**kwargs`. | Prevents “mystery meat” payloads and accidental hidden dependencies. |
| 3 | **Multiple named outputs** | Return a `dict` of discrete outputs via `return {"records": df, "count": len(df)}`, leveraging TaskFlow’s multiple return feature (this can be implicit by returning a dict, or explicit with `@task(multiple_outputs=True)`). | Downstream tasks can pull *only* what they need and don't need to parse larger objects. |
| 4 | **TaskFlow‑first** | Define tasks with `@task` (TaskFlow) rather than classic operators when writing Python tasks. | Makes tasks testable with `pytest` and keeps DAGs readable. |
| 5 | **Separation of concerns** | Task functions orchestrate **data flow and execution**; computation and logic lives in `util` functions/modules imported by the task. | Logic can be unit‑tested in isolation and reused in other tasks. |
| 6 | **Idempotency** | Tasks should safely re‑run without corrupting state; leverage run‑date‑based keys, checksums, or existence checks. | Supports retries & backfills. |

## Recommended Patterns

### Minimal TaskFlow Example

```python
from airflow.decorators import dag, task
from pendulum import datetime
from utils.stac import generate_collection  # util function (external)

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={  # DAG‑level parameters accessible to the first task
        "collection_id": "sample-collection",
        "description": "Sample STAC collection generated via Airflow",
    },
    tags=["stac", "example"],
)
def stac_collection_dag(params=None):  # Airflow injects params dict

    @task(multiple_outputs=True)
    def build_collection(collection_id: str, description: str) -> dict[str, str]:
        """Generate a STAC collection body and return both body and ID."""
        collection_body = generate_collection(
            collection_id=collection_id,
            description=description,
        )  # heavy lifting happens in utils
        return {"collection_body": collection_body, "collection_id": collection_id}

    # Task invocation – passing explicit params pulled from dag.params
    outputs = build_collection(
        collection_id=params["collection_id"],
        description=params["description"],
    )

    @task()
    def publish_collection(collection_body: dict):
        """Pass collection to ingestion API."""
        ingest_collection(collection_body)

    publish_collection(collection_body=outputs["collection_body"])

stac_collection_dag()
```

*Key takeaways:* explicit arg names, `multiple_outputs`, util functions (`fetch_api_events`, `normalize_events`, `write_to_warehouse`).

### Multiple outputs with `@task(multiple_outputs=True)`

Use when returning more than one value so Airflow stores each key as a separate XCom value:

```python
@task(multiple_outputs=True)
def split_dataset(path: str) -> dict[str, str]:
    train, test = make_splits(path)
    return {"train_path": train, "test_path": test}
```

Down‑stream tasks access exactly what they need:

```python
training_data, test_data = split_dataset(path="s3://bucket/data.csv")
train_model(train_path=training_data) # only need training data from the first task
test_model(model=train_model, test_path=test_data) # only need test data from the first task
```

### Delegating compute to utils

```python
from veda.utils.stac import generate_collection  # example util function

@task(multiple_outputs=True)
def build_collection(collection_id: str, description: str) -> dict[str, str]:
    """Generate a STAC collection body and return both body and ID."""
    collection_body = generate_collection(
        collection_id=collection_id,
        description=description,
    )  # logic is contained in util function
    return {"collection_body": collection_body, "collection_id": collection_id}
```

## Anti‑Patterns to Avoid

| Anti‑Pattern | Why to avoid |
| --------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| **Monolithic payloads**: outputting multiple values into a dict or JSON and passing to next task as a single XCom | Downstream tasks must deserialize and know key names; incidental tight coupling between tasks. |
| **Hidden parameters**: accessing fields on `kwargs["ti"].xcom_pull()` (or similar) inside tasks | Hides dependencies; makes signatures lie; breaks static analysis & tests. |
| **Heavy logic in DAG file**: performing data transformations directly in the DAG definition | Complicates refactors; hampers testability; Increases DAG parse time |
| **Non‑idempotent side effects**: tasks must be idempotent - each task does one thing, and can be reversed or retried independently | Retries/backfills can cause duplicated data or data loss. |

## Further Reading

* [Airflow 2 TaskFlow API docs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)
* "DAG writing best practices in Apache Airflow" – [Astronomer article](https://www.astronomer.io/docs/learn/dag-best-practices/)
* [Adding a DAG](docs/contributing/add_a_general_dag.md)
