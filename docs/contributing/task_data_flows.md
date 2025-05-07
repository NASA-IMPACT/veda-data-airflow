# Airflow Task Design Guide

*A concise reference for contributors designing, implementing, and reviewing tasks in SM2A projects.*

---

## 1 Airflow Task Design Goals

| # | Goals | What it means | Why it matters |
| - | - | - | - |
| 1 | **Explicit parameters**     | Declare every input (e.g., `bucket: str`, `run_date: datetime`) as a named argument in the TaskFlow function signature. | Readers (and IDEs) know exactly what values the task needs; type hints enable linting & autocomplete. |
| 2 | **Direct parameter access** | Pass scalar / simple objects directly—avoid wrapping them in catch‑all dicts or `**kwargs`. | Prevents “mystery meat” payloads and accidental hidden dependencies. |
| 3 | **Multiple named outputs**  | Return a `dict` of discrete outputs via `return {"records": df, "count": len(df)}` **or** leverage TaskFlow’s multiple return feature (`return record_count, df`). | Down‑stream tasks can pull *only* what they need; avoids parallel parsing of giant blobs. |
| 4 | **TaskFlow‑first** | Define tasks with `@task` (TaskFlow) rather than classic operators when writing Python logic. | Native Python makes tasks testable with `pytest` and keeps DAG files declarative.                     |
| 5 | **Separation of concerns**  | Task functions orchestrate **data flow & I/O**; heavy computation lives in pure *utility* functions/modules imported by the task. | Compute logic can be unit‑tested in isolation and reused in scripts / notebooks. |
| 6 | **Idempotency** | Tasks should safely re‑run without corrupting state; leverage run‑date‑based keys, checksums, or existence checks. | Supports retries & backfills. |
| 7   | **Observability** | Use Airflow’s `task_instance.log` plus structured logging (e.g., JSON) for key metrics, parameters, and row counts. | Easier debugging & monitoring. |

---

## 2  Recommended Patterns

### 2.1 Minimal TaskFlow Example

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

### 2.2 Multiple outputs with `@task(multiple_outputs=True)`

Use when returning more than one value so Airflow stores each key separately in XCom:

```python
@task(multiple_outputs=True)
def split_dataset(path: str) -> dict[str, str]:
    train, test = make_splits(path)
    return {"train_path": train, "test_path": test}
```

Down‑stream tasks access exactly what they need:

```python
train_model(train_path=split_dataset()["train_path"])
```

### 2.3 Delegating compute to utils

```python
@task(multiple_outputs=True)
def build_collection(collection_id: str, description: str) -> dict[str, str]:
    """Generate a STAC collection body and return both body and ID."""
    collection_body = generate_collection(
        collection_id=collection_id,
        description=description,
    )  # heavy lifting happens in utils
    return {"collection_body": collection_body, "collection_id": collection_id}
```

*Benefits*: `compute_sales_totals` can be unit‑tested with simple inputs; the task stays small.

---

## 3  Anti‑Patterns to Avoid

| Anti‑Pattern                                                                                        | Why it hurts                                                                     |
| --------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| **Monolithic payloads**: stuffing dozens of values into a dict or JSON and passing as a single XCom | Downstream tasks must deserialize and know key names; accidental tight coupling. |
| **Hidden ABI**: accessing fields on `kwargs["ti"].xcom_pull()` inside tasks                         | Hides dependencies; makes signatures lie; breaks static analysis & tests.        |
| **Heavy logic in DAG file**: performing data transformations directly in the DAG definition         | Bloats git diffs; complicates refactors; hampers testability.                    |
| **Non‑idempotent side effects**: writing to the same table without partitioning by `run_id` or date | Retries/backfills cause duplicate rows or data loss.                             |

---

## 4  Further Reading
TODO links
* Airflow 2 TaskFlow API docs
* “Designing a Production‑Ready DAG” – Astronomer blog
* **SM2A** project `docs/` → *DAG Best Practices*

---

> *Have suggestions or questions?* Open an issue or reach out in **#veda-data-services** on Slack.
