import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.param import Param
from airflow.utils.trigger_rule import TriggerRule
from slack_notifications import slack_fail_alert
from veda_data_pipeline.groups.transfer_group import subdag_transfer

dag_doc_md = """
### Discover files from S3
#### Purpose
This DAG transfers files that are to permanent locations for indexing with STAC.
#### Notes
- This DAG can run with a configuration similar to this <br>
```json
{
    "origin_bucket": "covid-eo-dashboard",
    "origin_prefix": "s3-prefix/",
    "filename_regex": "^(.*).tif$",
    "target_bucket": "target_s3_bucket",
    "collection": "collection-id",
    "cogify": false,
    "dry_run": true
}
```
- [Supports linking to external content](https://github.com/NASA-IMPACT/veda-data-pipelines)
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "on_failure_callback": slack_fail_alert,
    "doc_md": dag_doc_md,
}

templat_dag_run_conf = {
    "origin_bucket": "<bucket>",
    "origin_prefix": "<prefix>/",
    "filename_regex": "<file_regex>",
    "target_bucket": "<target_bucket>",
    "collection": "<collection-id>",
    "cogify": Param(default=False, type="boolean"),
    "dry_run": Param(default=False, type="boolean"),
}

with DAG(
    "veda_transfer", schedule=None, params=templat_dag_run_conf, **dag_args
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

    transfer_grp = subdag_transfer()

    start >> transfer_grp >> end
