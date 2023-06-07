from veda_data_pipeline.groups.cog_validation import subdag_cog_validation_task
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
import pendulum
from datetime import timedelta



dag_run_config = {"cog_s3uri": "s3://veda-pipeline-sit-mwaa-853558080719/tifs/20221004_182828_21_227a_3B_AnalyticMS_SR_harmonized_clip.tif",
 "expectations": list(dict({"id": "expect_column_max_to_be_between", "args": {"column": "x", "min_value": 0, "max_value": 1000}}))
 }


dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule_interval": None,
    "catchup": False,
    "dagrun_timeout": timedelta(minutes=60),
}
task_id = "sea_ice_task"

def generate_cog_task(ti):
    config = ti.dag_run.conf
    s3uri = config['cog_s3uri']
    return s3uri

# TODO - add doc_md_DAG to dag_args and remove doc_md_DAG from dag_run_config

doc_md_DAG = """
### <span style="color:blue">Ice spire</span> 
#### This DAG will:
COG Validation rename
<span style="color:red">{
{"cog_s3uri": "s3://veda-pipeline-sit-mwaa-853558080719/tifs/20221004_182828_21_227a_3B_AnalyticMS_SR_harmonized_clip.tif",
 "expectations": [
 {"id": "expect_column_max_to_be_between", "args": {"column": "x", "min_value": 0, "max_value": 1000}}
 ]
 }	
} </span>
"""
with DAG("validate_cog", params=dag_run_config, **dag_args) as dag:
    start = DummyOperator(task_id="Start")
    end = DummyOperator(task_id="End")
    generate_cog = PythonOperator(
        task_id="generate_cog",
        python_callable=generate_cog_task,
        dag=dag,
    )

    start >> generate_cog >> subdag_cog_validation_task(task_id=f"generate_cog") >> end