import logging

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import os
import importlib.util
import json

def convert_notebook_and_import_function(notebook_path, function_name):
    os.system(f'jupyter nbconvert --to python {notebook_path}')
    script_path = notebook_path.replace('.ipynb', '.py')
    spec = importlib.util.spec_from_file_location("module.name", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, function_name)

def get_notebook_name(**context):
    manifest = { # test data - assume this comes from input
        'datetime_nb': 's3://isayah-veda/my_notebook.ipynb'
    }
    notebook_name = 's3://isayah-veda/my_notebook.ipynb' # manifest.get('datetime_nb')
    if notebook_name:
        context['task_instance'].xcom_push(key='notebook_name', value=notebook_name)


def execute_notebook_function(ti):
    s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
    print("this is xcom: ", ti.xcom_pull(task_ids='load_manifest'))
    file_obj = s3_hook.get_key(
        key=ti.xcom_pull(key='notebook_name', task_ids='load_manifest'),
        bucket_name='isayah-veda'
    )
    file_obj.download_file('/path/to/local/file')
    user_func = convert_notebook_and_import_function('/path/to/local/file', 'user_defined_function')
    result = user_func()
    return result

def log_datetime(ti):
    result = ti.xcom_pull(task_ids='execute_notebook_function')
    logging.info(result)



with DAG(
    dag_id="notebook_poc_ingest",
    start_date=pendulum.today("UTC").add(days=-1),
    schedule_interval=None,
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    get_notebook_name_op = PythonOperator(
        task_id="load_manifest",
        python_callable=get_notebook_name,
        op_kwargs={"text": "Get Notebook Name"},
        dag=dag,
        provide_context=True,
    )

    execute_func_op = PythonOperator(
        task_id="execute_notebook_function",
        python_callable=execute_notebook_function,
        op_kwargs={"text": "Execute Notebook Function"},
        dag=dag,
    )

    log_datetime_op = PythonOperator(
        task_id="log_datetime",
        python_callable=log_datetime,
        op_kwargs={"text": "Log Datetime"},
        dag=dag,
    )

    end = EmptyOperator(task_id="end", dag=dag)
    
    start >> get_notebook_name_op >> execute_func_op >> log_datetime_op >> end
    
