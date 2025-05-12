#!/usr/bin/env sh

set -e

AIRFLOW_VERSION=2.8.4

pip install --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt" "apache-airflow[celery,amazon]==${AIRFLOW_VERSION}"
pip install -r sm2a/airflow_worker/requirements.txt "apache-airflow[celery,amazon]==${AIRFLOW_VERSION}"
pip install  -r sm2a/airflow_worker/requirements-in.txt apache-airflow==${AIRFLOW_VERSION}
pip install -r sm2a/test-requirements.txt "apache-airflow[celery,amazon]==${AIRFLOW_VERSION}"
