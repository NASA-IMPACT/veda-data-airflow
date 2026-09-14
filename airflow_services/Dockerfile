FROM --platform=linux/amd64 apache/airflow:slim-3.2.2-python3.12

ARG AIRFLOW_VERSION=3.2.2

USER root
# `apt-get autoremove` is used to remove packages that were automatically installed to satisfy
# dependencies for other packages and are now no longer needed.
# `apt-get clean` clears out the local repository of retrieved package files

RUN apt-get update \
  && apt-get install -y --no-install-recommends gcc libc6-dev libcurl4-openssl-dev libssl-dev git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY --chown=airflow:airflow airflow_services/requirements.txt "${AIRFLOW_HOME}/requirements.txt"
COPY --chown=airflow:airflow airflow_services/requirements-in.txt "${AIRFLOW_HOME}/requirements-in.txt"

USER airflow

RUN uv pip install virtualenv\ 
  && uv pip install --no-cache-dir -r requirements.txt -c "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"\
  && uv pip install --no-cache-dir -r requirements-in.txt apache-airflow==${AIRFLOW_VERSION}

ENV PATH=".venv/bin:$PATH"


COPY --chown=airflow:airflow dags "${AIRFLOW_HOME}/dags"
COPY --chown=airflow:airflow plugins "${AIRFLOW_HOME}/plugins"
COPY --chown=airflow:airflow infrastructure/configuration "${AIRFLOW_HOME}/configuration"
COPY --chown=airflow:airflow scripts "${AIRFLOW_HOME}/scripts"
COPY --chown=airflow:airflow airflow_services/webserver_config.py "${AIRFLOW_HOME}/webserver_config.py"

RUN cp ${AIRFLOW_HOME}/configuration/airflow.cfg* ${AIRFLOW_HOME}/.

#ENV
ENV PYTHONPATH="/opt/airflow"
