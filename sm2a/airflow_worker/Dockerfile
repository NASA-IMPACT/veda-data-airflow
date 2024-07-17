FROM --platform=linux/arm64 osgeo/gdal:ubuntu-small-3.6.3
ARG AIRFLOW_VERSION=2.8.4

ARG UNAME=airflow

ARG UID=50000

ARG GID=0

ARG AIRFLOW_HOME=/opt/airflow

RUN groupadd -g $GID -o $UNAME

RUN useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME

WORKDIR /opt/airflow

RUN chown $UNAME:$GID /opt/airflow

RUN apt-get -y update \
        && apt install -y python3-pip \
        && apt-get install -y --no-install-recommends gcc libc6-dev libcurl4-openssl-dev libssl-dev \
        && apt-get autoremove -yqq --purge \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

USER airflow

ENV PATH $PATH:/home/airflow/.local/bin

COPY --chown=airflow:airflow airflow_worker/requirements.txt "${AIRFLOW_HOME}/requirements.txt"

RUN pip install --upgrade pip \
    && pip install "apache-airflow[celery,amazon]==${AIRFLOW_VERSION}" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt" \
    && pip install --no-cache-dir -r requirements.txt -c "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

COPY --chown=airflow:airflow dags "${AIRFLOW_HOME}/dags"
COPY --chown=airflow:airflow plugins "${AIRFLOW_HOME}/plugins"
COPY --chown=airflow:airflow infrastructure/configuration "${AIRFLOW_HOME}/configuration"
COPY --chown=airflow:airflow scripts "${AIRFLOW_HOME}/scripts"

RUN cp ${AIRFLOW_HOME}/configuration/airflow.cfg* ${AIRFLOW_HOME}/.


RUN pip install pypgstac==0.7.4

# ENV
ENV AIRFLOW_HOME ${AIRFLOW_HOME}
ENV TZ UTC
ENV PYTHONPATH /opt/airflow

CMD /bin/bash