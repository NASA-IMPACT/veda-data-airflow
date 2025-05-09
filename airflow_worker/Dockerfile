FROM --platform=linux/amd64 ghcr.io/osgeo/gdal:ubuntu-small-3.6.4
COPY --from=ghcr.io/astral-sh/uv:0.7.3 /uv /uvx /bin/

ARG AIRFLOW_VERSION=2.10.5
ARG UNAME=airflow
ARG UID=50000
ARG GID=0
ARG AIRFLOW_HOME=/opt/airflow

RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME

WORKDIR /opt/airflow

RUN chown $UNAME:$GID /opt/airflow

RUN apt-get -y update \
        && apt-get install -y --no-install-recommends gcc libc6-dev libcurl4-openssl-dev libssl-dev \
        && apt-get autoremove -yqq --purge \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:airflow airflow_worker/requirements.txt "${AIRFLOW_HOME}/requirements.txt"
COPY --chown=airflow:airflow airflow_worker/requirements-in.txt "${AIRFLOW_HOME}/requirements-in.txt"

RUN uv python install 3.11
RUN uv venv

RUN uv pip install "apache-airflow[celery,amazon]==${AIRFLOW_VERSION}" -c "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"\
    && uv pip install --no-cache-dir -r requirements.txt -c "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"\
    && uv pip install --no-cache-dir -r requirements-in.txt apache-airflow==${AIRFLOW_VERSION}

ENV PATH=".venv/bin:$PATH"

COPY --chown=airflow:airflow dags "${AIRFLOW_HOME}/dags"
COPY --chown=airflow:airflow plugins "${AIRFLOW_HOME}/plugins"
COPY --chown=airflow:airflow infrastructure/configuration "${AIRFLOW_HOME}/configuration"
COPY --chown=airflow:airflow scripts "${AIRFLOW_HOME}/scripts"

RUN cp ${AIRFLOW_HOME}/configuration/airflow.cfg* ${AIRFLOW_HOME}/.

# ENV
ENV AIRFLOW_HOME ${AIRFLOW_HOME}
ENV TZ UTC
ENV PYTHONPATH /opt/airflow
ENV PYSTAC_STAC_VERSION_OVERRIDE=1.0.0

CMD /bin/bash
