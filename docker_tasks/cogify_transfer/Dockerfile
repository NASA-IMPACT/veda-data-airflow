FROM --platform=linux/amd64 python:3.9-slim-bullseye as production
WORKDIR /app
# Install Python dependencies
RUN pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN rm requirements.txt
COPY handler.py .
# Precompile
RUN python -m compileall .
RUN apt-get update && \
    apt-get install -y nano
RUN pip install ipython
ENV GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR GDAL_MAX_DATASET_POOL_SIZE=1024 GDAL_CACHEMAX=1024000000 GDAL_HTTP_MAX_RETRY=5 GDAL_HTTP_RETRY_DELAY=3
