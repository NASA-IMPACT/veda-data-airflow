FROM --platform=linux/amd64 ghcr.io/lambgeo/lambda-gdal:3.5-python3.9
RUN yum update -y

WORKDIR /app
ENTRYPOINT []
RUN pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY handler.py handler.py
