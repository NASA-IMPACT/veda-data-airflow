FROM --platform=linux/amd64 osgeo/gdal:alpine-normal-latest
RUN apk add py3-pip

WORKDIR /app

RUN pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY handler.py handler.py
