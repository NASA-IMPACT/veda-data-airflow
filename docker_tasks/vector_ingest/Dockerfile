FROM --platform=linux/amd64 python:3.9

WORKDIR /app

RUN pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY handler.py handler.py
