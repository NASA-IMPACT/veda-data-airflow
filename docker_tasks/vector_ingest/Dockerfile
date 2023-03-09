FROM --platform=linux/amd64 ghcr.io/lambgeo/lambda-gdal:3.5-python3.9

WORKDIR /app

RUN pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN find . -type f -name '*.pyc' | while read f; do n=$(echo $f | sed 's/__pycache__\///' | sed 's/.cpython-[2-3][0-9]//'); cp $f $n; done;
RUN find . -type d -a -name '__pycache__' -print0 | xargs -0 rm -rf
RUN find . -type f -a -name '*.py' -print0 | xargs -0 rm -f
RUN find . -type d -a -name 'tests' -print0 | xargs -0 rm -rf
RUN echo "Remove lambda python packages"
RUN rm -rdf ./numpy/doc/
RUN rm -rdf ./stack
RUN rm -rdf ./docutils*
RUN ln -s /var/lang/bin/python3 /usr/local/bin/python

COPY handler.py handler.py
