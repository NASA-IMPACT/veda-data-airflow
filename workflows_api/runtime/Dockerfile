FROM public.ecr.aws/lambda/python:3.9

COPY . ./runtime
RUN pip install -r ./runtime/requirements.txt

CMD [ "runtime.handler.handler" ]