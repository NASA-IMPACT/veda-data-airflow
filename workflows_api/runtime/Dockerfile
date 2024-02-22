FROM public.ecr.aws/lambda/python:3.9

COPY ./requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt

COPY . ${LAMBDA_TASK_ROOT}

CMD [ "lambda_function.handler" ]