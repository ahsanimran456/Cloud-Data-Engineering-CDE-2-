FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY sql_loader.py ${LAMBDA_TASK_ROOT}

CMD ["sql_loader.lambda_handler"]
