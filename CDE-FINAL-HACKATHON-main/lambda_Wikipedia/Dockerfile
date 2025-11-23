FROM python:3.11-slim

WORKDIR /var/task

RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pandas requests lxml html5lib boto3 awslambdaric

COPY app.py /var/task/app.py

CMD ["python3", "-m", "awslambdaric", "app.lambda_handler"]
