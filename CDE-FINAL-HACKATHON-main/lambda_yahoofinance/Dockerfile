FROM python:3.11-slim

WORKDIR /var/task

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    libssl-dev \
    libffi-dev \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir yfinance pandas numpy requests boto3 awslambdaric

COPY app.py /var/task/app.py

CMD ["python3", "-m", "awslambdaric", "app.lambda_handler"]
