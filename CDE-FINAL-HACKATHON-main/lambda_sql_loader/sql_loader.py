import os
import io
import json
import boto3
import pandas as pd
import pyodbc
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET = os.environ.get("BUCKET", "data-hackathon-smit-abdulrehman810")
SQL_SERVER = os.environ.get("SQL_SERVER")
SQL_DATABASE = os.environ.get("SQL_DATABASE")
SQL_USER = os.environ.get("SQL_USER")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD")

def sql_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD}"
    )
    return pyodbc.connect(conn_str)

def load_to_sql(df):
    conn = sql_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO StockPrices
        (symbol, Datetime, Open, High, Low, Close, Volume, ingest_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row["symbol"],
            row["Datetime"],
            row["Open"],
            row["High"],
            row["Low"],
            row["Close"],
            row["Volume"],
            row["ingest_timestamp"],
        ))

    conn.commit()
    cursor.close()
    conn.close()

def process_record(message):
    body = json.loads(message["body"])
    record = body["Records"][0]

    key = record["s3"]["object"]["key"]
    bucket = record["s3"]["bucket"]["name"]

    logger.info(f"Processing file: s3://{bucket}/{key}")

    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_data = obj["Body"].read()

    df = pd.read_csv(io.BytesIO(csv_data))
    df["ingest_timestamp"] = pd.Timestamp.utcnow()

    load_to_sql(df)

def lambda_handler(event, context):
    logger.info("Received SQS event")
    logger.info(json.dumps(event))

    for record in event["Records"]:
        process_record(record)

    return {"status": "done"}
