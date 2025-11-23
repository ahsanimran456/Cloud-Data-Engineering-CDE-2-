import os
import io
import json
import boto3
import pandas as pd
import pytz
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET = os.environ.get("BUCKET")
STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "staging/yahoofinance")

def clean_df(df):
    df.columns = [c.strip() for c in df.columns]

    if "Datetime" not in df.columns:
        df["Datetime"] = pd.Timestamp.now(tz=pytz.UTC)
    else:
        df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")

    numeric = ["Open","High","Low","Close","Volume"]
    for col in numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ingest_timestamp"] = pd.Timestamp.now(tz=pytz.UTC)
    return df

def write_parquet(df, key):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(df)
    out_buffer = io.BytesIO()
    pq.write_table(table, out_buffer, compression="snappy")

    out_buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=out_buffer.read())

def process_sqs(event_body):
    rec = event_body["Records"][0]
    bucket = rec["s3"]["bucket"]["name"]
    key = rec["s3"]["object"]["key"]

    obj = s3.get_object(Bucket=bucket, Key=key)
    raw_csv = obj["Body"].read()
    df = pd.read_csv(io.BytesIO(raw_csv))
    df = clean_df(df)

    symbol = df["symbol"].iloc[0]
    dt = df["Datetime"].iloc[0]
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    hhmm = dt.strftime("%H%M")

    parquet_key = f"{STAGING_PREFIX}/year={year}/month={month}/day={day}/{symbol}-{hhmm}.parquet"
    write_parquet(df, parquet_key)

    logger.info(f"Uploaded parquet to s3://{BUCKET}/{parquet_key}")

def lambda_handler(event, context):
    logger.info("SQS Event received")
    for record in event["Records"]:
        message = json.loads(record["body"])
        process_sqs(message)

    return {"status":"ok"}
