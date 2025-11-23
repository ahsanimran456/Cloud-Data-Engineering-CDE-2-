import requests
import pandas as pd
import boto3
import datetime
import json

s3 = boto3.client('s3')
bucket = "data-hackathon-smit-abdulrehman810"

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def lambda_handler(event, context):
    now = datetime.datetime.utcnow()
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%H%M")

    tables = pd.read_html(WIKI_URL)
    df = tables[0]  # first table is S&P 500

    df['Date first added'] = df['Date first added'].astype(str)

    payload = {
        "source": "sp500",
        "timestamp": now.isoformat(),
        "records": df.to_dict(orient="records")
    }

    key = f"raw/sp500/{date_path}/{timestamp}.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload),
        Metadata={"source": "sp500"}
    )

    return {"status": "success", "file": key}
