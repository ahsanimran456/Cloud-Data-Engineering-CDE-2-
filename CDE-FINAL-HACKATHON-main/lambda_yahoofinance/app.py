import yfinance as yf
import boto3
import datetime
import json
import pandas as pd

s3 = boto3.client('s3')
bucket = "data-hackathon-smit-abdulrehman810"

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA"]

def lambda_handler(event, context):
    now = datetime.datetime.utcnow()
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%H%M")

    data = {}

    for symbol in TICKERS:
        try:
            df = yf.download(symbol, interval="1m", period="1d")
            if not df.empty:
                row = df.tail(1).reset_index().to_dict(orient="records")[0]

                clean_row = {}
                for k, v in row.items():
                    if isinstance(v, (pd.Timestamp, datetime.datetime)):
                        v = v.isoformat()
                    clean_row[str(k)] = v

                data[symbol] = clean_row

        except Exception as e:
            data[symbol] = {"error": str(e)}

    payload = {
        "source": "yahoofinance",
        "timestamp": now.isoformat(),
        "records": data
    }

    key = f"raw/yahoofinance/{date_path}/{timestamp}.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload),
        Metadata={"source": "yahoofinance"}
    )

    return {"status": "success", "file": key}
