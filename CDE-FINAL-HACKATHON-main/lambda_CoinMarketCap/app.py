import requests
from bs4 import BeautifulSoup
import boto3
import datetime
import json

s3 = boto3.client('s3')
bucket = "data-hackathon-smit-abdulrehman810"

CMC_URL = "https://coinmarketcap.com/"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9"
}

def lambda_handler(event, context):
    now = datetime.datetime.utcnow()
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%H%M")

    response = requests.get(CMC_URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    data = []

    rows = soup.select("table tbody tr")
    for row in rows[:50]:
        cols = row.select("td")

        if len(cols) < 5:
            continue

        item = {
            "rank": cols[0].text.strip(),
            "name": cols[2].text.strip(),
            "price": cols[3].text.strip(),
            "24h": cols[4].text.strip(),
            "7d": cols[5].text.strip() if len(cols) > 5 else None
        }

        data.append(item)

    payload = {
        "source": "coinmarketcap",
        "timestamp": now.isoformat(),
        "records": data
    }

    key = f"raw/coinmarketcap/{date_path}/{timestamp}.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload),
        Metadata={"source": "coinmarketcap"}
    )

    return {"status": "success", "file": key}
