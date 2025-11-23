import sys
import boto3
import json
import pandas as pd
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import snowflake.connector

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SQS_MESSAGE'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

sqs_message = json.loads(args["SQS_MESSAGE"])

record = sqs_message["Records"][0]
bucket = record["s3"]["bucket"]["name"]
key = record["s3"]["object"]["key"]

s3 = boto3.client("s3")
obj = s3.get_object(Bucket=bucket, Key=key)
df = pd.read_csv(obj["Body"])

df.columns = [c.strip() for c in df.columns]
if "Datetime" in df.columns:
    df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")

sf_user = "abdulrehman810"
sf_pass = "Kp!7qA9zMh@4"
sf_account = "ab12345-pk78901"
sf_database = "ABDUL_DATAWAREHOUSE"
sf_schema = "PUBLIC"
sf_warehouse = "ABDUL_WH_XS"

conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_pass,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)

cursor = conn.cursor()

insert_query = """
INSERT INTO STOCK_PRICES (symbol, Datetime, Open, High, Low, Close, Volume)
VALUES (%(symbol)s, %(Datetime)s, %(Open)s, %(High)s, %(Low)s, %(Close)s, %(Volume)s)
"""

for idx, row in df.iterrows():
    cursor.execute(insert_query, row.to_dict())

conn.commit()
cursor.close()
conn.close()

print("Snowflake load complete")
