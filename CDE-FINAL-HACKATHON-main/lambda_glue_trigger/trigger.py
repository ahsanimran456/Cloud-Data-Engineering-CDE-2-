import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client('glue')

def lambda_handler(event, context):
    for record in event["Records"]:
        message_body = json.loads(record["body"])
        
        response = glue.start_job_run(
            JobName="q3_snowflake_etl",
            Arguments={
                "--SQS_MESSAGE": json.dumps(message_body)
            }
        )
        
        logger.info(f"Started Glue Job Run: {response['JobRunId']}")
    
    return {"status": "ok"}
