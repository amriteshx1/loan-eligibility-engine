import json
import os

import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "loan-user-uploads")
QUEUE_NAME = os.environ.get("SQS_QUEUE_NAME", "loan-upload-events")
CLIENT_OPTIONS = {
    "endpoint_url": "http://localhost:4566",
    "region_name": REGION,
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
}

s3 = boto3.client("s3", **CLIENT_OPTIONS)
sqs = boto3.client("sqs", **CLIENT_OPTIONS)

try:
    s3.head_bucket(Bucket=BUCKET)
except ClientError:
    s3.create_bucket(Bucket=BUCKET)

queue_url = sqs.create_queue(QueueName=QUEUE_NAME)["QueueUrl"]
queue_arn = sqs.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=["QueueArn"],
)["Attributes"]["QueueArn"]

s3.put_bucket_cors(
    Bucket=BUCKET,
    CORSConfiguration={
        "CORSRules": [
            {
                "AllowedOrigins": [
                    "http://localhost:8000",
                    "http://127.0.0.1:8000",
                ],
                "AllowedMethods": ["PUT"],
                "AllowedHeaders": ["*"],
                "ExposeHeaders": ["ETag"],
                "MaxAgeSeconds": 3600,
            }
        ]
    },
)

s3.put_bucket_notification_configuration(
    Bucket=BUCKET,
    NotificationConfiguration={
        "QueueConfigurations": [
            {
                "Id": "user-csv-created",
                "QueueArn": queue_arn,
                "Events": ["s3:ObjectCreated:Put"],
            }
        ]
    },
)

print(
    json.dumps(
        {
            "bucket": BUCKET,
            "queue_name": QUEUE_NAME,
            "queue_arn": queue_arn,
        }
    )
)

