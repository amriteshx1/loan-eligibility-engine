import json
import logging
import signal
import threading
import time
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from .config import get_settings
from .ingestion import object_keys_from_event, process_uploaded_object

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOGGER = logging.getLogger(__name__)
STOP_EVENT = threading.Event()


def _sqs_client() -> Any:
    settings = get_settings()
    endpoint = settings.s3_internal_endpoint_url.strip()
    kwargs: dict[str, Any] = {
        "region_name": settings.s3_region,
        "config": Config(signature_version="v4"),
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if settings.s3_access_key_id and settings.s3_secret_access_key:
        kwargs["aws_access_key_id"] = settings.s3_access_key_id
        kwargs["aws_secret_access_key"] = settings.s3_secret_access_key
    return boto3.client("sqs", **kwargs)


def _wait_for_queue(client: Any) -> str:
    settings = get_settings()
    while not STOP_EVENT.is_set():
        try:
            response = client.get_queue_url(QueueName=settings.sqs_queue_name)
            return str(response["QueueUrl"])
        except (BotoCoreError, ClientError):
            LOGGER.info("Waiting for the local ingestion queue")
            STOP_EVENT.wait(2)
    raise RuntimeError("Worker stopped before the queue became available.")


def _stop(_signal_number: int, _frame: Any) -> None:
    STOP_EVENT.set()


def run() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    settings = get_settings()
    client = _sqs_client()
    queue_url = _wait_for_queue(client)
    LOGGER.info(
        "Ingestion worker is listening on %s via %s",
        settings.sqs_queue_name,
        settings.s3_internal_endpoint_url.strip() or "AWS",
    )

    while not STOP_EVENT.is_set():
        try:
            response = client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
                VisibilityTimeout=300,
            )
        except (BotoCoreError, ClientError):
            LOGGER.exception("Could not receive ingestion events")
            time.sleep(2)
            continue

        for message in response.get("Messages", []):
            receipt_handle = message["ReceiptHandle"]
            try:
                payload = json.loads(message["Body"])
                for object_key in object_keys_from_event(payload, settings.s3_bucket):
                    process_uploaded_object(object_key)
            except Exception:
                LOGGER.exception("Ingestion event failed and will be retried")
                continue

            client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
            )


if __name__ == "__main__":
    run()

