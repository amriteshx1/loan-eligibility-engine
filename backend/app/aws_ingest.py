"""SQS adapter for the existing CSV ingestion functions.

S3 object-created messages are parsed with the same helper the local worker
uses. A message is retried only when parsing or the database claim fails.
A rejected CSV is recorded on the batch and is not retried.
"""

import json
from typing import Any

from backend.app.config import get_settings
from backend.app.ingestion import object_keys_from_event, process_uploaded_object


def _payload(body: Any) -> dict[str, Any]:
    payload = json.loads(body) if isinstance(body, str) else body
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        raise ValueError("S3 event body must be a JSON object.")
    return payload


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    bucket = get_settings().s3_bucket
    failures: list[dict[str, str]] = []
    for record in event.get("Records", []):
        message_id = str(record.get("messageId", ""))
        try:
            payload = _payload(record.get("body") or "{}")
            for object_key in object_keys_from_event(payload, bucket):
                process_uploaded_object(object_key)
        except Exception:
            if message_id:
                failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}
