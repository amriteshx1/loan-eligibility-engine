"""SQS adapter for deterministic matching.

A message is retried when the batch is not ready yet, another worker holds
it, or the database or n8n handoff fails. A missing or failed batch is
dropped. Upserts make a retried batch safe to run again.
"""

import json
import logging
from typing import Any
from uuid import UUID

from backend.app.matching_service import (
    BatchNotMatchable,
    match_batch,
)

LOGGER = logging.getLogger(__name__)


def _body(raw: Any) -> dict[str, Any]:
    payload = json.loads(raw) if isinstance(raw, str) else raw
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        raise ValueError("Matching message must be a JSON object.")
    return payload


def _process(record: dict[str, Any]) -> None:
    payload = _body(record.get("body") or "{}")
    try:
        batch_id = UUID(str(payload["batch_id"]))
    except (KeyError, ValueError, TypeError) as exc:
        raise ValueError("batch_id must be a UUID") from exc
    match_batch(batch_id)


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    failures: list[dict[str, str]] = []
    for record in event.get("Records", []):
        message_id = str(record.get("messageId", ""))
        try:
            _process(record)
        except (BatchNotMatchable, ValueError):
            LOGGER.exception("Dropping matching message %s", message_id or "<missing>")
        except Exception:
            LOGGER.exception("Matching message %s will be retried", message_id or "<missing>")
            if message_id:
                failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}
