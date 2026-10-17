import csv
import io
import logging
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

import psycopg

from .db import (
    claim_pending_batch,
    connection,
    mark_batch_completed,
    mark_batch_failed,
)
from .notifier import trigger_matching_workflow
from .storage import open_object

LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "user_id",
    "email",
    "monthly_income",
    "credit_score",
    "employment_status",
    "age",
}


class CsvValidationError(ValueError):
    pass


def _required_text(row: dict[str, str | None], field: str, line: int) -> str:
    value = (row.get(field) or "").strip()
    if not value:
        raise CsvValidationError(f"Line {line}: {field} is required.")
    return value


def _decimal_value(row: dict[str, str | None], field: str, line: int) -> Decimal:
    raw = _required_text(row, field, line)
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise CsvValidationError(
            f"Line {line}: {field} must be a number."
        ) from exc
    if not value.is_finite() or value < 0:
        raise CsvValidationError(
            f"Line {line}: {field} must be a non-negative finite number."
        )
    return value


def _integer_value(
    row: dict[str, str | None],
    field: str,
    line: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = _required_text(row, field, line)
    try:
        value = int(raw)
    except ValueError as exc:
        raise CsvValidationError(
            f"Line {line}: {field} must be a whole number."
        ) from exc
    if not minimum <= value <= maximum:
        raise CsvValidationError(
            f"Line {line}: {field} must be between {minimum} and {maximum}."
        )
    return value


def _validated_row(
    row: dict[str, str | None],
    line: int,
) -> tuple[str, str, Decimal, int, str, int]:
    email = _required_text(row, "email", line)
    if "@" not in email:
        raise CsvValidationError(f"Line {line}: email is invalid.")

    employment_status = " ".join(
        _required_text(row, "employment_status", line).lower().split()
    )

    return (
        _required_text(row, "user_id", line),
        email,
        _decimal_value(row, "monthly_income", line),
        _integer_value(row, "credit_score", line, 0, 1000),
        employment_status,
        _integer_value(row, "age", line, 0, 130),
    )


def _copy_users(batch_id: UUID, object_key: str) -> int:
    body = open_object(object_key)
    row_count = 0

    with io.TextIOWrapper(body, encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None:
            raise CsvValidationError("The CSV is empty or has no header row.")

        normalized_headers = [header.strip() for header in reader.fieldnames]
        if len(normalized_headers) != len(set(normalized_headers)):
            raise CsvValidationError("The CSV contains duplicate column names.")
        missing = sorted(REQUIRED_COLUMNS - set(normalized_headers))
        if missing:
            raise CsvValidationError(
                "CSV is missing required columns: " + ", ".join(missing) + "."
            )
        reader.fieldnames = normalized_headers

        with connection() as conn:
            conn.execute("DELETE FROM users WHERE batch_id = %s", (batch_id,))
            with conn.cursor().copy(
                """
                COPY users (
                    batch_id,
                    user_id,
                    email,
                    monthly_income,
                    credit_score,
                    employment_status,
                    age
                ) FROM STDIN
                """
            ) as copy:
                for row in reader:
                    line = reader.line_num
                    user = _validated_row(row, line)
                    copy.write_row((batch_id, *user))
                    row_count += 1

            if row_count == 0:
                raise CsvValidationError("The CSV contains no user rows.")

    return row_count


def process_uploaded_object(object_key: str) -> None:
    batch = claim_pending_batch(object_key)
    if batch is None:
        LOGGER.info("Ignoring event for unknown or already claimed object %s", object_key)
        return

    batch_id = batch["id"]
    try:
        row_count = _copy_users(batch_id, object_key)
    except CsvValidationError as exc:
        mark_batch_failed(batch_id, str(exc))
        return
    except UnicodeDecodeError:
        mark_batch_failed(batch_id, "The CSV must use UTF-8 encoding.")
        return
    except psycopg.errors.UniqueViolation:
        mark_batch_failed(batch_id, "The CSV contains duplicate user_id values.")
        return
    except Exception as exc:
        LOGGER.exception("Unexpected ingestion failure for batch %s", batch_id)
        mark_batch_failed(batch_id, f"CSV import failed: {type(exc).__name__}")
        return

    mark_batch_completed(batch_id, row_count)
    trigger_matching_workflow(batch_id)


def object_keys_from_event(payload: dict[str, Any], expected_bucket: str) -> list[str]:
    from urllib.parse import unquote_plus

    keys: list[str] = []
    for record in payload.get("Records", []):
        event_name = str(record.get("eventName", ""))
        s3 = record.get("s3", {})
        bucket_name = s3.get("bucket", {}).get("name")
        encoded_key = s3.get("object", {}).get("key")
        if (
            event_name.removeprefix("s3:").startswith("ObjectCreated:")
            and bucket_name == expected_bucket
            and encoded_key
        ):
            keys.append(unquote_plus(str(encoded_key)))
    return keys

