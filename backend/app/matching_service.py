"""Set-based numeric filtering plus deterministic matching for one batch.

PostgreSQL applies income, credit-score, and age limits in one join. This
service classifies the survivors and writes resolved matches. Unresolved
qualitative checks are posted to Workflow B. The optional user-id bounds let
a later worker take a slice of the same join without moving the predicates
back into Python.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import httpx
import psycopg
from psycopg.types.json import Json

from .config import get_settings
from .db import (
    connection,
    mark_workflow_trigger_failed,
    mark_workflow_triggered,
)
from .matching import (
    CheckCounter,
    evaluate_candidates,
    llm_call_count,
)

LOGGER = logging.getLogger(__name__)

# Same user page size Workflow B used after it moved from 150 to 2000.
# It bounds each fetch from the set-based join and the recorded page count.
USER_PAGE_SIZE = 2000
WRITE_BATCH = 1000

SCOPE_SQL = """
SELECT
  b.status,
  (SELECT COUNT(*)::int FROM users u WHERE u.batch_id = b.id) AS users_in_batch,
  (SELECT COUNT(*)::int FROM loan_products p WHERE p.active) AS active_products,
  (
    SELECT COUNT(*)::int
    FROM loan_products p
    WHERE p.active
      AND p.income_currency <> 'USD'
      AND p.minimum_monthly_income IS NOT NULL
  ) AS products_skipped_currency
FROM ingestion_batches b
WHERE b.id = %s
"""

CANDIDATE_SQL = """
SELECT
  u.id AS user_record_id,
  u.user_id,
  u.name,
  u.email,
  u.monthly_income,
  u.credit_score,
  u.employment_status,
  u.age,
  p.id AS product_id,
  p.product_name,
  p.source_name,
  p.minimum_monthly_income,
  p.income_currency,
  p.credit_score_min,
  p.credit_score_max,
  p.age_min,
  p.age_max,
  p.accepted_employment_statuses,
  p.qualitative_criteria
FROM users u
JOIN loan_products p
  ON p.active = TRUE
 AND (
      p.income_currency = 'USD'
      OR p.minimum_monthly_income IS NULL
 )
 AND (p.minimum_monthly_income IS NULL OR u.monthly_income >= p.minimum_monthly_income)
 AND (p.credit_score_min IS NULL OR u.credit_score >= p.credit_score_min)
 AND (p.credit_score_max IS NULL OR u.credit_score <= p.credit_score_max)
 AND (p.age_min IS NULL OR u.age >= p.age_min)
 AND (p.age_max IS NULL OR u.age <= p.age_max)
WHERE u.batch_id = %s
  AND (%s::bigint IS NULL OR u.id >= %s)
  AND (%s::bigint IS NULL OR u.id <= %s)
ORDER BY u.id, p.id
"""

UPSERT_SQL = """
WITH incoming AS (
  SELECT *
  FROM jsonb_to_recordset(%s::jsonb) AS match(
    user_record_id bigint,
    product_id bigint,
    match_method text,
    match_reason text
  )
),
upserted AS (
  INSERT INTO matches (
    batch_id,
    user_record_id,
    product_id,
    match_method,
    match_reason,
    evaluated_at
  )
  SELECT
    %s::uuid,
    incoming.user_record_id,
    incoming.product_id,
    incoming.match_method,
    incoming.match_reason,
    %s::timestamptz
  FROM incoming
  ON CONFLICT (user_record_id, product_id)
  DO UPDATE SET
    batch_id = EXCLUDED.batch_id,
    match_method = EXCLUDED.match_method,
    match_reason = EXCLUDED.match_reason,
    evaluated_at = EXCLUDED.evaluated_at
  WHERE matches.notified_at IS NULL
  RETURNING 1
)
SELECT COUNT(*)::int AS matches_written FROM upserted
"""

STALE_SQL = """
WITH removed AS (
  DELETE FROM matches
  WHERE %s::text = 'completed'
    AND batch_id = %s::uuid
    AND notified_at IS NULL
    AND evaluated_at < %s::timestamptz
  RETURNING 1
)
SELECT COUNT(*)::int AS stale_matches_removed
FROM removed
"""

RUN_SQL = """
WITH inserted AS (
  INSERT INTO matching_runs (
    batch_id,
    status,
    users_in_batch,
    active_products,
    theoretical_pairs,
    sql_candidates,
    deterministic_remaining,
    qualitative_pairs,
    qualitative_llm_pairs,
    llm_calls,
    llm_errors,
    matches_written,
    stale_matches_removed,
    pages_processed,
    products_skipped_currency
  ) VALUES (
    %s::uuid, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s
  )
  RETURNING id
)
SELECT
  inserted.id AS matching_run_id,
  (SELECT COUNT(*)::int FROM matches WHERE batch_id = %s::uuid) AS matches_in_batch
FROM inserted
"""

LOCK_SQL = "SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS locked"
UNLOCK_SQL = "SELECT pg_advisory_unlock(hashtextextended(%s, 0))"


class MatchingBusy(Exception):
    """Another worker holds this batch. The caller should retry."""


class BatchNotReady(Exception):
    """Ingestion has not finished. The caller should retry."""


class BatchNotMatchable(Exception):
    """The batch is missing or failed. Retrying will not make it matchable."""


@dataclass
class MatchResult:
    batch_id: UUID
    run_started: str
    users_in_batch: int
    active_products: int
    theoretical_pairs: int
    products_skipped_currency: int
    sql_candidates: int = 0
    deterministic_remaining: int = 0
    qualitative_pairs: int = 0
    qualitative_llm_pairs: int = 0
    llm_calls: int = 0
    deterministic_matches_written: int = 0
    pages_processed: int = 1
    pending: list[dict[str, Any]] = field(default_factory=list)
    llm_checks: list[dict[str, Any]] = field(default_factory=list)

    def payload(self) -> dict[str, Any]:
        return {
            "deterministic_complete": True,
            "batch_id": str(self.batch_id),
            "run_started": self.run_started,
            "users_in_batch": self.users_in_batch,
            "active_products": self.active_products,
            "theoretical_pairs": self.theoretical_pairs,
            "products_skipped_currency": self.products_skipped_currency,
            "sql_candidates": self.sql_candidates,
            "deterministic_remaining": self.deterministic_remaining,
            "qualitative_pairs": self.qualitative_pairs,
            "qualitative_llm_pairs": self.qualitative_llm_pairs,
            "llm_calls": self.llm_calls,
            "deterministic_matches_written": self.deterministic_matches_written,
            "pages_processed": self.pages_processed,
            "user_page_size": USER_PAGE_SIZE,
            "pending": self.pending,
            "llm_checks": self.llm_checks,
        }


def pages_for_users(users_in_batch: int) -> int:
    if users_in_batch <= 0:
        return 1
    return (users_in_batch + USER_PAGE_SIZE - 1) // USER_PAGE_SIZE


def _handed_off(batch_id: UUID) -> bool:
    with connection() as conn:
        row = conn.execute(
            """
            SELECT workflow_triggered_at
            FROM ingestion_batches
            WHERE id = %s
            """,
            (batch_id,),
        ).fetchone()
    return bool(row and row["workflow_triggered_at"])


def _upsert(
    conn: psycopg.Connection[Any],
    batch_id: UUID,
    run_started: str,
    matches: list[dict[str, Any]],
) -> int:
    if not matches:
        return 0
    written = 0
    for offset in range(0, len(matches), WRITE_BATCH):
        chunk = matches[offset : offset + WRITE_BATCH]
        row = conn.execute(
            UPSERT_SQL,
            (Json(chunk), batch_id, run_started),
        ).fetchone()
        written += int(row["matches_written"])
    return written


def match_deterministic(
    batch_id: UUID,
    read_conn: psycopg.Connection[Any],
    write_conn: psycopg.Connection[Any],
    run_started: str,
    *,
    first_id: int | None = None,
    last_id: int | None = None,
) -> MatchResult:
    scope = read_conn.execute(SCOPE_SQL, (batch_id,)).fetchone()
    if scope is None:
        raise BatchNotMatchable("missing")
    status = scope["status"] or ""
    if status != "completed":
        if status in {"pending", "processing"}:
            raise BatchNotReady(status)
        raise BatchNotMatchable(status or "missing")

    users_in_batch = int(scope["users_in_batch"])
    active_products = int(scope["active_products"])
    result = MatchResult(
        batch_id=batch_id,
        run_started=run_started,
        users_in_batch=users_in_batch,
        active_products=active_products,
        theoretical_pairs=users_in_batch * active_products,
        products_skipped_currency=int(scope["products_skipped_currency"]),
        pages_processed=pages_for_users(users_in_batch),
    )
    counter = CheckCounter()
    pending: list[dict[str, Any]] = []
    llm_checks: list[dict[str, Any]] = []
    buffer: list[dict[str, Any]] = []

    # One server-side cursor. PostgreSQL evaluates the numeric join; the
    # application only classifies the rows the cursor returns.
    with read_conn.cursor(name="loan_match_candidates") as cur:
        cur.itersize = USER_PAGE_SIZE
        cur.execute(
            CANDIDATE_SQL,
            (batch_id, first_id, first_id, last_id, last_id),
        )
        while True:
            rows = cur.fetchmany(USER_PAGE_SIZE)
            if not rows:
                break
            classified = evaluate_candidates(list(rows), counter)
            result.sql_candidates += len(rows)
            result.deterministic_remaining += classified.deterministic_remaining
            result.qualitative_pairs += classified.qualitative_pairs
            pending.extend(classified.pending)
            llm_checks.extend(classified.llm_checks)
            buffer.extend(classified.matches)
            if len(buffer) >= WRITE_BATCH:
                result.deterministic_matches_written += _upsert(
                    write_conn, batch_id, run_started, buffer
                )
                buffer.clear()

    if buffer:
        result.deterministic_matches_written += _upsert(
            write_conn, batch_id, run_started, buffer
        )
    result.pending = pending
    result.llm_checks = llm_checks
    result.qualitative_llm_pairs = len(pending)
    result.llm_calls = llm_call_count(len(llm_checks))
    return result


def run_deterministic(batch_id: UUID) -> MatchResult:
    from datetime import datetime, timezone

    run_started = datetime.now(timezone.utc).isoformat()
    started = time.perf_counter()
    with connection() as read_conn, connection() as write_conn:
        read_conn.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
        )
        result = match_deterministic(batch_id, read_conn, write_conn, run_started)
    LOGGER.info(
        "Deterministic matching for %s finished in %.2fs: users=%s sql_candidates=%s "
        "deterministic_remaining=%s qualitative_llm_pairs=%s llm_calls=%s matches=%s",
        batch_id,
        time.perf_counter() - started,
        result.users_in_batch,
        result.sql_candidates,
        result.deterministic_remaining,
        result.qualitative_llm_pairs,
        result.llm_calls,
        result.deterministic_matches_written,
    )
    return result


def finalize_locally(result: MatchResult) -> None:
    """Record the run when n8n is not configured. Workflow C is not called."""
    LOGGER.warning(
        "N8N_MATCH_WEBHOOK_URL is empty. Recording matching_runs for %s without Workflow C.",
        result.batch_id,
    )
    with connection() as conn:
        removed = conn.execute(
            STALE_SQL,
            ("completed", result.batch_id, result.run_started),
        ).fetchone()
        conn.execute(
            RUN_SQL,
            (
                result.batch_id,
                "completed",
                result.users_in_batch,
                result.active_products,
                result.theoretical_pairs,
                result.sql_candidates,
                result.deterministic_remaining,
                result.qualitative_pairs,
                result.qualitative_llm_pairs,
                result.llm_calls,
                0,
                result.deterministic_matches_written,
                int(removed["stale_matches_removed"]),
                result.pages_processed,
                result.products_skipped_currency,
                result.batch_id,
            ),
        )


def deliver_handoff(result: MatchResult) -> None:
    settings = get_settings()
    url = settings.n8n_match_webhook_url.strip()
    if not url:
        finalize_locally(result)
        return
    response = httpx.post(
        url,
        json=result.payload(),
        headers={"X-Webhook-Secret": settings.n8n_webhook_secret},
        timeout=httpx.Timeout(840.0, connect=10.0),
    )
    response.raise_for_status()


def match_batch(batch_id: UUID) -> MatchResult | None:
    if _handed_off(batch_id):
        LOGGER.info("Matching already handed off for batch %s", batch_id)
        return None

    with connection() as lock_conn:
        locked = lock_conn.execute(LOCK_SQL, (str(batch_id),)).fetchone()["locked"]
        lock_conn.commit()
        if not locked:
            raise MatchingBusy(f"Matching is already running for batch {batch_id}")
        try:
            if _handed_off(batch_id):
                return None
            try:
                result = run_deterministic(batch_id)
            except (BatchNotReady, BatchNotMatchable):
                raise
            except Exception as exc:
                mark_workflow_trigger_failed(
                    batch_id,
                    f"Deterministic matching failed: {type(exc).__name__}",
                )
                raise
            try:
                deliver_handoff(result)
            except Exception as exc:
                mark_workflow_trigger_failed(batch_id, f"n8n handoff failed: {exc}")
                raise
            mark_workflow_triggered(batch_id)
            return result
        finally:
            lock_conn.execute(UNLOCK_SQL, (str(batch_id),))
            lock_conn.commit()


def _sqs_client() -> Any:
    import boto3

    settings = get_settings()
    endpoint = settings.s3_internal_endpoint_url.strip()
    kwargs: dict[str, Any] = {"region_name": settings.s3_region}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if settings.s3_access_key_id and settings.s3_secret_access_key:
        kwargs["aws_access_key_id"] = settings.s3_access_key_id
        kwargs["aws_secret_access_key"] = settings.s3_secret_access_key
    return boto3.client("sqs", **kwargs)


def _enqueue_matching(queue_url: str, batch_id: UUID) -> None:
    _sqs_client().send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({"batch_id": str(batch_id)}),
    )
    LOGGER.info("Queued deterministic matching for batch %s", batch_id)


def start_matching(batch_id: UUID) -> None:
    settings = get_settings()
    queue_url = settings.match_queue_url.strip()
    if queue_url:
        try:
            _enqueue_matching(queue_url, batch_id)
        except Exception as exc:
            mark_workflow_trigger_failed(batch_id, f"Could not start matching: {exc}")
            raise
        return
    match_batch(batch_id)
