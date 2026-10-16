"""Apply db/schema.sql from inside the VPC when the stack is created.

RDS is private, so the laptop cannot open it directly. This runs the same
schema file used by the local database, then answers CloudFormation.
"""

import json
import time
import urllib.request
from pathlib import Path
from typing import Any

from backend.app.db import connection

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "db" / "schema.sql"


def _statements(script: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current).strip().rstrip(";").strip()
            if statement:
                statements.append(statement)
            current = []
    trailing = "\n".join(current).strip().rstrip(";").strip()
    if trailing:
        statements.append(trailing)
    return statements


def apply_schema() -> None:
    script = SCHEMA_PATH.read_text(encoding="utf-8")
    statements = _statements(script)
    if not statements:
        raise RuntimeError(f"No SQL statements found in {SCHEMA_PATH}.")
    last_error: Exception | None = None
    for _attempt in range(8):
        try:
            with connection() as conn:
                for statement in statements:
                    conn.execute(statement)
            return
        except Exception as exc:
            last_error = exc
            time.sleep(5)
    raise RuntimeError("Could not apply the loan schema.") from last_error


def _respond(event: dict[str, Any], status: str, reason: str) -> None:
    body = json.dumps(
        {
            "Status": status,
            "Reason": reason[:1000],
            "PhysicalResourceId": "loan-engine-schema",
            "StackId": event["StackId"],
            "RequestId": event["RequestId"],
            "LogicalResourceId": event["LogicalResourceId"],
            "Data": {},
        }
    ).encode()
    request = urllib.request.Request(
        event["ResponseURL"],
        data=body,
        method="PUT",
        headers={"Content-Type": ""},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        response.read()


def handler(event: dict[str, Any], _context: Any) -> None:
    status = "SUCCESS"
    reason = "Schema is current."
    try:
        if event.get("RequestType") != "Delete":
            apply_schema()
    except Exception as exc:
        status = "FAILED"
        reason = str(exc)
    _respond(event, status, reason)
    if status == "FAILED":
        raise RuntimeError(reason)
