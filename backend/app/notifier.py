from uuid import UUID

import httpx

from .config import get_settings
from .db import mark_workflow_trigger_failed, mark_workflow_triggered


def trigger_matching_workflow(batch_id: UUID) -> None:
    settings = get_settings()
    if not settings.n8n_match_webhook_url:
        return

    try:
        response = httpx.post(
            settings.n8n_match_webhook_url,
            json={"batch_id": str(batch_id)},
            headers={"X-Webhook-Secret": settings.n8n_webhook_secret},
            timeout=15,
        )
        response.raise_for_status()
    except (httpx.HTTPError, httpx.TimeoutException) as exc:
        mark_workflow_trigger_failed(batch_id, f"n8n webhook failed: {exc}")
        return

    mark_workflow_triggered(batch_id)

