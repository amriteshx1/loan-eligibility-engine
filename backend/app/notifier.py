from uuid import UUID


def trigger_matching_workflow(batch_id: UUID) -> None:
    # Imported lazily so the ingestion module and the matcher do not cycle.
    from .matching_service import start_matching

    start_matching(batch_id)

