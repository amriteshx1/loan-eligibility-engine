import re
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import get_settings
from .db import create_batch, database_is_ready, get_batch
from .storage import create_presigned_upload

app = FastAPI(title="Loan Eligibility Engine", version="0.1.0")

UI_DIRECTORY = Path(__file__).resolve().parents[2] / "ui"
app.mount("/assets", StaticFiles(directory=UI_DIRECTORY), name="assets")

ALLOWED_CSV_CONTENT_TYPES = {
    "text/csv",
    "application/csv",
    "application/vnd.ms-excel",
}


class UploadRequest(BaseModel):
    filename: str = Field(min_length=1, max_length=255)
    content_type: str = Field(default="text/csv", max_length=100)


class UploadResponse(BaseModel):
    batch_id: UUID
    object_key: str
    upload_url: str
    expires_in_seconds: int


def _safe_csv_filename(filename: str) -> str:
    basename = filename.replace("\\", "/").rsplit("/", 1)[-1].strip()
    if not basename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Only .csv files are accepted.",
        )
    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", basename)
    if not safe_name or safe_name in {".", ".."}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="The filename is invalid.",
        )
    return safe_name


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(UI_DIRECTORY / "index.html")


@app.get("/health")
def health() -> dict[str, str]:
    if not database_is_ready():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database is unavailable.",
        )
    return {"status": "ok"}


@app.post(
    "/api/uploads",
    response_model=UploadResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_upload(request: UploadRequest) -> UploadResponse:
    safe_filename = _safe_csv_filename(request.filename)
    content_type = request.content_type.lower().strip()
    if content_type not in ALLOWED_CSV_CONTENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="The file content type must be CSV.",
        )

    batch_id = uuid4()
    object_key = f"uploads/{batch_id}/{safe_filename}"
    create_batch(batch_id, object_key, safe_filename)
    upload_url = create_presigned_upload(object_key, content_type)
    settings = get_settings()
    return UploadResponse(
        batch_id=batch_id,
        object_key=object_key,
        upload_url=upload_url,
        expires_in_seconds=settings.presigned_url_expiry_seconds,
    )


@app.get("/api/batches/{batch_id}")
def batch_status(batch_id: UUID) -> dict[str, Any]:
    batch = get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="Batch not found.")
    return batch

