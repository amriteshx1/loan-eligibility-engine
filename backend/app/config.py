from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = (
        "postgresql://loan_app:local-app-password@localhost:5432/loan_engine"
    )

    s3_bucket: str = "loan-user-uploads"
    s3_region: str = "us-east-1"
    s3_internal_endpoint_url: str = "http://localhost:4566"
    s3_public_endpoint_url: str = "http://localhost:4566"
    s3_access_key_id: str = "test"
    s3_secret_access_key: str = "test"
    sqs_queue_name: str = "loan-upload-events"
    presigned_url_expiry_seconds: int = Field(default=900, ge=60, le=3600)

    n8n_match_webhook_url: str = ""
    n8n_webhook_secret: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()

