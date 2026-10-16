from functools import lru_cache
from urllib.parse import quote_plus

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LOCAL_DATABASE_URL = (
    "postgresql://loan_app:local-app-password@localhost:5432/loan_engine"
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # DB_HOST builds the RDS URL. Docker Compose sets DATABASE_URL and leaves
    # DB_HOST empty, so the local database string is used as-is.
    database_url: str = ""
    db_host: str = ""
    db_port: int = 5432
    db_name: str = "loan_engine"
    db_user: str = "loan_app"
    db_password: str = ""
    db_sslmode: str = ""

    s3_bucket: str = "loan-user-uploads"
    s3_region: str = "us-east-1"
    # Empty endpoints and keys mean the real AWS API and the IAM role.
    # Docker Compose sets the LocalStack endpoint and the local test keys.
    s3_internal_endpoint_url: str = ""
    s3_public_endpoint_url: str = ""
    s3_access_key_id: str = ""
    s3_secret_access_key: str = ""
    sqs_queue_name: str = "loan-upload-events"
    presigned_url_expiry_seconds: int = Field(default=900, ge=60, le=3600)

    n8n_match_webhook_url: str = ""
    n8n_webhook_secret: str = ""

    @model_validator(mode="after")
    def assemble_database_url(self) -> "Settings":
        host = self.db_host.strip()
        if host:
            user = quote_plus(self.db_user)
            password = quote_plus(self.db_password)
            auth = f"{user}:{password}" if self.db_password else user
            ssl = f"?sslmode={self.db_sslmode}" if self.db_sslmode else ""
            self.database_url = (
                f"postgresql://{auth}@{host}:{self.db_port}/{self.db_name}{ssl}"
            )
            return self
        if self.database_url.strip():
            return self
        self.database_url = LOCAL_DATABASE_URL
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()

