from typing import Any

import boto3
from botocore.client import Config

from .config import get_settings


def _client(endpoint_url: str) -> Any:
    settings = get_settings()
    endpoint = endpoint_url.strip()
    kwargs: dict[str, Any] = {
        "region_name": settings.s3_region,
        "config": Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if endpoint else "virtual"},
        ),
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if settings.s3_access_key_id and settings.s3_secret_access_key:
        kwargs["aws_access_key_id"] = settings.s3_access_key_id
        kwargs["aws_secret_access_key"] = settings.s3_secret_access_key
    return boto3.client("s3", **kwargs)


def create_presigned_upload(object_key: str, content_type: str) -> str:
    settings = get_settings()
    return _client(settings.s3_public_endpoint_url).generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": settings.s3_bucket,
            "Key": object_key,
            "ContentType": content_type,
        },
        ExpiresIn=settings.presigned_url_expiry_seconds,
        HttpMethod="PUT",
    )


def open_object(object_key: str) -> Any:
    settings = get_settings()
    response = _client(settings.s3_internal_endpoint_url).get_object(
        Bucket=settings.s3_bucket,
        Key=object_key,
    )
    return response["Body"]

