from typing import Any

import boto3
from botocore.client import Config

from .config import get_settings


def _client(endpoint_url: str) -> Any:
    settings = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


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

