from __future__ import annotations

from unittest.mock import MagicMock, patch

from gaokao_vault.config import S3Config
from gaokao_vault.storage.s3 import S3Storage


def _storage(client: MagicMock) -> S3Storage:
    config = S3Config(
        endpoint_url="http://minio:9000",
        public_url="https://example.invalid/minio-s3",
        bucket_name="screenshots",
        presign_expires=120,
    )
    with patch("gaokao_vault.storage.s3.boto3.client", return_value=client):
        return S3Storage(config)


def test_s3_storage_upload_presign_and_delete(tmp_path) -> None:
    client = MagicMock()
    client.generate_presigned_url.return_value = "http://minio:9000/screenshots/path/image.png?signature=test"
    storage = _storage(client)
    image = tmp_path / "image.png"
    image.write_bytes(b"png")

    assert storage.upload_image(image, "path/image.png") == "path/image.png"
    assert storage.presigned_url("path/image.png").startswith(
        "https://example.invalid/minio-s3/screenshots/path/image.png"
    )
    storage.delete_image("path/image.png")

    client.upload_file.assert_called_once_with(
        str(image),
        "screenshots",
        "path/image.png",
        ExtraArgs={"ContentType": "image/png"},
    )
    client.delete_object.assert_called_once_with(Bucket="screenshots", Key="path/image.png")


def test_s3_storage_creates_missing_bucket() -> None:
    client = MagicMock()
    client.head_bucket.side_effect = RuntimeError("missing")
    storage = _storage(client)

    storage.ensure_bucket()

    client.create_bucket.assert_called_once_with(Bucket="screenshots")
