"""Remote storage for Scherlok profiles — download, work locally, upload back.

Supports S3, GCS, and Azure Blob Storage. The profiles.db file is a SQLite
database that gets synced to/from cloud storage transparently.

Usage:
    scherlok config --store s3://bucket/path/profiles.db
    scherlok config --store gs://bucket/path/profiles.db
    scherlok config --store az://container/path/profiles.db
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def parse_remote_url(url: str) -> tuple[str, str, str]:
    """Parse remote URL into (provider, bucket, key).

    Supports:
        s3://bucket/path/profiles.db
        gs://bucket/path/profiles.db
        az://container/path/profiles.db
    """
    if url.startswith("s3://"):
        path = url[5:]
    elif url.startswith("gs://"):
        path = url[5:]
    elif url.startswith("az://"):
        path = url[5:]
    else:
        raise ValueError(f"Unsupported remote URL: {url}. Use s3://, gs://, or az://")

    provider = url[:2]
    parts = path.split("/", 1)
    if len(parts) < 2:
        raise ValueError(f"Invalid remote URL: {url}. Format: <provider>://bucket/path/profiles.db")

    return provider, parts[0], parts[1]


def _check_cli(cmd: str) -> bool:
    """Check if a CLI tool is available."""
    return shutil.which(cmd) is not None


def download(remote_url: str, local_path: Path) -> bool:
    """Download profiles.db from remote storage to local path.

    Returns True if file was downloaded, False if remote file doesn't exist yet.
    """
    provider, bucket, key = parse_remote_url(remote_url)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if provider == "s3":
            if not _check_cli("aws"):
                raise RuntimeError("AWS CLI not found. Install: pip install awscli")
            result = subprocess.run(
                ["aws", "s3", "cp", f"s3://{bucket}/{key}", str(local_path)],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode != 0:
                if "NoSuchKey" in result.stderr or "404" in result.stderr:
                    return False
                raise RuntimeError(f"S3 download failed: {result.stderr}")

        elif provider == "gs":
            if not _check_cli("gsutil"):
                raise RuntimeError("gsutil not found. Install: pip install google-cloud-storage")
            result = subprocess.run(
                ["gsutil", "cp", f"gs://{bucket}/{key}", str(local_path)],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode != 0:
                if "No URLs matched" in result.stderr or "not found" in result.stderr.lower():
                    return False
                raise RuntimeError(f"GCS download failed: {result.stderr}")

        elif provider == "az":
            if not _check_cli("az"):
                raise RuntimeError("Azure CLI not found. Install: https://aka.ms/installazurecli")
            result = subprocess.run(
                ["az", "storage", "blob", "download",
                 "--container-name", bucket,
                 "--name", key,
                 "--file", str(local_path),
                 "--no-progress"],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode != 0:
                if "BlobNotFound" in result.stderr or "not found" in result.stderr.lower():
                    return False
                raise RuntimeError(f"Azure download failed: {result.stderr}")

        return True

    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Download from {remote_url} timed out after 60s")


def upload(local_path: Path, remote_url: str) -> None:
    """Upload local profiles.db back to remote storage."""
    if not local_path.exists():
        return

    provider, bucket, key = parse_remote_url(remote_url)

    try:
        if provider == "s3":
            result = subprocess.run(
                ["aws", "s3", "cp", str(local_path), f"s3://{bucket}/{key}"],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode != 0:
                raise RuntimeError(f"S3 upload failed: {result.stderr}")

        elif provider == "gs":
            result = subprocess.run(
                ["gsutil", "cp", str(local_path), f"gs://{bucket}/{key}"],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode != 0:
                raise RuntimeError(f"GCS upload failed: {result.stderr}")

        elif provider == "az":
            result = subprocess.run(
                ["az", "storage", "blob", "upload",
                 "--container-name", bucket,
                 "--name", key,
                 "--file", str(local_path),
                 "--overwrite",
                 "--no-progress"],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode != 0:
                raise RuntimeError(f"Azure upload failed: {result.stderr}")

    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Upload to {remote_url} timed out after 60s")


def sync_context(remote_url: str | None, local_path: Path):
    """Context manager that downloads before and uploads after.

    Usage:
        with sync_context(remote_url, local_path):
            # work with local SQLite as usual
            ...
        # file automatically uploaded back
    """
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        if remote_url:
            download(remote_url, local_path)
        try:
            yield local_path
        finally:
            if remote_url:
                upload(local_path, remote_url)

    return _ctx()
