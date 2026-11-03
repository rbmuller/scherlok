"""Tests for `sync_context`, the download/upload wrapper around a remote store."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from scherlok.store.remote import sync_context

REMOTE_STORE = "s3://example-bucket/scherlok/profiles.db"


@pytest.fixture()
def remote():
    with (
        patch("scherlok.store.remote.download") as download,
        patch("scherlok.store.remote.upload") as upload,
    ):
        yield download, upload


def test_default_downloads_then_uploads(remote, tmp_path):
    download, upload = remote
    local = tmp_path / "profiles.db"

    with sync_context(REMOTE_STORE, local) as path:
        assert path == local
        download.assert_called_once_with(REMOTE_STORE, local)
        upload.assert_not_called()

    upload.assert_called_once_with(local, REMOTE_STORE)


def test_upload_on_exit_false_downloads_but_never_uploads(remote, tmp_path):
    download, upload = remote
    local = tmp_path / "profiles.db"

    with sync_context(REMOTE_STORE, local, upload_on_exit=False):
        pass

    download.assert_called_once_with(REMOTE_STORE, local)
    upload.assert_not_called()


def test_upload_on_exit_false_never_uploads_when_the_body_raises(remote, tmp_path):
    _download, upload = remote

    with pytest.raises(RuntimeError):
        with sync_context(REMOTE_STORE, tmp_path / "profiles.db", upload_on_exit=False):
            raise RuntimeError("boom")

    upload.assert_not_called()


def test_no_remote_store_touches_nothing(remote, tmp_path):
    download, upload = remote

    with sync_context(None, tmp_path / "profiles.db"):
        pass

    download.assert_not_called()
    upload.assert_not_called()
