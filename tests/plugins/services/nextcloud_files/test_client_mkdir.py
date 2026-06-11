"""WebDAV mkdir semantics for the Nextcloud Files sync client.

Regression for the 2026-07-22 incident: MKCOL was issued only for the leaf
directory, so nested trees created in one burst returned 409 (missing
parent) and every file below them silently never reached the server. 405
(collection exists) was also logged as an ERROR although it is the expected
answer on idempotent sync retries.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from plugins.services.nextcloud_files.client import NextcloudFilesClient


def _client_with(responses):
    """Client whose MKCOL calls pop canned status codes in order."""
    client = NextcloudFilesClient.__new__(NextcloudFilesClient)
    client._base_url = "https://nc.example.com"
    client._username = "hermes"
    client._dav_base = "/remote.php/dav/files/hermes"

    calls = []

    async def request(method, url):
        assert method == "MKCOL"
        calls.append(url)
        resp = MagicMock()
        resp.status_code = responses[len(calls) - 1]
        if resp.status_code >= 400 and resp.status_code != 405:
            def boom():
                raise RuntimeError(f"HTTP {resp.status_code}")
            resp.raise_for_status = boom
        else:
            resp.raise_for_status = lambda: None
        return resp

    http = MagicMock()
    http.request = AsyncMock(side_effect=request)
    client._http = http
    return client, calls


@pytest.mark.asyncio
async def test_mkdir_creates_all_ancestors_in_order():
    client, calls = _client_with([201, 201, 201])
    assert await client.mkdir("/a/b/c") is True
    assert [u.split("/files/hermes")[1] for u in calls] == ["/a", "/a/b", "/a/b/c"]


@pytest.mark.asyncio
async def test_mkdir_treats_405_as_already_exists():
    # Retried sync: every ancestor already exists — must be success, not error.
    client, calls = _client_with([405, 405, 201])
    assert await client.mkdir("a/b/c") is True
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_mkdir_fails_on_real_error():
    client, calls = _client_with([201, 500, 201])
    assert await client.mkdir("/a/b/c") is False
    assert len(calls) == 2, "must stop at the failing ancestor"
