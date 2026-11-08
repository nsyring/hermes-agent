"""Startup resilience of the Nextcloud Files sync service.

Regression for the 2026-11-07 incident: the gateway started during a DNS
outage, notify_push endpoint discovery failed once and incoming sync stayed
disabled until the next restart; the initial sync failed the same way and was
never retried.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from plugins.services.nextcloud_files.service import (
    NextcloudFilesService,
    NotifyPushListener,
)


def _caps_response(ws_url):
    resp = MagicMock()
    resp.raise_for_status = lambda: None
    notify_push = {"endpoints": {"websocket": ws_url}} if ws_url else {}
    resp.json = lambda: {"ocs": {"data": {"capabilities": {"notify_push": notify_push}}}}
    return resp


@pytest.fixture
def fast_sleep(monkeypatch):
    real_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda _s: real_sleep(0))


def _listener(monkeypatch, get):
    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kw: MagicMock(get=get, aclose=AsyncMock()))
    listener = NotifyPushListener("https://nc.example.com", "hermes", "pw")
    listener._listen_loop = AsyncMock()
    return listener


@pytest.mark.asyncio
async def test_failed_discovery_is_retried_in_background(monkeypatch, fast_sleep):
    get = AsyncMock(side_effect=[OSError("Temporary failure in name resolution"),
                                 _caps_response("wss://nc.example.com/push/ws")])
    listener = _listener(monkeypatch, get)

    assert await listener.start() is True
    await listener._listener_task

    assert listener._ws_url == "wss://nc.example.com/push/ws"
    listener._listen_loop.assert_awaited_once()
    assert get.await_count == 2


@pytest.mark.asyncio
async def test_missing_notify_push_capability_disables_incoming_sync(monkeypatch):
    get = AsyncMock(return_value=_caps_response(None))
    listener = _listener(monkeypatch, get)

    assert await listener.start() is False
    listener._listen_loop.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_sync_retries_until_success(fast_sleep):
    service = NextcloudFilesService.__new__(NextcloudFilesService)
    service._initial_sync = AsyncMock(side_effect=[False, False, True])

    await service._initial_sync_with_retry()

    assert service._initial_sync.await_count == 3
