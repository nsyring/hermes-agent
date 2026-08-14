"""aimux bridge background service.

Runs a loopback-only aiohttp listener that the aimux daemon POSTs to when a
new chat message is stored. Each notification is mapped (by ``channel_id``)
to a target platform session and injected as a ``MessageEvent`` so the agent
wakes and reads the message itself via the aimux-chat MCP tools.

Wire contract (v1), agreed with claude-pm-crush; aimux daemon side pinned by
``TestRingPushWebhookContractV1`` (aimux >= v0.6.6-beta49):

    POST http://127.0.0.1:<listen_port>/aimux/notify
    Header:  X-Aimux-Secret: <shared secret>
    Body:    {"version": 1,
              "event": "message",
              "channel_id": "...", "channel_name": "...",
              "message_id": "...",   # always present; may be "" for local hub events
              "from": {"type": "agent|user|federation", "name": "..."},
              "preview": "..."}      # optional, ignored in v1
    Responses:
      204  accepted + injected (also: ignored sender, or debounced)
      400  malformed body / missing channel_id / missing message_id field
      401  missing or wrong secret (constant-time compare)
      404  no route configured for channel_id (rings into the void)

The listener binds strictly to 127.0.0.1 and refuses to start without a
configured secret — there is never an open unauthenticated listener.

Note on ``message_id``: the daemon fills it for federation messages but may
send an empty string for local hub events. The field is always present; we
require its *presence* but tolerate an empty value (used only to build the
injected event's message_id).
"""
from __future__ import annotations

import asyncio
import hmac
import logging
import os
from typing import Any, Dict, Optional

from aiohttp import web

from .base import BaseService

logger = logging.getLogger(__name__)

_DEFAULT_PORT = 9127
_DEFAULT_HOST = "127.0.0.1"
# Debounce identical channel rings so a burst doesn't spam the session. The
# daemon already debounces per (URL, channel) ~5s, this is belt-and-suspenders.
_DEFAULT_DEBOUNCE_S = 5.0


class AimuxBridgeService(BaseService):
    """Loopback doorbell: aimux daemon POST -> MessageEvent injection."""

    name = "aimux_bridge"

    def __init__(self, config: dict, gateway_runner: Optional[Any] = None):
        super().__init__(config, gateway_runner)
        extra = config

        self._host: str = str(extra.get("listen_host", _DEFAULT_HOST))
        self._port: int = int(extra.get("listen_port", _DEFAULT_PORT))
        self._debounce_s: float = float(extra.get("debounce_seconds", _DEFAULT_DEBOUNCE_S))

        # Shared secret from env (never inlined in config.yaml). Same value is
        # set in the aimux daemon's chat.push_webhooks[].secret.
        self._secret_env = str(extra.get("secret_env", "AIMUX_BRIDGE_SECRET"))
        self._secret: str = os.environ.get(self._secret_env, "").strip()

        # Senders whose messages never ring (self-send guard). The daemon
        # already skips self-sends token-based (never name-based), so this is
        # a redundant safety net that in practice never fires — Hermes' own
        # sends appear under the canonical invite name, not these.
        ignore = extra.get("ignore_senders") or []
        self._ignore_senders = {str(s).lower() for s in ignore}

        # channel_id -> {platform, chat_id, user_id, user_name?}
        self._routes: Dict[str, Dict[str, str]] = {}
        for route in extra.get("routes") or []:
            cid = route.get("channel_id")
            if cid and route.get("platform") and route.get("chat_id"):
                self._routes[str(cid)] = route

        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        # channel_id -> monotonic timestamp of last accepted ring (debounce)
        self._last_ring: Dict[str, float] = {}
        self._shutdown = False

    # ── Lifecycle ────────────────────────────────────────────────────────
    async def start(self) -> bool:
        if not self._secret:
            logger.warning(
                "[aimux bridge] no shared secret configured "
                "(env %s empty) — refusing to open an unauthenticated listener",
                self._secret_env,
            )
            return False
        if not self._routes:
            logger.warning("[aimux bridge] no routes configured — nothing to deliver to")
            return False
        if self.gateway_runner is None:
            logger.warning("[aimux bridge] no gateway_runner — cannot inject messages")
            return False

        app = web.Application()
        app.router.add_post("/aimux/notify", self._handle_notify)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host=self._host, port=self._port)
        try:
            await self._site.start()
        except OSError as exc:
            logger.error("[aimux bridge] could not bind %s:%d: %s", self._host, self._port, exc)
            await self._runner.cleanup()
            self._runner = None
            return False

        logger.info(
            "[aimux bridge] listening on http://%s:%d/aimux/notify (%d route(s))",
            self._host, self._port, len(self._routes),
        )
        return True

    async def stop(self) -> None:
        self._shutdown = True
        if self._site is not None:
            try:
                await self._site.stop()
            except Exception:
                logger.debug("[aimux bridge] site stop error", exc_info=True)
        if self._runner is not None:
            try:
                await self._runner.cleanup()
            except Exception:
                logger.debug("[aimux bridge] runner cleanup error", exc_info=True)
        self._site = None
        self._runner = None
        logger.info("[aimux bridge] stopped")

    # ── HTTP handler ─────────────────────────────────────────────────────
    async def _handle_notify(self, request: web.Request) -> web.Response:
        # Auth first — constant-time compare, wrong/missing -> 401.
        provided = request.headers.get("X-Aimux-Secret", "")
        if not hmac.compare_digest(provided, self._secret):
            return web.Response(status=401, text="unauthorized")

        try:
            payload = await request.json()
        except Exception:
            return web.Response(status=400, text="invalid json")
        if not isinstance(payload, dict):
            return web.Response(status=400, text="body must be an object")

        # channel_id is the only hard requirement. message_id must be PRESENT
        # (the daemon always includes the field) but may be an empty string
        # for local hub events — tolerate that.
        channel_id = payload.get("channel_id")
        if not channel_id:
            return web.Response(status=400, text="missing channel_id")
        if "message_id" not in payload:
            return web.Response(status=400, text="missing message_id field")
        message_id = payload.get("message_id") or ""

        sender = ((payload.get("from") or {}).get("name") or "").lower()
        if sender and sender in self._ignore_senders:
            # Ignored sender — accept but don't ring.
            return web.Response(status=204)

        route = self._routes.get(str(channel_id))
        if route is None:
            # No mapping — ring into the void. 404 is useful in the daemon's log.
            logger.info("[aimux bridge] no route for channel %s — dropping", channel_id)
            return web.Response(status=404, text="no route for channel")

        # Debounce per channel.
        now = asyncio.get_event_loop().time()
        last = self._last_ring.get(str(channel_id), 0.0)
        if now - last < self._debounce_s:
            return web.Response(status=204)
        self._last_ring[str(channel_id)] = now

        try:
            await self._inject(route, payload, message_id)
        except Exception:
            logger.exception("[aimux bridge] injection failed for channel %s", channel_id)
            # Fire-and-forget contract: don't make the daemon retry.
            return web.Response(status=204)

        return web.Response(status=204)

    # ── Injection ────────────────────────────────────────────────────────
    async def _inject(self, route: Dict[str, str], payload: Dict[str, Any], message_id: str) -> None:
        """Inject a doorbell MessageEvent into the target platform session."""
        from gateway.config import Platform
        from gateway.platforms.base import MessageEvent, MessageType

        runner = self.gateway_runner
        try:
            platform = Platform(route["platform"])
        except ValueError:
            logger.error("[aimux bridge] unknown platform '%s'", route.get("platform"))
            return

        adapter = runner.adapters.get(platform)
        if not adapter:
            logger.warning("[aimux bridge] adapter for %s not connected", platform.value)
            return

        channel_name = payload.get("channel_name") or payload.get("channel_id")
        from_name = (payload.get("from") or {}).get("name") or "unknown"
        text = (
            f"[aimux] Neue Nachricht in #{channel_name} von {from_name}. "
            f"Rufe read_messages fuer diesen Channel auf, um sie zu lesen und zu beantworten."
        )

        chat_id = route["chat_id"]
        user_id = route.get("user_id") or f"service:{self.name}"
        user_name = route.get("user_name") or f"aimux/{from_name}"

        chat_type = "dm"
        if hasattr(adapter, "_classify_chat"):
            chat_type = adapter._classify_chat(chat_id)

        source = adapter.build_source(
            chat_id=chat_id,
            user_id=user_id,
            user_name=user_name,
            chat_type=chat_type,
        )

        # message_id may be empty (local hub event); fall back to channel_id
        # so the injected event id stays unique-ish and non-empty.
        mid = message_id or f"chan{payload.get('channel_id')}"
        msg_event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=f"aimux_{mid}",
        )

        await adapter.handle_message(msg_event)
        logger.info(
            "[aimux bridge] rang %s/%s for channel %s (msg %s)",
            platform.value, chat_id, payload.get("channel_id"), message_id or "<empty>",
        )
