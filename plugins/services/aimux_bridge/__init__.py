"""aimux bridge background-service plugin entry point.

Registers a long-running service via
:meth:`PluginContext.register_background_service` so the gateway can start
it after platforms come up and stop it before they disconnect.

The service runs a loopback-only aiohttp listener that the aimux daemon
POSTs to when a new chat message is stored; matching notifications are
injected as ``MessageEvent`` doorbells into the configured platform
session (see ``service.py`` for the wire contract).
"""
from __future__ import annotations

import logging
from typing import Any

try:
    import aiohttp  # noqa: F401
    _AIOHTTP = True
except ImportError:
    _AIOHTTP = False

logger = logging.getLogger(__name__)


def _check_requirements() -> bool:
    return _AIOHTTP


def _validate_config(cfg: dict) -> bool:
    """Need at least one route so we know where to deliver. The secret is
    validated at start() (it comes from the environment, not the config
    shape), so we only sanity-check the route list here.
    """
    extra = cfg.get("extra") or cfg  # tolerate flat-vs-nested config
    routes = extra.get("routes") or []
    return bool(routes)


def _service_factory(config: dict, gateway_runner: Any) -> Any:
    """Build an ``AimuxBridgeService`` from the gateway config dict.

    The dict is the raw ``services.aimux_bridge`` block from config.yaml
    (including ``enabled``, ``extra``, etc.). We pass the ``extra`` block
    through as the service config.
    """
    from .service import AimuxBridgeService

    extra = dict(config.get("extra") or {})
    return AimuxBridgeService(extra, gateway_runner)


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system at startup."""
    ctx.register_background_service(
        name="aimux_bridge",
        label="aimux Bridge",
        service_factory=_service_factory,
        check_fn=_check_requirements,
        validate_config=_validate_config,
        install_hint="pip install aiohttp   # already a Hermes dependency",
    )


__all__ = ["register"]
