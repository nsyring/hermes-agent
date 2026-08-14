"""Shared base class for the aimux bridge background service.

Lives inside the plugin so the service module is self-contained and does
not depend on a sibling ``gateway/services/`` directory in the core tree.
Mirrors the canonical ``BaseService`` shape used by the other bundled
service plugins (see ``plugins/services/nextcloud_notifications/base.py``).
"""
from abc import ABC, abstractmethod
from typing import Any, Optional


class BaseService(ABC):
    """Background service that observes an external source and routes events.

    The plugin-registered ``service_factory`` is called with
    ``(config_dict, gateway_runner)`` — subclasses MUST accept both. The
    factory contract is enforced by
    ``gateway.service_registry.service_registry.create_service``.
    """

    name: str = "base"

    def __init__(self, config: dict, gateway_runner: Optional[Any] = None):
        self.config = config
        self.gateway_runner = gateway_runner

    @abstractmethod
    async def start(self) -> bool:
        """Start background tasks. Returns True on success."""

    @abstractmethod
    async def stop(self) -> None:
        """Clean shutdown — cancel tasks, close connections."""
