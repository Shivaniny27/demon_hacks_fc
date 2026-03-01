"""
WebSocket Connection Manager.

Manages all connected clients and broadcasts new report events in real time.
The frontend map listens on ws://.../ws and updates instantly when a new
hazard report is classified.
"""

import json
import logging
from typing import Any
from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self):
        self._active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._active.append(ws)
        logger.info("WS client connected. Total: %d", len(self._active))

    def disconnect(self, ws: WebSocket):
        if ws in self._active:
            self._active.remove(ws)
        logger.info("WS client disconnected. Total: %d", len(self._active))

    async def broadcast(self, event: str, payload: Any):
        """Send a JSON message to all connected clients."""
        message = json.dumps({"event": event, "payload": payload})
        stale = []
        for ws in self._active:
            try:
                await ws.send_text(message)
            except Exception:
                stale.append(ws)
        for ws in stale:
            self.disconnect(ws)

    async def send_to(self, ws: WebSocket, event: str, payload: Any):
        """Send a JSON message to a single client."""
        try:
            await ws.send_text(json.dumps({"event": event, "payload": payload}))
        except Exception:
            self.disconnect(ws)

    @property
    def active_count(self) -> int:
        return len(self._active)


# Singleton instance shared across all routes
manager = ConnectionManager()
