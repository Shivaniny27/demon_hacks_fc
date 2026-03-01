"""
Structured request/response logging middleware.

Adds to every request:
  - X-Request-ID  (uuid4, echoed in response header)
  - X-Process-Time (ms)

Emits a single structured log line per request:
  INFO | method=GET path=/api/v1/nav/congestion/all status=200 ms=14.2 req_id=…
"""
from __future__ import annotations

import logging
import time
import uuid
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("loopsense.access")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Attach request-ID + emit one access log line per HTTP request."""

    SKIP_PATHS = {"/health", "/", "/favicon.ico", "/docs", "/redoc", "/openapi.json"}

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        req_id   = str(uuid.uuid4())[:8]
        t0       = time.perf_counter()

        request.state.request_id = req_id

        try:
            response = await call_next(request)
        except Exception as exc:
            elapsed = (time.perf_counter() - t0) * 1_000
            logger.error(
                "method=%s path=%s status=500 ms=%.1f req_id=%s error=%s",
                request.method, request.url.path, elapsed, req_id, exc,
            )
            raise

        elapsed = (time.perf_counter() - t0) * 1_000
        response.headers["X-Request-ID"]   = req_id
        response.headers["X-Process-Time"] = f"{elapsed:.1f}ms"

        if request.url.path not in self.SKIP_PATHS:
            level = logging.WARNING if response.status_code >= 400 else logging.INFO
            logger.log(
                level,
                "method=%s path=%s status=%d ms=%.1f req_id=%s",
                request.method,
                request.url.path,
                response.status_code,
                elapsed,
                req_id,
            )

        return response
