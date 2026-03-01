"""
Global error handlers — return consistent JSON error shapes.

All API errors are:
  { "error": "<code>", "message": "<human msg>", "req_id": "<8-char id>" }
"""
from __future__ import annotations

import logging

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger("loopsense.errors")


def _req_id(request: Request) -> str:
    return getattr(request.state, "request_id", "--------")


async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    logger.warning(
        "HTTP %d — %s  [%s %s]  req_id=%s",
        exc.status_code, exc.detail,
        request.method, request.url.path,
        _req_id(request),
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error":   _status_code_to_slug(exc.status_code),
            "message": str(exc.detail),
            "req_id":  _req_id(request),
        },
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    errors = exc.errors()
    logger.warning(
        "Validation error — %d field(s) invalid  [%s %s]  req_id=%s",
        len(errors), request.method, request.url.path, _req_id(request),
    )
    return JSONResponse(
        status_code=422,
        content={
            "error":   "validation_error",
            "message": f"{len(errors)} field(s) failed validation",
            "details": [{"loc": e["loc"], "msg": e["msg"]} for e in errors],
            "req_id":  _req_id(request),
        },
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(
        "Unhandled exception  [%s %s]  req_id=%s",
        request.method, request.url.path, _req_id(request),
    )
    return JSONResponse(
        status_code=500,
        content={
            "error":   "internal_server_error",
            "message": "An unexpected error occurred. Please try again.",
            "req_id":  _req_id(request),
        },
    )


def _status_code_to_slug(code: int) -> str:
    return {
        400: "bad_request",
        401: "unauthorized",
        403: "forbidden",
        404: "not_found",
        409: "conflict",
        422: "validation_error",
        429: "rate_limit_exceeded",
        500: "internal_server_error",
        502: "bad_gateway",
        503: "service_unavailable",
    }.get(code, f"http_{code}")
