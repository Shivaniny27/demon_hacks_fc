"""
LoopSense + LoopNav — Chicago Loop Pedestrian Intelligence Platform
FastAPI Application Entry Point
"""

import logging
import os
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import settings
from app.database import init_db
from app.api.websocket import manager
from app.api.middleware.logging import RequestLoggingMiddleware
from app.api.middleware.errors import (
    http_exception_handler,
    validation_exception_handler,
    unhandled_exception_handler,
)

# ── Existing LoopSense routers ─────────────────────────────────────────────────
from app.api.v1 import reports, hazards, cta, weather, routing

# ── New LoopNav routers ────────────────────────────────────────────────────────
from app.api.v1 import nav_route, layers, ai_route, stats, ws_session, isochrone

# ── Phase 3 — CTA Intelligence routers ────────────────────────────────────────
from app.api.v1 import ada, tracker, station, congestion

logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Rate limiter ───────────────────────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    logger.info("LoopSense + LoopNav backend starting…")

    # ── PostgreSQL (existing LoopSense) ────────────────────────────────────────
    try:
        await init_db()
        logger.info("Database tables ready.")
    except Exception as db_exc:
        logger.warning("Database unavailable (%s) — report/hazard endpoints disabled.", db_exc)

    # ── Redis (LoopNav caching + stats) ───────────────────────────────────────
    redis_url = os.getenv("REDIS_URL", settings.REDIS_URL)
    try:
        app.state.redis = aioredis.from_url(redis_url, decode_responses=True)
        await app.state.redis.ping()
        logger.info("Redis connected at %s", redis_url)
    except Exception as e:
        logger.warning("Redis unavailable (%s) — caching disabled.", e)
        app.state.redis = None

    # ── Multi-layer pedestrian graph (LoopNav) ─────────────────────────────────
    if settings.USE_STUB_GRAPH:
        from app.graph.stub import get_graph_stub
        app.state.graph = get_graph_stub()
        logger.info(
            "Stub graph loaded — %d nodes, %d edges",
            app.state.graph.number_of_nodes(),
            app.state.graph.number_of_edges(),
        )
    else:
        # Dev1 provides app/routing/engine.py and app/graph/loader.py
        from app.graph.loader import get_graph  # type: ignore
        app.state.graph = await get_graph()
        logger.info(
            "Real graph loaded — %d nodes, %d edges",
            app.state.graph.number_of_nodes(),
            app.state.graph.number_of_edges(),
        )

    # ── Phase 3 — CTA Intelligence services (real-time polling) ───────────────
    redis = app.state.redis
    try:
        from app.services.elevator_service import get_elevator_service
        from app.services.outage_service import get_outage_service
        from app.services.pedway_service import get_pedway_service

        elev_svc  = get_elevator_service(redis=redis)
        await elev_svc.initialize()
        app.state.elevator_service = elev_svc
        logger.info("ElevatorService: polling started.")

        outage_svc = get_outage_service(redis=redis)
        await outage_svc.initialize()
        app.state.outage_service = outage_svc
        logger.info("OutageService: polling started.")

        pedway_svc = get_pedway_service(redis=redis)
        await pedway_svc.initialize()
        app.state.pedway_service = pedway_svc
        logger.info("PedwayService: initialized.")

        from app.services.congestion_service import get_congestion_service
        congestion_svc = get_congestion_service(redis=redis)
        await congestion_svc.initialize(redis=redis)
        app.state.congestion_service = congestion_svc
        logger.info(
            "CongestionService: initialized (real_data=%s).",
            congestion_svc._initialized,
        )

    except Exception as exc:
        logger.warning("Phase 3 services init warning: %s", exc)

    yield

    # ── Shutdown ───────────────────────────────────────────────────────────────
    for svc_attr in ("elevator_service", "outage_service", "pedway_service", "congestion_service"):
        svc = getattr(app.state, svc_attr, None)
        if svc and hasattr(svc, "shutdown"):
            try:
                await svc.shutdown()
            except Exception:
                pass
    if app.state.redis:
        await app.state.redis.close()
    logger.info("LoopSense + LoopNav shutting down.")


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="LoopSense + LoopNav API",
    description=(
        "LoopSense: real-time pedestrian hazard intelligence for the Chicago Loop.\n"
        "LoopNav: 3-layer pedestrian navigation (street / Lower Wacker / pedway)."
    ),
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_exception_handler(StarletteHTTPException, http_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(RequestValidationError, validation_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(Exception, unhandled_exception_handler)  # type: ignore[arg-type]

# Middleware — outermost runs last (CORS first, then logging)
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Request-ID"],
    expose_headers=["X-Request-ID", "X-Process-Time"],
)

# ── Existing LoopSense routers ─────────────────────────────────────────────────
PREFIX = "/api/v1"
app.include_router(reports.router,  prefix=PREFIX)
app.include_router(hazards.router,  prefix=PREFIX)
app.include_router(cta.router,      prefix=PREFIX)
app.include_router(weather.router,  prefix=PREFIX)
app.include_router(routing.router,  prefix=PREFIX)

# ── LoopNav routers ────────────────────────────────────────────────────────────
app.include_router(nav_route.router,  prefix=PREFIX)
app.include_router(layers.router,     prefix=PREFIX)
app.include_router(ai_route.router,   prefix=PREFIX)
app.include_router(stats.router,      prefix=PREFIX)
app.include_router(ws_session.router)          # WebSocket — no /api prefix
app.include_router(isochrone.router,  prefix=PREFIX)

# ── Phase 3 — CTA Intelligence ─────────────────────────────────────────────────
app.include_router(ada.router,        prefix=PREFIX)
app.include_router(tracker.router,    prefix=PREFIX)
app.include_router(station.router,    prefix=PREFIX)
app.include_router(congestion.router, prefix=PREFIX)

# ── Existing LoopSense WebSocket (hazard broadcast) ───────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """
    Real-time hazard broadcast stream.
    Events: new_report | upvote | report_expired
    """
    await manager.connect(ws)
    await manager.send_to(ws, "connected", {
        "message": "Connected to LoopSense real-time stream.",
        "active_connections": manager.active_count,
    })
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await manager.send_to(ws, "pong", {})
    except WebSocketDisconnect:
        manager.disconnect(ws)
        logger.info("LoopSense WebSocket client disconnected.")


# ── Health & Info ──────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health():
    graph_nodes = 0
    graph_edges = 0
    redis_ok    = False

    if hasattr(app.state, "graph") and app.state.graph:
        graph_nodes = app.state.graph.number_of_nodes()
        graph_edges = app.state.graph.number_of_edges()

    if hasattr(app.state, "redis") and app.state.redis:
        try:
            await app.state.redis.ping()
            redis_ok = True
        except Exception:
            pass

    return {
        "status":            "ok",
        "service":           "LoopSense + LoopNav API",
        "version":           "2.0.0",
        "graph_nodes":       graph_nodes,
        "graph_edges":       graph_edges,
        "graph_mode":        "stub" if settings.USE_STUB_GRAPH else "real",
        "redis":             "connected" if redis_ok else "unavailable",
        "active_ws_connections": manager.active_count,
    }


@app.get("/", tags=["System"])
async def root():
    return {
        "name":       "LoopSense + LoopNav",
        "tagline":    "Navigate Chicago's 3 Levels.",
        "docs":       "/docs",
        "health":     "/health",
        "websocket":  "ws://<host>/ws",
        "nav_route":  "POST /api/v1/nav/route",
        "ai_route":   "POST /api/v1/nav/ai-route",
        "layers":     "GET  /api/v1/nav/layers/{street|mid|pedway}",
        "isochrone":  "POST /api/v1/nav/isochrone",
        "stats":      "GET  /api/v1/nav/stats/stream  (SSE)",
        "ws_session":  "WS   /ws/session/{id}",
        "ada":         "GET  /api/v1/nav/ada/stations",
        "tracker":     "GET  /api/v1/nav/tracker/arrivals/{station_key}",
        "sync":        "POST /api/v1/nav/tracker/sync",
        "station":     "GET  /api/v1/nav/station/{station_key}",
        "congestion":  "GET  /api/v1/nav/congestion/{station_key}",
    }
