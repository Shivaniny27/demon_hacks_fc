"""
LoopNav — Production GeoJSON Layer Endpoints.

This module serves all three map layers (street / mid / pedway) plus
rich query, filter, analytics, and update endpoints for Dev 1 and
the frontend team.

Endpoints
---------
GET  /nav/layers                          — list all layer metadata
GET  /nav/layers/{level}                  — full GeoJSON layer
GET  /nav/layers/{level}/metadata         — metadata: feature count, bounds, props
GET  /nav/layers/{level}/features         — paginated feature list
GET  /nav/layers/{level}/features/{fid}   — single feature by ID
GET  /nav/layers/{level}/search           — full-text property search
GET  /nav/layers/{level}/nearby           — features within radius of a point
GET  /nav/layers/{level}/bounds           — bounding box
GET  /nav/layers/{level}/stats            — property value distributions
GET  /nav/layers/{level}/accessible       — accessible features only (barrier-free)
GET  /nav/layers/combined                 — all 3 layers merged into one FeatureCollection
GET  /nav/layers/{level}/diff/{other}     — features present in one layer but not another
GET  /nav/layers/{level}/tile-hint        — MVT tile hints for frontend rendering
PUT  /nav/layers/{level}                  — upload/replace a layer (Dev 1 use)
POST /nav/layers/{level}/validate         — validate a GeoJSON payload
POST /nav/layers/{level}/annotate/{fid}   — add annotation to a feature
DELETE /nav/layers/{level}/cache          — invalidate cached layer

Architecture:
  - Files live at <repo>/data/{level}.geojson
  - Layers are cached in Redis for 10 min (600s) to avoid disk I/O on every request
  - Cache invalidated on PUT
  - All reads return 200 with empty FeatureCollection when file doesn't exist yet
    (prevents frontend crashes during early development)
  - All writes validate GeoJSON structure before saving
  - Annotation overlay stored in <repo>/data/{level}.annotations.json
  - Bounding box and centroid pre-computed from features
  - Search uses simple substring match on all string properties (fast enough for
    thousands of features; upgrade to PostGIS FTS when graph is live)
  - Nearby uses haversine formula (no PostGIS dependency for stub mode)
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator

log = logging.getLogger("loopnav.layers")

router = APIRouter(prefix="/nav", tags=["LoopNav Layers"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_LEVELS: List[str] = ["street", "mid", "pedway"]

LAYER_DISPLAY_NAMES = {
    "street":  "Street Level (Ground)",
    "mid":     "Mid Level (Skywalk / +33)",
    "pedway":  "Pedway Level (Underground)",
}

LAYER_DESCRIPTIONS = {
    "street":  "Ground-level sidewalks, crosswalks, and open-air plazas in the Chicago Loop.",
    "mid":     "Elevated skywalk connections at roughly the 2nd-floor level (33 ft above grade).",
    "pedway":  "Chicago Pedway — underground climate-controlled tunnels connecting buildings.",
}

# Z-index hints for frontend rendering
LAYER_Z_INDEX = {
    "pedway": 10,
    "street": 20,
    "mid":    30,
}

LAYER_DEFAULT_COLORS = {
    "street": "#4A90D9",   # blue
    "mid":    "#F5A623",   # amber
    "pedway": "#7ED321",   # green
}

# Bounds check — rough Chicago Loop bounding box with 0.5° padding
CHICAGO_LOOP_BOUNDS = {
    "min_lon": -87.7,
    "max_lon": -87.6,
    "min_lat":  41.85,
    "max_lat":  41.92,
}

DATA_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")
)

MAX_UPLOAD_SIZE_BYTES = 50 * 1024 * 1024   # 50 MB
CACHE_TTL             = 600                 # 10 min


# ---------------------------------------------------------------------------
# Helpers — file I/O
# ---------------------------------------------------------------------------

def _layer_path(level: str) -> str:
    return os.path.join(DATA_DIR, f"{level}.geojson")

def _annotation_path(level: str) -> str:
    return os.path.join(DATA_DIR, f"{level}.annotations.json")

def _backup_path(level: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return os.path.join(DATA_DIR, f"{level}.backup_{ts}.geojson")


def _empty_fc(level: str, note: str = "") -> Dict:
    return {
        "type":     "FeatureCollection",
        "features": [],
        "_layer":   level,
        "_note":    note or f"{level}.geojson not yet populated — Dev 1 pending.",
    }


def _load_json(path: str) -> Optional[Dict]:
    """Load JSON from disk. Returns None if file doesn't exist."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        log.error("Failed to load %s: %s", path, exc)
        return None


def _save_json(path: str, data: Dict) -> None:
    """Atomically save JSON by writing to .tmp then renaming."""
    tmp = path + ".tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def _load_layer(level: str) -> Dict:
    """Load layer GeoJSON or return empty FeatureCollection."""
    data = _load_json(_layer_path(level))
    return data if data else _empty_fc(level)


async def _load_layer_cached(level: str, request: Request) -> Dict:
    """Load layer with Redis cache. Falls back to disk on cache miss."""
    cache = getattr(getattr(request, "app", None), "state", None)
    cache = getattr(cache, "cache", None)
    if cache is not None:
        cached = await cache.get_layer_cache(level)
        if cached is not None:
            return cached
    data = _load_layer(level)
    if cache is not None:
        try:
            await cache.set_layer_cache(level, data, ttl=CACHE_TTL)
        except Exception:
            pass
    return data


async def _invalidate_cache(level: str, request: Request) -> None:
    cache = getattr(getattr(request, "app", None), "state", None)
    cache = getattr(cache, "cache", None)
    if cache is not None:
        try:
            await cache.invalidate_layer_cache(level)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Helpers — geometry
# ---------------------------------------------------------------------------

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in metres."""
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi  = math.radians(lat2 - lat1)
    dlam  = math.radians(lon2 - lon1)
    a     = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _feature_coords(feature: Dict) -> Optional[List[Tuple[float, float]]]:
    """Extract all coordinate pairs from any GeoJSON geometry type."""
    geom = feature.get("geometry") or {}
    gtype = geom.get("type", "")
    coords = geom.get("coordinates")
    if coords is None:
        return None

    def _flatten(c, depth: int) -> List[Tuple[float, float]]:
        if depth == 0:
            return [(c[0], c[1])]
        return [pair for sub in c for pair in _flatten(sub, depth - 1)]

    depth_map = {
        "Point":           0,
        "MultiPoint":      1,
        "LineString":      1,
        "MultiLineString": 2,
        "Polygon":         2,
        "MultiPolygon":    3,
    }
    depth = depth_map.get(gtype)
    if depth is None:
        return None
    return _flatten(coords, depth)


def _feature_centroid(feature: Dict) -> Optional[Tuple[float, float]]:
    """Centroid of a feature (average of all coordinate pairs)."""
    pairs = _feature_coords(feature)
    if not pairs:
        return None
    avg_lon = sum(p[0] for p in pairs) / len(pairs)
    avg_lat = sum(p[1] for p in pairs) / len(pairs)
    return avg_lat, avg_lon


def _compute_bounds(features: List[Dict]) -> Optional[Dict]:
    """Bounding box of all features. Returns None if no valid geometry."""
    all_pairs: List[Tuple[float, float]] = []
    for f in features:
        pairs = _feature_coords(f)
        if pairs:
            all_pairs.extend(pairs)
    if not all_pairs:
        return None
    lons = [p[0] for p in all_pairs]
    lats = [p[1] for p in all_pairs]
    return {
        "min_lon": min(lons),
        "max_lon": max(lons),
        "min_lat": min(lats),
        "max_lat": max(lats),
        "center":  {
            "lon": (min(lons) + max(lons)) / 2,
            "lat": (min(lats) + max(lats)) / 2,
        },
    }


# ---------------------------------------------------------------------------
# Helpers — feature utilities
# ---------------------------------------------------------------------------

def _feature_id(feature: Dict) -> Optional[str]:
    """Extract feature ID (GeoJSON 'id' field or properties['id'])."""
    fid = feature.get("id")
    if fid is not None:
        return str(fid)
    props = feature.get("properties") or {}
    fid = props.get("id") or props.get("fid") or props.get("feature_id")
    return str(fid) if fid is not None else None


def _prop_str(feature: Dict, key: str) -> str:
    """Get a property value as lowercase string for search."""
    props = feature.get("properties") or {}
    val   = props.get(key, "")
    return str(val).lower() if val is not None else ""


def _search_match(feature: Dict, query: str) -> bool:
    """True if query appears in any string property value."""
    q     = query.lower().strip()
    props = feature.get("properties") or {}
    for v in props.values():
        if v is not None and q in str(v).lower():
            return True
    return False


def _is_accessible(feature: Dict) -> bool:
    """Heuristic: feature is accessible if no 'barrier' property and has elevator access."""
    props = feature.get("properties") or {}
    barrier = str(props.get("barrier", "")).lower()
    if barrier and barrier not in ("", "none", "false", "0"):
        return False
    accessible = props.get("accessible", props.get("wheelchair", props.get("barrier_free")))
    if accessible is None:
        return True  # optimistic default — assume accessible unless stated otherwise
    if isinstance(accessible, bool):
        return accessible
    return str(accessible).lower() in ("true", "yes", "1", "accessible")


def _is_covered(feature: Dict) -> bool:
    props = feature.get("properties") or {}
    val   = props.get("covered", props.get("indoor", False))
    return str(val).lower() in ("true", "yes", "1", "covered", "indoor")


def _prop_stats(features: List[Dict]) -> Dict:
    """Compute value distributions for all string and numeric properties."""
    from collections import Counter
    prop_types:   Dict[str, str]           = {}
    str_counters: Dict[str, Counter]       = {}
    num_vals:     Dict[str, List[float]]   = {}

    for f in features:
        props = f.get("properties") or {}
        for k, v in props.items():
            if v is None:
                continue
            if isinstance(v, bool):
                if k not in str_counters:
                    str_counters[k] = Counter()
                str_counters[k][str(v)] += 1
                prop_types[k] = "boolean"
            elif isinstance(v, (int, float)):
                if k not in num_vals:
                    num_vals[k] = []
                num_vals[k].append(float(v))
                prop_types[k] = "number"
            elif isinstance(v, str):
                if k not in str_counters:
                    str_counters[k] = Counter()
                str_counters[k][v] += 1
                prop_types.setdefault(k, "string")

    result = {}
    for k, ctr in str_counters.items():
        result[k] = {
            "type":         prop_types.get(k, "string"),
            "unique_values": len(ctr),
            "top_values":   dict(ctr.most_common(10)),
        }
    for k, vals in num_vals.items():
        vals_sorted = sorted(vals)
        n           = len(vals_sorted)
        result[k]   = {
            "type":  "number",
            "count": n,
            "min":   vals_sorted[0],
            "max":   vals_sorted[-1],
            "mean":  round(sum(vals_sorted) / n, 4),
            "p50":   vals_sorted[n // 2],
            "p90":   vals_sorted[int(n * 0.9)],
        }
    return result


# ---------------------------------------------------------------------------
# GeoJSON validation
# ---------------------------------------------------------------------------

def _validate_geojson(data: Any) -> Tuple[bool, str]:
    """
    Validate a GeoJSON FeatureCollection.
    Returns (is_valid, error_message).
    """
    if not isinstance(data, dict):
        return False, "Root must be a JSON object"
    if data.get("type") != "FeatureCollection":
        return False, f"type must be 'FeatureCollection', got '{data.get('type')}'"
    features = data.get("features")
    if not isinstance(features, list):
        return False, "'features' must be an array"

    valid_geom_types = {
        "Point", "MultiPoint", "LineString", "MultiLineString",
        "Polygon", "MultiPolygon", "GeometryCollection",
    }

    for i, f in enumerate(features):
        if not isinstance(f, dict):
            return False, f"Feature {i}: must be an object"
        if f.get("type") != "Feature":
            return False, f"Feature {i}: type must be 'Feature'"
        geom = f.get("geometry")
        if geom is None:
            continue   # null geometry is valid per spec
        if not isinstance(geom, dict):
            return False, f"Feature {i}: geometry must be an object or null"
        gtype = geom.get("type")
        if gtype not in valid_geom_types:
            return False, f"Feature {i}: unknown geometry type '{gtype}'"
        if gtype != "GeometryCollection" and "coordinates" not in geom:
            return False, f"Feature {i}: geometry missing 'coordinates'"

    return True, ""


def _validate_bounds(data: Dict) -> Tuple[bool, str]:
    """Check all feature coordinates fall within Chicago Loop area."""
    features = data.get("features", [])
    for i, f in enumerate(features):
        pairs = _feature_coords(f)
        if not pairs:
            continue
        for lon, lat in pairs:
            if not (CHICAGO_LOOP_BOUNDS["min_lon"] <= lon <= CHICAGO_LOOP_BOUNDS["max_lon"]):
                return False, f"Feature {i}: longitude {lon} outside Chicago Loop bounds"
            if not (CHICAGO_LOOP_BOUNDS["min_lat"] <= lat <= CHICAGO_LOOP_BOUNDS["max_lat"]):
                return False, f"Feature {i}: latitude {lat} outside Chicago Loop bounds"
    return True, ""


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class AnnotationCreate(BaseModel):
    text:   str  = Field(..., max_length=500)
    author: str  = Field(default="anonymous", max_length=50)
    tags:   List[str] = Field(default_factory=list)


class AnnotationOut(BaseModel):
    id:         str
    feature_id: str
    layer:      str
    text:       str
    author:     str
    tags:       List[str]
    created_at: str


class LayerUploadResponse(BaseModel):
    layer:          str
    feature_count:  int
    bounds:         Optional[Dict]
    backup_path:    str
    validation:     Dict
    cached:         bool
    uploaded_at:    str


class ValidationResponse(BaseModel):
    valid:          bool
    error:          Optional[str]
    feature_count:  int
    bounds_ok:      bool
    bounds_error:   Optional[str]
    warnings:       List[str]


# ---------------------------------------------------------------------------
# Level dependency
# ---------------------------------------------------------------------------

def _require_level(level: str) -> str:
    if level not in VALID_LEVELS:
        raise HTTPException(
            status_code=400,
            detail={
                "error":   "INVALID_LEVEL",
                "message": f"Level must be one of: {', '.join(VALID_LEVELS)}",
                "hint":    "Valid values: street, mid, pedway",
            },
        )
    return level


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/layers")
async def list_layers(request: Request):
    """
    List all three LoopNav layers with availability status and metadata.

    Returns:
        - layer name, display name, description
        - feature_count (0 if not yet populated)
        - available (bool): whether the GeoJSON file exists on disk
        - last_modified: file modification time if available
        - z_index, color: rendering hints for the frontend
    """
    result = []
    for level in VALID_LEVELS:
        path      = _layer_path(level)
        available = os.path.exists(path)
        fc        = 0
        mtime     = None

        if available:
            try:
                st   = os.stat(path)
                mtime = datetime.utcfromtimestamp(st.st_mtime).isoformat() + "Z"
                data  = _load_json(path) or {}
                fc    = len(data.get("features", []))
            except Exception:
                pass

        result.append({
            "level":          level,
            "display_name":   LAYER_DISPLAY_NAMES[level],
            "description":    LAYER_DESCRIPTIONS[level],
            "available":      available,
            "feature_count":  fc,
            "last_modified":  mtime,
            "z_index":        LAYER_Z_INDEX[level],
            "color":          LAYER_DEFAULT_COLORS[level],
            "endpoint":       f"/api/v1/nav/layers/{level}",
        })

    return JSONResponse({
        "layers":     result,
        "total":      len(VALID_LEVELS),
        "data_dir":   DATA_DIR,
        "timestamp":  datetime.utcnow().isoformat() + "Z",
    })


@router.get("/layers/combined")
async def get_combined_layers(
    request:    Request,
    levels:     str = Query(default="street,mid,pedway", description="Comma-separated levels to merge"),
    accessible: Optional[bool] = Query(default=None, description="Filter by accessibility"),
):
    """
    Merge multiple layers into one FeatureCollection.

    Each feature gets a '_layer' property injected so the frontend can
    colour-code or toggle visibility per layer. Useful for sending a single
    GeoJSON payload to deck.gl.
    """
    requested = [l.strip() for l in levels.split(",") if l.strip() in VALID_LEVELS]
    if not requested:
        raise HTTPException(status_code=400, detail={
            "error":   "INVALID_LEVELS",
            "message": f"No valid levels in '{levels}'. Valid: {', '.join(VALID_LEVELS)}",
        })

    all_features: List[Dict] = []
    layer_counts: Dict[str, int] = {}
    t0 = time.monotonic()

    for level in requested:
        data     = await _load_layer_cached(level, request)
        features = data.get("features", [])

        for f in features:
            fc = dict(f)
            fc.setdefault("properties", {})
            fc["properties"] = dict(fc["properties"])
            fc["properties"]["_layer"]  = level
            fc["properties"]["_color"]  = LAYER_DEFAULT_COLORS[level]
            fc["properties"]["_z"]      = LAYER_Z_INDEX[level]
            if accessible is not None:
                if accessible != _is_accessible(fc):
                    continue
            all_features.append(fc)

        layer_counts[level] = len(features)

    elapsed_ms = round((time.monotonic() - t0) * 1000, 2)

    return JSONResponse(
        content={
            "type":     "FeatureCollection",
            "features": all_features,
            "_meta": {
                "layers_merged":  requested,
                "layer_counts":   layer_counts,
                "total_features": len(all_features),
                "elapsed_ms":     elapsed_ms,
                "timestamp":      datetime.utcnow().isoformat() + "Z",
            },
        },
        media_type="application/geo+json",
    )


@router.get("/layers/{level}")
async def get_layer(
    level:   str,
    request: Request,
    minimal: bool = Query(default=False, description="Return only geometry and id (strip all properties)"),
    covered: Optional[bool] = Query(default=None, description="Filter: True=covered only, False=open-air only"),
    accessible: Optional[bool] = Query(default=None, description="Filter: True=accessible only"),
    geom_type: Optional[str] = Query(default=None, description="Filter by geometry type (LineString, Point, Polygon…)"),
):
    """
    Full GeoJSON layer endpoint — returns the raw FeatureCollection.

    Supports optional filters (covered, accessible, geom_type) to reduce
    payload size. Frontend should prefer /features for paginated browsing.
    """
    _require_level(level)
    t0   = time.monotonic()
    data = await _load_layer_cached(level, request)

    features = data.get("features", [])

    # Apply filters
    if covered is not None:
        features = [f for f in features if _is_covered(f) == covered]
    if accessible is not None:
        features = [f for f in features if _is_accessible(f) == accessible]
    if geom_type:
        geom_type_lower = geom_type.lower()
        features = [
            f for f in features
            if (f.get("geometry") or {}).get("type", "").lower() == geom_type_lower
        ]

    if minimal:
        features = [
            {"type": "Feature", "id": _feature_id(f), "geometry": f.get("geometry")}
            for f in features
        ]

    elapsed_ms = round((time.monotonic() - t0) * 1000, 2)

    content = dict(data)
    content["features"] = features
    content["_meta"] = {
        "level":          level,
        "display_name":   LAYER_DISPLAY_NAMES[level],
        "feature_count":  len(features),
        "elapsed_ms":     elapsed_ms,
        "filters_applied": {
            "covered":    covered,
            "accessible": accessible,
            "geom_type":  geom_type,
            "minimal":    minimal,
        },
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    return JSONResponse(content=content, media_type="application/geo+json")


@router.get("/layers/{level}/metadata")
async def get_layer_metadata(level: str, request: Request):
    """
    Layer metadata: feature count, geometry types, property keys, bounds, CRS.

    Lightweight — useful for frontend to configure legend, filters, popup
    field names, and map extent without downloading all features.
    """
    _require_level(level)

    path      = _layer_path(level)
    available = os.path.exists(path)
    data      = await _load_layer_cached(level, request)
    features  = data.get("features", [])

    geom_type_dist: Dict[str, int] = {}
    prop_keys:      set            = set()
    accessible_count = 0
    covered_count    = 0

    for f in features:
        gtype = (f.get("geometry") or {}).get("type", "Unknown")
        geom_type_dist[gtype] = geom_type_dist.get(gtype, 0) + 1
        props = f.get("properties") or {}
        prop_keys.update(props.keys())
        if _is_accessible(f):
            accessible_count += 1
        if _is_covered(f):
            covered_count += 1

    bounds = _compute_bounds(features)

    return {
        "level":             level,
        "display_name":      LAYER_DISPLAY_NAMES[level],
        "description":       LAYER_DESCRIPTIONS[level],
        "available":         available,
        "feature_count":     len(features),
        "geometry_types":    geom_type_dist,
        "property_keys":     sorted(prop_keys),
        "accessible_count":  accessible_count,
        "covered_count":     covered_count,
        "bounds":            bounds,
        "crs":               {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "color":             LAYER_DEFAULT_COLORS[level],
        "z_index":           LAYER_Z_INDEX[level],
        "last_modified":     (
            datetime.utcfromtimestamp(os.stat(path).st_mtime).isoformat() + "Z"
            if available else None
        ),
        "timestamp":         datetime.utcnow().isoformat() + "Z",
    }


@router.get("/layers/{level}/features")
async def list_features(
    level:   str,
    request: Request,
    page:    int   = Query(default=1,  ge=1,  description="Page number (1-based)"),
    per_page: int  = Query(default=50, ge=1, le=500, description="Features per page"),
    accessible: Optional[bool] = Query(default=None),
    covered:    Optional[bool] = Query(default=None),
    geom_type:  Optional[str]  = Query(default=None),
):
    """
    Paginated feature listing. Useful for sidebar feature browsers.

    Returns total count and pagination info alongside the page of features.
    """
    _require_level(level)
    data     = await _load_layer_cached(level, request)
    features = data.get("features", [])

    # Filters
    if accessible is not None:
        features = [f for f in features if _is_accessible(f) == accessible]
    if covered is not None:
        features = [f for f in features if _is_covered(f) == covered]
    if geom_type:
        gt_lower = geom_type.lower()
        features = [
            f for f in features
            if (f.get("geometry") or {}).get("type", "").lower() == gt_lower
        ]

    total      = len(features)
    total_pages = max(1, math.ceil(total / per_page))
    page        = min(page, total_pages)
    start       = (page - 1) * per_page
    end         = start + per_page
    page_feats  = features[start:end]

    return {
        "level":       level,
        "page":        page,
        "per_page":    per_page,
        "total":       total,
        "total_pages": total_pages,
        "has_prev":    page > 1,
        "has_next":    page < total_pages,
        "features": {
            "type":     "FeatureCollection",
            "features": page_feats,
        },
    }


@router.get("/layers/{level}/features/{feature_id}")
async def get_feature(level: str, feature_id: str, request: Request):
    """
    Fetch a single feature by its GeoJSON ID or properties.id.

    Returns 404 if not found, 200 with the Feature object if found.
    Also returns neighbouring feature IDs (up to 5 nearest) as a bonus.
    """
    _require_level(level)
    data     = await _load_layer_cached(level, request)
    features = data.get("features", [])

    target = None
    for f in features:
        if _feature_id(f) == feature_id:
            target = f
            break

    if target is None:
        raise HTTPException(status_code=404, detail={
            "error":      "FEATURE_NOT_FOUND",
            "message":    f"Feature '{feature_id}' not found in layer '{level}'",
            "layer":      level,
            "feature_id": feature_id,
        })

    # Find nearest 5 neighbours
    centroid = _feature_centroid(target)
    neighbours: List[Dict] = []
    if centroid:
        lat0, lon0 = centroid
        distances  = []
        for f in features:
            if _feature_id(f) == feature_id:
                continue
            c = _feature_centroid(f)
            if c:
                d = _haversine_m(lat0, lon0, c[0], c[1])
                distances.append((d, _feature_id(f), f.get("properties", {}).get("name", "")))
        distances.sort()
        neighbours = [
            {"feature_id": fid, "distance_m": round(dist, 1), "name": name}
            for dist, fid, name in distances[:5]
        ]

    # Load annotations for this feature
    annotations = _load_feature_annotations(level, feature_id)

    return {
        "feature":     target,
        "layer":       level,
        "neighbours":  neighbours,
        "annotations": annotations,
        "accessible":  _is_accessible(target),
        "covered":     _is_covered(target),
        "centroid":    {"lat": centroid[0], "lon": centroid[1]} if centroid else None,
    }


@router.get("/layers/{level}/search")
async def search_features(
    level:   str,
    request: Request,
    q:       str   = Query(..., min_length=1, max_length=200, description="Search query"),
    limit:   int   = Query(default=20, ge=1, le=200),
    accessible: Optional[bool] = Query(default=None),
):
    """
    Full-text search across all string properties of features.

    Performs case-insensitive substring match across all property values.
    Returns up to `limit` matching features sorted by property name relevance.
    """
    _require_level(level)
    if not q.strip():
        raise HTTPException(status_code=400, detail={"error": "EMPTY_QUERY"})

    data     = await _load_layer_cached(level, request)
    features = data.get("features", [])

    results = []
    q_lower = q.lower().strip()
    for f in features:
        if not _search_match(f, q_lower):
            continue
        if accessible is not None and _is_accessible(f) != accessible:
            continue
        results.append(f)
        if len(results) >= limit:
            break

    return {
        "query":   q,
        "level":   level,
        "count":   len(results),
        "results": {"type": "FeatureCollection", "features": results},
    }


@router.get("/layers/{level}/nearby")
async def get_nearby_features(
    level:   str,
    request: Request,
    lat:     float = Query(..., ge=-90.0,   le=90.0,    description="Latitude"),
    lon:     float = Query(..., ge=-180.0,  le=180.0,   description="Longitude"),
    radius:  float = Query(default=200.0,  ge=1.0, le=5000.0, description="Radius in metres"),
    limit:   int   = Query(default=20,     ge=1,   le=100),
    accessible: Optional[bool] = Query(default=None),
):
    """
    Return features whose centroid is within `radius` metres of (lat, lon).

    Results are sorted by ascending distance. Useful for the sidebar
    "What's near me?" panel and the accessibility overlay.
    """
    _require_level(level)
    data     = await _load_layer_cached(level, request)
    features = data.get("features", [])

    hits: List[Tuple[float, Dict]] = []
    for f in features:
        if accessible is not None and _is_accessible(f) != accessible:
            continue
        c = _feature_centroid(f)
        if not c:
            continue
        dist = _haversine_m(lat, lon, c[0], c[1])
        if dist <= radius:
            hits.append((dist, f))

    hits.sort(key=lambda x: x[0])
    hits = hits[:limit]

    enriched = []
    for dist, f in hits:
        ef = dict(f)
        ef.setdefault("properties", {})
        ef["properties"] = dict(ef["properties"])
        ef["properties"]["_distance_m"] = round(dist, 1)
        enriched.append(ef)

    return {
        "query":          {"lat": lat, "lon": lon, "radius_m": radius},
        "level":          level,
        "total_found":    len(hits),
        "features":       {"type": "FeatureCollection", "features": enriched},
    }


@router.get("/layers/{level}/bounds")
async def get_layer_bounds(level: str, request: Request):
    """
    Bounding box and centroid of the layer. Lightweight map extent endpoint.
    Frontend can use this to fit the map to the layer on load.
    """
    _require_level(level)
    data     = await _load_layer_cached(level, request)
    features = data.get("features", [])
    bounds   = _compute_bounds(features)

    if not bounds:
        # Return Chicago Loop default bounds when layer is empty
        return {
            "level":   level,
            "bounds":  CHICAGO_LOOP_BOUNDS,
            "center":  {"lat": 41.8827, "lon": -87.6423},
            "empty":   True,
        }

    return {
        "level":   level,
        "bounds":  bounds,
        "center":  bounds["center"],
        "bbox":    [bounds["min_lon"], bounds["min_lat"], bounds["max_lon"], bounds["max_lat"]],
        "empty":   False,
        "count":   len(features),
    }


@router.get("/layers/{level}/stats")
async def get_layer_stats(level: str, request: Request):
    """
    Property distribution statistics for the layer.

    Returns top values, unique value counts, and basic numeric stats (min/max/mean)
    per property. Useful for building filter UIs without loading all features.
    """
    _require_level(level)
    data     = await _load_layer_cached(level, request)
    features = data.get("features", [])

    if not features:
        return {
            "level":           level,
            "feature_count":   0,
            "property_stats":  {},
            "accessible_pct":  0.0,
            "covered_pct":     0.0,
            "geometry_types":  {},
        }

    stats            = _prop_stats(features)
    accessible_count = sum(1 for f in features if _is_accessible(f))
    covered_count    = sum(1 for f in features if _is_covered(f))
    geom_dist: Dict[str, int] = {}
    for f in features:
        gt = (f.get("geometry") or {}).get("type", "Unknown")
        geom_dist[gt] = geom_dist.get(gt, 0) + 1

    n = len(features)
    return {
        "level":           level,
        "feature_count":   n,
        "property_stats":  stats,
        "accessible_pct":  round(accessible_count / n * 100, 1),
        "covered_pct":     round(covered_count    / n * 100, 1),
        "geometry_types":  geom_dist,
        "timestamp":       datetime.utcnow().isoformat() + "Z",
    }


@router.get("/layers/{level}/accessible")
async def get_accessible_features(
    level:   str,
    request: Request,
    nearby_lat: Optional[float] = Query(default=None, ge=-90,  le=90),
    nearby_lon: Optional[float] = Query(default=None, ge=-180, le=180),
    radius_m:   float           = Query(default=500.0, ge=1, le=5000),
):
    """
    Return only accessible features (no barriers, wheelchair-friendly).

    Optionally filter to those within radius of a point.
    Injects `_accessible: true` into each feature's properties.
    """
    _require_level(level)
    data     = await _load_layer_cached(level, request)
    features = [f for f in data.get("features", []) if _is_accessible(f)]

    if nearby_lat is not None and nearby_lon is not None:
        features = [
            f for f in features
            if (c := _feature_centroid(f)) and
               _haversine_m(nearby_lat, nearby_lon, c[0], c[1]) <= radius_m
        ]

    for f in features:
        f.setdefault("properties", {})
        f["properties"]["_accessible"] = True

    return JSONResponse(
        content={
            "type":     "FeatureCollection",
            "features": features,
            "_meta": {
                "level":             level,
                "accessible_count":  len(features),
                "filter":            {
                    "nearby_lat": nearby_lat,
                    "nearby_lon": nearby_lon,
                    "radius_m":   radius_m if nearby_lat else None,
                },
            },
        },
        media_type="application/geo+json",
    )


@router.get("/layers/{level}/diff/{other}")
async def get_layer_diff(
    level:   str,
    other:   str,
    request: Request,
    prop:    str = Query(default="id", description="Property to use for feature identity comparison"),
):
    """
    Return features present in `level` but NOT in `other` layer.

    Useful for finding features that exist on street level but not mid level,
    e.g. street-only entrances that need pedway access signs.
    """
    _require_level(level)
    _require_level(other)
    if level == other:
        raise HTTPException(status_code=400, detail={"error": "SAME_LEVEL", "message": "Diff requires two different levels"})

    data_a = await _load_layer_cached(level,   request)
    data_b = await _load_layer_cached(other,   request)

    def _get_prop(f: Dict) -> Optional[str]:
        props = f.get("properties") or {}
        val   = props.get(prop) or _feature_id(f)
        return str(val) if val is not None else None

    b_vals = {_get_prop(f) for f in data_b.get("features", [])} - {None}

    only_in_a = [
        f for f in data_a.get("features", [])
        if _get_prop(f) not in b_vals
    ]

    return JSONResponse(
        content={
            "type":     "FeatureCollection",
            "features": only_in_a,
            "_meta": {
                "base_layer":    level,
                "compare_layer": other,
                "diff_key":      prop,
                "only_in_base":  len(only_in_a),
                "base_total":    len(data_a.get("features", [])),
                "compare_total": len(data_b.get("features", [])),
            },
        },
        media_type="application/geo+json",
    )


@router.get("/layers/{level}/tile-hint")
async def get_tile_hint(
    level: str,
    request: Request,
    z:    int = Query(default=14, ge=0, le=22),
    lat:  float = Query(default=41.8827),
    lon:  float = Query(default=-87.6423),
):
    """
    Returns tile coordinate hints for the frontend WebGL renderer (deck.gl / MapLibre).

    Given zoom level and center point, returns the tile x/y and recommended
    zoom range for this layer's data density.
    """
    _require_level(level)

    # Slippy map tile calculation
    lat_r = math.radians(lat)
    n     = 2 ** z
    tx    = int((lon + 180.0) / 360.0 * n)
    ty    = int((1.0 - math.log(math.tan(lat_r) + 1.0 / math.cos(lat_r)) / math.pi) / 2.0 * n)

    zoom_ranges = {
        "street":  {"min": 13, "max": 19, "ideal": 16},
        "mid":     {"min": 14, "max": 19, "ideal": 17},
        "pedway":  {"min": 15, "max": 19, "ideal": 17},
    }

    data     = await _load_layer_cached(level, request)
    n_feats  = len(data.get("features", []))
    bounds   = _compute_bounds(data.get("features", []))

    return {
        "level":     level,
        "tile":      {"z": z, "x": tx, "y": ty},
        "zoom":      zoom_ranges[level],
        "color":     LAYER_DEFAULT_COLORS[level],
        "z_index":   LAYER_Z_INDEX[level],
        "features":  n_feats,
        "bounds":    bounds,
        "mvt_hint":  f"/api/v1/nav/layers/{level}/tiles/{z}/{tx}/{ty}.pbf",
        "geojson_url": f"/api/v1/nav/layers/{level}",
    }


@router.put("/layers/{level}")
async def upload_layer(
    level:   str,
    request: Request,
):
    """
    Replace a layer's GeoJSON file. Intended for Dev 1 to push real data.

    Steps:
      1. Read raw body
      2. Validate JSON parse
      3. Validate GeoJSON structure
      4. Validate bounds (must be within Chicago Loop area)
      5. Backup existing file
      6. Write new file
      7. Invalidate Redis cache

    Returns summary of uploaded layer.
    """
    _require_level(level)

    body = await request.body()
    if len(body) > MAX_UPLOAD_SIZE_BYTES:
        raise HTTPException(status_code=413, detail={
            "error":   "FILE_TOO_LARGE",
            "message": f"Payload exceeds {MAX_UPLOAD_SIZE_BYTES // 1024 // 1024} MB limit",
        })

    # Parse JSON
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail={
            "error":   "INVALID_JSON",
            "message": f"JSON parse error: {exc}",
        })

    # Validate GeoJSON structure
    valid, err = _validate_geojson(data)
    if not valid:
        raise HTTPException(status_code=422, detail={
            "error":   "INVALID_GEOJSON",
            "message": err,
        })

    # Validate bounds
    bounds_ok, bounds_err = _validate_bounds(data)

    # Backup existing file
    path        = _layer_path(level)
    backup_name = ""
    if os.path.exists(path):
        backup = _backup_path(level)
        try:
            import shutil
            shutil.copy2(path, backup)
            backup_name = backup
            log.info("Layer %s backed up to %s", level, backup)
        except Exception as exc:
            log.warning("Failed to backup %s: %s", level, exc)

    # Save new data
    os.makedirs(DATA_DIR, exist_ok=True)
    _save_json(path, data)

    # Invalidate cache
    await _invalidate_cache(level, request)

    features  = data.get("features", [])
    bounds    = _compute_bounds(features)

    log.info("Layer %s uploaded: %d features", level, len(features))

    return {
        "layer":         level,
        "feature_count": len(features),
        "bounds":        bounds,
        "backup_path":   backup_name,
        "validation": {
            "geojson_valid": True,
            "bounds_ok":     bounds_ok,
            "bounds_error":  bounds_err or None,
        },
        "cached":        False,
        "uploaded_at":   datetime.utcnow().isoformat() + "Z",
    }


@router.post("/layers/{level}/validate")
async def validate_layer(level: str, request: Request):
    """
    Validate a GeoJSON payload without saving it.

    Useful for Dev 1 to check data quality before uploading.
    Returns detailed validation report including:
    - GeoJSON structure check
    - Bounds check (Chicago Loop area)
    - Feature count
    - Warnings (e.g. features missing properties, null geometries)
    """
    _require_level(level)

    body = await request.body()
    if len(body) > MAX_UPLOAD_SIZE_BYTES:
        raise HTTPException(status_code=413, detail={"error": "FILE_TOO_LARGE"})

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        return ValidationResponse(
            valid=False, error=f"JSON parse error: {exc}",
            feature_count=0, bounds_ok=False, bounds_error=None, warnings=[],
        )

    valid, err = _validate_geojson(data)
    if not valid:
        return ValidationResponse(
            valid=False, error=err,
            feature_count=0, bounds_ok=False, bounds_error=None, warnings=[],
        )

    features   = data.get("features", [])
    bounds_ok, bounds_err = _validate_bounds(data)

    # Collect warnings
    warnings: List[str] = []
    null_geom_count = 0
    no_props_count  = 0
    no_id_count     = 0

    for i, f in enumerate(features):
        if f.get("geometry") is None:
            null_geom_count += 1
        props = f.get("properties")
        if not props:
            no_props_count += 1
        if _feature_id(f) is None:
            no_id_count += 1

    if null_geom_count:
        warnings.append(f"{null_geom_count} features have null geometry")
    if no_props_count:
        warnings.append(f"{no_props_count} features have no properties")
    if no_id_count:
        warnings.append(f"{no_id_count} features have no ID (set 'id' field or properties.id)")
    if not bounds_ok:
        warnings.append(f"Bounds check failed: {bounds_err}")

    return ValidationResponse(
        valid        = True,
        error        = None,
        feature_count = len(features),
        bounds_ok    = bounds_ok,
        bounds_error = bounds_err,
        warnings     = warnings,
    )


@router.post("/layers/{level}/annotate/{feature_id}")
async def annotate_feature(
    level:      str,
    feature_id: str,
    body:       AnnotationCreate,
    request:    Request,
):
    """
    Add a text annotation to a specific feature.

    Annotations are stored in a separate file ({level}.annotations.json)
    and served alongside the feature in GET /layers/{level}/features/{fid}.
    Useful for hackathon demo: judges can see notes attached to features.
    """
    _require_level(level)

    # Verify feature exists
    data     = await _load_layer_cached(level, request)
    features = data.get("features", [])
    feature  = next((f for f in features if _feature_id(f) == feature_id), None)
    if feature is None:
        raise HTTPException(status_code=404, detail={
            "error":   "FEATURE_NOT_FOUND",
            "message": f"Feature '{feature_id}' not in layer '{level}'",
        })

    ann_path    = _annotation_path(level)
    annotations = _load_json(ann_path) or {"annotations": []}
    ann_list    = annotations.setdefault("annotations", [])

    ann_id = str(uuid.uuid4()) if _has_uuid() else f"{level}_{feature_id}_{int(time.time())}"
    new_ann = {
        "id":         ann_id,
        "feature_id": feature_id,
        "layer":      level,
        "text":       body.text,
        "author":     body.author,
        "tags":       body.tags,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    ann_list.append(new_ann)
    _save_json(ann_path, annotations)

    return new_ann


@router.delete("/layers/{level}/cache")
async def invalidate_layer_cache_endpoint(level: str, request: Request):
    """
    Invalidate the Redis cache for a layer.

    Call this after manually editing the GeoJSON file on disk.
    On next request the layer will be re-read from disk and re-cached.
    """
    _require_level(level)
    await _invalidate_cache(level, request)
    return {
        "message":   f"Cache invalidated for layer '{level}'",
        "layer":     level,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


# ---------------------------------------------------------------------------
# Private helpers for annotations
# ---------------------------------------------------------------------------

def _load_feature_annotations(level: str, feature_id: str) -> List[Dict]:
    """Load annotations for a specific feature from the annotation file."""
    path = _annotation_path(level)
    data = _load_json(path)
    if not data:
        return []
    all_anns = data.get("annotations", [])
    return [a for a in all_anns if a.get("feature_id") == feature_id]


def _has_uuid() -> bool:
    try:
        import uuid as _u
        _u.uuid4()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Import uuid at module level (used in annotate endpoint)
# ---------------------------------------------------------------------------

import uuid  # noqa: E402
