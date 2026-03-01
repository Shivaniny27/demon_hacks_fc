"""
LoopNav Data Downloader
Downloads all raw data sources needed to build the Chicago Loop graph.
Run this immediately -- downloads happen in background while you code.
"""

import os
import json
import time
import threading
import requests

RAW_DIR = os.path.join(os.path.dirname(__file__), "raw")
os.makedirs(RAW_DIR, exist_ok=True)

# Chicago Loop bounding box
LOOP_BBOX = {
    "south": 41.878,
    "west": -87.638,
    "north": 41.887,
    "east": -87.621,
}


def log(msg):
    safe = msg.encode("ascii", errors="replace").decode("ascii")
    print(f"[{time.strftime('%H:%M:%S')}] {safe}", flush=True)


# 1. Chicago Pedway Routes GeoJSON
def download_pedway():
    out = os.path.join(RAW_DIR, "pedway.geojson")
    if os.path.exists(out):
        log("pedway.geojson already exists -- skipping")
        return
    log("Downloading Chicago Pedway GeoJSON...")

    # Socrata JSON API -- more reliable than geospatial export
    urls = [
        "https://data.cityofchicago.org/resource/xjx9-jyd5.geojson?$limit=5000",
        "https://data.cityofchicago.org/resource/xjx9-jyd5.json?$limit=5000",
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=60, headers={"Accept": "application/json"})
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                features = []
                for row in data:
                    geom = row.get("the_geom") or row.get("geometry")
                    if geom:
                        props = {k: v for k, v in row.items() if k not in ("the_geom", "geometry")}
                        props["level"] = "pedway"
                        props["covered"] = True
                        features.append({"type": "Feature", "properties": props, "geometry": geom})
                data = {"type": "FeatureCollection", "features": features}
            with open(out, "w") as f:
                json.dump(data, f)
            log(f"[OK] pedway.geojson saved ({len(data.get('features', []))} features)")
            return
        except Exception as e:
            log(f"  Tried {url[:60]} -- {e}")

    log("[FAIL] All pedway URLs failed -- using hardcoded fallback")
    _write_pedway_fallback(out)


def _write_pedway_fallback(out):
    """Hardcoded key pedway segments for the Loop."""
    features = [
        {
            "type": "Feature",
            "properties": {"name": "Block 37 Connector", "level": "pedway", "covered": True,
                           "open_hours": "06:00-22:00"},
            "geometry": {"type": "LineString", "coordinates": [
                [-87.6279, 41.8830], [-87.6268, 41.8830]
            ]},
        },
        {
            "type": "Feature",
            "properties": {"name": "Chase Tower Pedway", "level": "pedway", "covered": True,
                           "open_hours": "06:00-20:00"},
            "geometry": {"type": "LineString", "coordinates": [
                [-87.6305, 41.8797], [-87.6290, 41.8797]
            ]},
        },
        {
            "type": "Feature",
            "properties": {"name": "Millennium Station Connector", "level": "pedway", "covered": True,
                           "open_hours": "05:00-23:00"},
            "geometry": {"type": "LineString", "coordinates": [
                [-87.6244, 41.8843], [-87.6244, 41.8830]
            ]},
        },
        {
            "type": "Feature",
            "properties": {"name": "City Hall - Daley Center Link", "level": "pedway", "covered": True,
                           "open_hours": "07:00-19:00"},
            "geometry": {"type": "LineString", "coordinates": [
                [-87.6333, 41.8839], [-87.6320, 41.8839]
            ]},
        },
        {
            "type": "Feature",
            "properties": {"name": "Macy's Pedway", "level": "pedway", "covered": True,
                           "open_hours": "09:00-21:00"},
            "geometry": {"type": "LineString", "coordinates": [
                [-87.6268, 41.8838], [-87.6260, 41.8838]
            ]},
        },
        {
            "type": "Feature",
            "properties": {"name": "Union Station Concourse", "level": "pedway", "covered": True,
                           "open_hours": "05:00-23:00"},
            "geometry": {"type": "LineString", "coordinates": [
                [-87.6408, 41.8789], [-87.6395, 41.8789]
            ]},
        },
    ]
    data = {"type": "FeatureCollection", "features": features}
    with open(out, "w") as f:
        json.dump(data, f)
    log(f"[OK] pedway.geojson written with {len(features)} hardcoded fallback segments")


# 2. OSM walkable streets via Overpass
def download_osm_streets():
    out = os.path.join(RAW_DIR, "streets_osm.geojson")
    if os.path.exists(out):
        log("streets_osm.geojson already exists -- skipping")
        return
    log("Downloading OSM walkable streets (Overpass)...")

    s, w, n, e = LOOP_BBOX["south"], LOOP_BBOX["west"], LOOP_BBOX["north"], LOOP_BBOX["east"]
    bbox = f"{s},{w},{n},{e}"
    query = f"""
    [out:json][timeout:60];
    (
      way["highway"~"footway|pedestrian|path|steps|corridor|sidewalk|crossing"]({bbox});
      way["highway"~"primary|secondary|tertiary|residential|unclassified"]["foot"!="no"]({bbox});
    );
    out body geom;
    """

    try:
        r = requests.post(
            "https://overpass-api.de/api/interpreter",
            data={"data": query},
            timeout=90,
        )
        r.raise_for_status()
        osm_data = r.json()

        features = []
        for elem in osm_data.get("elements", []):
            if elem["type"] == "way" and "geometry" in elem:
                coords = [[g["lon"], g["lat"]] for g in elem["geometry"]]
                if len(coords) < 2:
                    continue
                tags = elem.get("tags", {})
                features.append({
                    "type": "Feature",
                    "properties": {
                        "osm_id": elem["id"],
                        "highway": tags.get("highway", ""),
                        "name": tags.get("name", ""),
                        "level": "street",
                        "covered": False,
                        "sheltered": tags.get("covered", "no") == "yes",
                        "surface": tags.get("surface", "asphalt"),
                        "accessible": tags.get("wheelchair", "yes") != "no",
                    },
                    "geometry": {"type": "LineString", "coordinates": coords},
                })

        geojson = {"type": "FeatureCollection", "features": features}
        with open(out, "w") as f:
            json.dump(geojson, f)
        log(f"[OK] streets_osm.geojson saved ({len(features)} ways)")
    except Exception as e:
        log(f"[FAIL] OSM streets download failed: {e}")


# 3. CTA L station locations
def download_cta_stations():
    out = os.path.join(RAW_DIR, "cta_stations.geojson")
    if os.path.exists(out):
        log("cta_stations.geojson already exists -- skipping")
        return
    log("Downloading CTA L station locations...")

    s, w, n, e = LOOP_BBOX["south"], LOOP_BBOX["west"], LOOP_BBOX["north"], LOOP_BBOX["east"]

    # Socrata JSON API for CTA rail stations
    urls = [
        "https://data.cityofchicago.org/resource/8pix-ypme.geojson?$limit=5000",
        "https://data.cityofchicago.org/resource/8pix-ypme.json?$limit=5000",
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            data = r.json()

            if isinstance(data, list):
                features = []
                for row in data:
                    geom = row.get("location") or row.get("the_geom")
                    lat = float(row.get("location", {}).get("latitude", 0) or
                                row.get("lat", 0) or 0)
                    lon = float(row.get("location", {}).get("longitude", 0) or
                                row.get("lon", 0) or 0)
                    if not (w <= lon <= e and s <= lat <= n):
                        continue
                    features.append({
                        "type": "Feature",
                        "properties": {
                            "name": row.get("station_name", ""),
                            "station_id": row.get("map_id", ""),
                            "level": "street",
                            "type": "cta_station",
                        },
                        "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    })
            else:
                # Already GeoJSON
                features = []
                for feat in data.get("features", []):
                    coords = feat.get("geometry", {}).get("coordinates", [0, 0])
                    lon, lat = coords[0], coords[1]
                    if w <= lon <= e and s <= lat <= n:
                        feat["properties"]["level"] = "street"
                        feat["properties"]["type"] = "cta_station"
                        features.append(feat)

            out_data = {"type": "FeatureCollection", "features": features}
            with open(out, "w") as f:
                json.dump(out_data, f)
            log(f"[OK] cta_stations.geojson saved ({len(features)} Loop stations)")
            return
        except Exception as e:
            log(f"  Tried {url[:60]} -- {e}")

    log("[FAIL] CTA stations download failed -- using hardcoded fallback")
    _write_cta_fallback(out)


def _write_cta_fallback(out):
    stations = [
        {"name": "Ogilvie Transportation Center", "lon": -87.6400, "lat": 41.8826},
        {"name": "Union Station", "lon": -87.6404, "lat": 41.8789},
        {"name": "Millennium Station (Metra)", "lon": -87.6244, "lat": 41.8843},
        {"name": "LaSalle/Van Buren (Brown/Orange/Pink/Purple)", "lon": -87.6318, "lat": 41.8765},
        {"name": "Harold Washington Library-State/Van Buren", "lon": -87.6280, "lat": 41.8765},
        {"name": "Adams/Wabash", "lon": -87.6264, "lat": 41.8794},
        {"name": "Washington/Wabash", "lon": -87.6260, "lat": 41.8833},
        {"name": "Randolph/Wabash", "lon": -87.6260, "lat": 41.8847},
        {"name": "Washington/Wells", "lon": -87.6337, "lat": 41.8833},
        {"name": "Quincy/Wells", "lon": -87.6337, "lat": 41.8787},
        {"name": "LaSalle (Blue)", "lon": -87.6318, "lat": 41.8753},
        {"name": "Clark/Lake", "lon": -87.6308, "lat": 41.8856},
        {"name": "State/Lake", "lon": -87.6278, "lat": 41.8856},
        {"name": "Monroe (Red)", "lon": -87.6278, "lat": 41.8805},
        {"name": "Jackson (Red)", "lon": -87.6278, "lat": 41.8782},
    ]
    features = [{
        "type": "Feature",
        "properties": {"name": s["name"], "level": "street", "type": "cta_station"},
        "geometry": {"type": "Point", "coordinates": [s["lon"], s["lat"]]},
    } for s in stations]
    data = {"type": "FeatureCollection", "features": features}
    with open(out, "w") as f:
        json.dump(data, f)
    log(f"[OK] cta_stations.geojson written with {len(features)} hardcoded stations")


# 4. Chicago building footprints (non-critical)
def download_building_footprints():
    out = os.path.join(RAW_DIR, "buildings.geojson")
    if os.path.exists(out):
        log("buildings.geojson already exists -- skipping")
        return
    log("Downloading Chicago building footprints (Loop area)...")

    s, w, n, e = LOOP_BBOX["south"], LOOP_BBOX["west"], LOOP_BBOX["north"], LOOP_BBOX["east"]

    # Socrata JSON with bounding box -- correct API syntax
    url = (
        f"https://data.cityofchicago.org/resource/hz9b-7nh8.geojson"
        f"?$limit=2000"
        f"&$where=within_box(shape,{s},{w},{n},{e})"
    )
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        data = r.json()
        with open(out, "w") as f:
            json.dump(data, f)
        log(f"[OK] buildings.geojson saved ({len(data.get('features', []))} buildings)")
    except Exception as e:
        log(f"[SKIP] Building footprints failed: {e}")
        log("  Non-critical -- connector snapping uses node proximity fallback")


# Run all downloads in parallel threads
def main():
    log("=== LoopNav Data Downloader ===")
    log(f"Saving to: {RAW_DIR}")
    log("Starting parallel downloads...")

    tasks = [
        threading.Thread(target=download_pedway, name="pedway"),
        threading.Thread(target=download_osm_streets, name="osm"),
        threading.Thread(target=download_cta_stations, name="cta"),
        threading.Thread(target=download_building_footprints, name="buildings"),
    ]

    for t in tasks:
        t.daemon = True
        t.start()

    for t in tasks:
        t.join()

    log("=== All downloads complete ===")
    log(f"Files in {RAW_DIR}:")
    for fname in os.listdir(RAW_DIR):
        path = os.path.join(RAW_DIR, fname)
        log(f"  {fname}  ({os.path.getsize(path)//1024} KB)")


if __name__ == "__main__":
    main()
