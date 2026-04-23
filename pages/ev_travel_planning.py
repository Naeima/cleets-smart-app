# ============================================================
# EV TRAVEL PLANNING — SECTION C (CANONICAL IMPLEMENTATION)
# ============================================================

from __future__ import annotations

import os
import io
import json
import time
import math
import heapq
import pickle
import hashlib
import gzip
import zipfile
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
import geopandas as gpd
import numpy as np

from shapely.geometry import Point, LineString
from shapely.ops import split as shp_split

import dash
from dash import html, dcc, Input, Output, State, callback
from dash import register_page

import folium
from folium.plugins import MarkerCluster, Draw
from folium.raster_layers import WmsTileLayer
from folium.plugins import BeautifyIcon

# ---- requests retry helpers ----
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ---- optional deps ----
try:
    import osmnx as ox  # type: ignore
    HAS_OSMNX = True
except Exception:
    HAS_OSMNX = False
    ox = None  # type: ignore

try:
    import networkx as nx  # type: ignore
except Exception as e:
    raise RuntimeError("networkx is required for routing") from e

try:
    import matplotlib.cm as cm
    import matplotlib.colors as mcolors
except Exception:
    cm = None  # type: ignore
    mcolors = None  # type: ignore

try:
    import pydeck as pdk  # type: ignore
    HAS_PYDECK = True
except Exception:
    HAS_PYDECK = False
    pdk = None  # type: ignore

MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY", "").strip() or None


# ============================================================
# Dash registration
# ============================================================

register_page(
    __name__,
    path="/ev-travel-planning",
    name="EV Travel Planning",
)

# ============================================================
# Global config
# ============================================================

EV_GDRIVE_FILE_ID = "1P3smzZTMBbLzM7F49wkOJivNBbTqFd1m"

CACHE_DIR = ".cache_wfs"
os.makedirs(CACHE_DIR, exist_ok=True)

GRAPH_CACHE_DIR = os.path.join(CACHE_DIR, "graphs")
FLOOD_CACHE_DIR = os.path.join(CACHE_DIR, "flood_unions")
DATA_CACHE_DIR = os.path.join(CACHE_DIR, "data")
WFS_LAYER_CACHE_DIR = os.path.join(CACHE_DIR, "wfs_layers")

os.makedirs(GRAPH_CACHE_DIR, exist_ok=True)
os.makedirs(FLOOD_CACHE_DIR, exist_ok=True)
os.makedirs(DATA_CACHE_DIR, exist_ok=True)
os.makedirs(WFS_LAYER_CACHE_DIR, exist_ok=True)

LOGO_GDRIVE_FILE_OR_URL = (
    "https://drive.google.com/file/d/1QLQPln4dRyWXh65E5ua_rC3CGTChwKxc/view?usp=sharing"
)
LOGO_CACHE_PATH = os.path.join(CACHE_DIR, "cleets_logo-01.png")

OWS_BASE = "https://datamap.gov.wales/geoserver/ows"

# Semantic aliases used by some WMS/WFS catalogues and legacy matching code.
# NRW exposes canonical INSPIRE layer names; planning/risk classes are attributes,
# not standalone layer names. The aliases below map those semantic requests onto
# the canonical published layer(s).
OWS_LAYER_ALIASES = {
    # Planning flood zones are encoded as attributes inside the merged FMfP layer.
    "Flood Zone 2 (undefended)": ["inspire-nrw:NRW_FLOODZONE_RIVERS_SEAS_MERGED"],
    "Flood Zone 3 (undefended)": ["inspire-nrw:NRW_FLOODZONE_RIVERS_SEAS_MERGED"],
    # NRW risk layers are published separately for rivers and sea.
    "Risk of Flooding (Rivers & Sea)": [
        "inspire-nrw:NRW_FLOOD_RISK_FROM_RIVERS",
        "inspire-nrw:NRW_FLOOD_RISK_FROM_SEA",
    ],
    # Human-readable labels used inside this app.
    "FRAW – Rivers": ["inspire-nrw:NRW_FLOOD_RISK_FROM_RIVERS"],
    "FRAW – Sea": ["inspire-nrw:NRW_FLOOD_RISK_FROM_SEA"],
    "FRAW – Surface": ["inspire-nrw:NRW_FLOOD_RISK_FROM_SURFACE_WATER_SMALL_WATERCOURSES"],
    "FMfP – Rivers & Sea": ["inspire-nrw:NRW_FLOODZONE_RIVERS_SEAS_MERGED"],
    "FMfP – Surface/Small Watercourses": ["inspire-nrw:NRW_FLOODZONE_SURFACE_WATER_AND_SMALL_WATERCOURSES"],
    "Live – Warning Areas": ["inspire-nrw:NRW_FLOOD_WARNING"],
    "Live – Alert Areas": ["inspire-nrw:NRW_FLOOD_WATCH_AREAS"],
    "Historic Flood Extents": ["inspire-nrw:NRW_HISTORIC_FLOODMAP"],
}

FRAW_WMS = {
    "FRAW – Rivers": "inspire-nrw:NRW_FLOOD_RISK_FROM_RIVERS",
    "FRAW – Sea": "inspire-nrw:NRW_FLOOD_RISK_FROM_SEA",
    "FRAW – Surface": "inspire-nrw:NRW_FLOOD_RISK_FROM_SURFACE_WATER_SMALL_WATERCOURSES",
}
FMFP_WMS = {
    "FMfP – Rivers & Sea": "inspire-nrw:NRW_FLOODZONE_RIVERS_SEAS_MERGED",
    "FMfP – Surface/Small Watercourses": "inspire-nrw:NRW_FLOODZONE_SURFACE_WATER_AND_SMALL_WATERCOURSES",
}
LIVE_WMS = {
    "Live – Warning Areas": "inspire-nrw:NRW_FLOOD_WARNING",
    "Live – Alert Areas": "inspire-nrw:NRW_FLOOD_WATCH_AREAS",
}
CONTEXT_WMS = {"Historic Flood Extents": "inspire-nrw:NRW_HISTORIC_FLOODMAP"}

FRAW_WFS = {
    "FRAW Rivers": "inspire-nrw:NRW_FLOOD_RISK_FROM_RIVERS",
    "FRAW Sea": "inspire-nrw:NRW_FLOOD_RISK_FROM_SEA",
    "FRAW Surface": "inspire-nrw:NRW_FLOOD_RISK_FROM_SURFACE_WATER_SMALL_WATERCOURSES",
}
FMFP_WFS = {
    "FMfP Rivers & Sea": "inspire-nrw:NRW_FLOODZONE_RIVERS_SEAS_MERGED",
    "FMfP Surface/Small": "inspire-nrw:NRW_FLOODZONE_SURFACE_WATER_AND_SMALL_WATERCOURSES",
}
LIVE_WFS = {
    "Warnings": "inspire-nrw:NRW_FLOOD_WARNING",
    "Alerts": "inspire-nrw:NRW_FLOOD_WATCH_AREAS",
}


def _capabilities_cache_path(service: str) -> str:
    svc = str(service).strip().lower()
    return os.path.join(WFS_LAYER_CACHE_DIR, f"{svc}_capabilities.xml")


def _fetch_ows_capabilities(service: str = "WMS", ttl_days: int = 14) -> bytes:
    cache_path = _capabilities_cache_path(service)
    if os.path.exists(cache_path) and (time.time() - os.path.getmtime(cache_path)) < ttl_days * 86400:
        try:
            with open(cache_path, "rb") as f:
                return f.read()
        except Exception:
            pass

    params = {
        "service": str(service).upper(),
        "version": "1.3.0" if str(service).upper() == "WMS" else "2.0.0",
        "request": "GetCapabilities",
    }
    sess = _requests_session()
    r = sess.get(OWS_BASE, params=params, timeout=60)
    r.raise_for_status()
    raw = r.content
    try:
        with open(cache_path, "wb") as f:
            f.write(raw)
    except Exception:
        pass
    return raw


def _norm_tokens(text_: str) -> List[str]:
    s = re.sub(r"[^a-z0-9]+", " ", str(text_).lower()).strip()
    return [t for t in s.split() if t]


# Tokens that are helpful for fuzzy lookup, excluding semantic classes which are
# represented as attributes rather than standalone NRW layer names.
_LAYER_STOPWORDS = {
    "flood", "zone", "risk", "of", "from", "the", "and", "undefended",
}


def _parse_ows_layers(service: str = "WMS") -> List[Dict[str, str]]:
    try:
        raw = _fetch_ows_capabilities(service=service)
        root = ET.fromstring(raw)
    except Exception:
        return []

    layers: List[Dict[str, str]] = []
    seen: set[Tuple[str, str]] = set()
    ns_name = lambda tag: tag.rsplit("}", 1)[-1]

    for el in root.iter():
        if ns_name(el.tag) != "Layer":
            continue
        name = None
        title = None
        for ch in list(el):
            t = ns_name(ch.tag)
            if t == "Name" and ch.text:
                name = ch.text.strip()
            elif t == "Title" and ch.text:
                title = ch.text.strip()
        if not name and not title:
            continue
        key = (name or "", title or "")
        if key in seen:
            continue
        seen.add(key)
        layers.append({"name": name or "", "title": title or ""})
    return layers


def resolve_ows_layers(requested: str, service: str = "WMS") -> List[str]:
    """Resolve human/semantic layer requests onto canonical NRW published names.

    This avoids failures such as trying to discover separate layers called
    'Flood Zone 2 (undefended)' or 'Risk of Flooding (Rivers & Sea)' when NRW
    instead publishes merged INSPIRE layers whose risk/zone classes live in
    feature attributes.
    """
    req = str(requested or "").strip()
    if not req:
        return []

    alias = OWS_LAYER_ALIASES.get(req)
    if alias:
        return list(dict.fromkeys(alias))

    advertised = _parse_ows_layers(service=service)
    if not advertised:
        return [req]

    req_low = req.lower()
    exact = [x["name"] for x in advertised if x["name"].lower() == req_low]
    if exact:
        return list(dict.fromkeys(exact))

    exact_title = [x["name"] for x in advertised if x["title"].lower() == req_low and x["name"]]
    if exact_title:
        return list(dict.fromkeys(exact_title))

    req_tokens = [t for t in _norm_tokens(req) if t not in _LAYER_STOPWORDS]
    if not req_tokens:
        return [req]

    scored: List[Tuple[int, int, str]] = []
    for item in advertised:
        name = item["name"]
        title = item["title"]
        if not name:
            continue
        toks = set(_norm_tokens(name) + _norm_tokens(title))
        overlap = len([t for t in req_tokens if t in toks])
        if overlap:
            scored.append((overlap, len(toks), name))

    if scored:
        scored.sort(key=lambda x: (-x[0], x[1], x[2]))
        best_overlap = scored[0][0]
        return list(dict.fromkeys([name for ov, _n, name in scored if ov == best_overlap]))

    return [req]


SIM_DEFAULTS = dict(
    start_lat=51.4816,
    start_lon=-3.1791,
    end_lat=51.6214,
    end_lon=-3.9436,
    battery_kwh=64.0,
    init_soc=90.0,
    reserve_soc=10.0,
    target_soc=80.0,
    kwh_per_km=0.18,
    max_charger_offset_km=1.5,
    min_leg_km=20.0,
    route_buffer_m=30,
    wfs_pad_m=800,
    wfs_pad_m_fast=120,
    soc_step_normal=0.05,
    soc_step_fast=0.10,
)

FAST_MODE_DEFAULT = bool(int(os.getenv("ONS_FAST_MODE", "0")))

# RCSP knobs
SOC_STEP = 0.025
CHARGE_STEP = 0.10
DEFAULT_POWER_KW = 50.0
BASE_RISK_PENALTY_PER_KM = 60.0
EXTREME_RISK_PENALTY_PER_KM = 240.0
EXTREME_BUFFER_M = 60.0
MAX_GRAPH_BBOX_DEG = 1.0
ROUTE_BUFFER_M = 30

ZONE_COLORS = {
    "Zone 3": "#D32F2F",
    "High": "#D32F2F",
    "Zone 2": "#FFC107",
    "Medium": "#FFC107",
    "Zone 1": "#2E7D32",
    "Low": "#2E7D32",
    "Very Low": "#2E7D32",
    "Outside": "#2E7D32",
    "Unknown": "#2E7D32",
}
ZONE_PRIORITY = ["Zone 3", "High", "Zone 2", "Medium", "Zone 1", "Low", "Very Low", "Outside", "Unknown"]
_PRI = {z: i for i, z in enumerate(ZONE_PRIORITY)}

# Optimisation helper: thinning
MAX_FOLIUM_POINTS = 5000
MAX_ROUTE_POINTS = 3000
ENABLE_ROUTE_FLOOD_UNION = False  # expensive flood union for routing


# ============================================================
# Small utilities
# ============================================================

def coords_2d(coords_iter):
    """Yield (x, y) from possibly (x,y,z,...) coordinate tuples."""
    for c in coords_iter:
        yield (float(c[0]), float(c[1]))

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0088
    phi1 = math.radians(float(lat1))
    phi2 = math.radians(float(lat2))
    dphi = math.radians(float(lat2) - float(lat1))
    dl = math.radians(float(lon2) - float(lon1))
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

def value_to_hex(val: float, vmin: float, vmax: float, cmap: str = "viridis") -> str:
    if cm is None or mcolors is None:
        t = 0.0 if vmax <= vmin else (float(val) - vmin) / (vmax - vmin)
        t = max(0.0, min(1.0, t))
        r = int(255 * t)
        g = int(255 * (1.0 - t))
        b = 80
        return f"#{r:02x}{g:02x}{b:02x}"
    norm = mcolors.Normalize(vmin=vmin, vmax=vmax, clip=True)
    m = cm.get_cmap(cmap)
    rgba = m(norm(float(val)))
    return mcolors.to_hex(rgba, keep_alpha=False)

def bbox_expand(bounds_lonlat: Tuple[float, float, float, float], pad_m: float) -> Tuple[float, float, float, float]:
    """
    bounds_lonlat: (min_lon, min_lat, max_lon, max_lat)
    pad_m: meters to pad (approx conversion)
    """
    minx, miny, maxx, maxy = map(float, bounds_lonlat)
    pad_deg_lat = pad_m / 111_000.0
    mid_lat = 0.5 * (miny + maxy)
    pad_deg_lon = pad_m / (111_000.0 * max(0.2, math.cos(math.radians(mid_lat))))
    return (minx - pad_deg_lon, miny - pad_deg_lat, maxx + pad_deg_lon, maxy + pad_deg_lat)

def _bbox_for(ev_gdf: gpd.GeoDataFrame, pad_m: float = 800.0) -> Tuple[float, float, float, float]:
    if ev_gdf is None or ev_gdf.empty:
        # sensible Wales-ish bbox fallback
        return bbox_expand((-4.5, 51.0, -2.5, 52.5), pad_m)
    b = ev_gdf.total_bounds  # minx, miny, maxx, maxy
    return bbox_expand((b[0], b[1], b[2], b[3]), pad_m)

def _stable_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def _requests_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": "CLEETS-EV/1.0"})
    return s


# ============================================================
# Data loading (Google Drive tabular)
# ============================================================

def read_tabular_resilient_gdrive(
    file_id: str,
    cache_dir: str = DATA_CACHE_DIR,
    cache_name: str | None = None,
    ttl_days: int = 7,
    **kwargs,
) -> pd.DataFrame:
    """
    Download a Google Drive file and parse it as:
      - CSV (with encoding fallbacks), OR
      - Excel (.xlsx/.xls), OR
      - gzipped CSV/Excel

    Cache stores raw bytes. Parser runs on bytes.
    """
    os.makedirs(cache_dir, exist_ok=True)
    cache_name = cache_name or f"gdrive_{file_id}.bin"
    cache_path = os.path.join(cache_dir, cache_name)
    bad_path = cache_path + ".bad"

    def _looks_like_html(raw: bytes, content_type: str | None) -> bool:
        ctype = (content_type or "").lower()
        head = raw[:8192].lstrip().lower()
        return (
            "text/html" in ctype
            or head.startswith(b"<!doctype html")
            or head.startswith(b"<html")
            or b"<head" in head[:2000]
        )

    def _raise_if_drive_permission_html(raw: bytes) -> None:
        txt = raw[:200000].decode("utf-8", errors="ignore").lower()
        if any(s in txt for s in ["you need access", "request access", "sign in", "to continue to google drive"]):
            raise RuntimeError(
                "Google Drive returned an access/permission page (HTML), not the dataset. "
                "Fix sharing to 'Anyone with the link can view' or use an authenticated download."
            )

    def _download_from_drive() -> tuple[bytes, str | None]:
        sess = _requests_session()
        base = "https://drive.google.com/uc"

        r1 = sess.get(base, params={"export": "download", "id": file_id}, timeout=180)
        r1.raise_for_status()
        raw1 = r1.content
        ctype1 = r1.headers.get("Content-Type")

        if not _looks_like_html(raw1, ctype1):
            return raw1, ctype1

        _raise_if_drive_permission_html(raw1)

        token = None
        html_txt = r1.text

        m = re.search(r"confirm=([0-9a-zA-Z_]+)", html_txt)
        if m:
            token = m.group(1)
        if not token:
            m = re.search(r'name="confirm"\s+value="([^"]+)"', html_txt)
            if m:
                token = m.group(1)
        if not token:
            for k, v in r1.cookies.items():
                if k.startswith("download_warning"):
                    token = v
                    break

        if not token:
            raise RuntimeError("Google Drive returned HTML but no confirm token was found.")

        r2 = sess.get(base, params={"export": "download", "id": file_id, "confirm": token}, timeout=180)
        r2.raise_for_status()
        return r2.content, r2.headers.get("Content-Type")

    def _maybe_gunzip(raw: bytes) -> bytes:
        if raw[:2] == b"\x1f\x8b":
            return gzip.decompress(raw)
        return raw

    def _try_read_excel(raw: bytes) -> pd.DataFrame | None:
        try:
            bio = io.BytesIO(raw)
            if zipfile.is_zipfile(bio):
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    names = set(zf.namelist())
                    if "xl/workbook.xml" in names or any(n.startswith("xl/") for n in names):
                        return pd.read_excel(io.BytesIO(raw), **{k: v for k, v in kwargs.items() if k != "encoding"})
            if raw[:8] == b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1":  # legacy XLS
                return pd.read_excel(io.BytesIO(raw), **{k: v for k, v in kwargs.items() if k != "encoding"})
        except Exception:
            return None
        return None

    def _try_read_csv(raw: bytes) -> pd.DataFrame | None:
        encs = [
            kwargs.get("encoding"),
            "utf-8",
            "utf-8-sig",
            "cp1252",
            "latin1",
        ]
        seen = set()
        for enc in encs:
            if not enc or enc in seen:
                continue
            seen.add(enc)
            try:
                df_ = pd.read_csv(
                    io.BytesIO(raw),
                    encoding=enc,
                    **{k: v for k, v in kwargs.items() if k != "encoding"},
                )
                return df_
            except UnicodeDecodeError:
                continue
            except pd.errors.ParserError:
                continue
            except Exception:
                continue
        return None

    def _parse_bytes(raw: bytes, content_type: str | None) -> pd.DataFrame | None:
        raw = _maybe_gunzip(raw)
        if _looks_like_html(raw, content_type):
            _raise_if_drive_permission_html(raw)
            return None

        df_x = _try_read_excel(raw)
        if isinstance(df_x, pd.DataFrame) and not df_x.empty:
            return df_x

        df_c = _try_read_csv(raw)
        if isinstance(df_c, pd.DataFrame) and df_c.shape[1] >= 2:
            return df_c

        return None

    # ---------- cache if fresh ----------
    if os.path.exists(cache_path):
        age_ok = (time.time() - os.path.getmtime(cache_path)) < ttl_days * 86400
        if age_ok:
            try:
                with open(cache_path, "rb") as f:
                    raw_cached = f.read()
                df_cached = _parse_bytes(raw_cached, content_type=None)
                if df_cached is not None:
                    return df_cached
            except Exception:
                pass

    # ---------- download ----------
    raw, ctype = _download_from_drive()
    df = _parse_bytes(raw, ctype)
    if df is None:
        try:
            with open(bad_path, "wb") as f:
                f.write(raw)
        except Exception:
            pass
        raise RuntimeError(
            "Google Drive download could not be parsed as CSV/Excel (likely HTML permission/interstitial, "
            "or an unexpected file type). Cache was not updated. Inspect: "
            f"{bad_path}"
        )

    with open(cache_path, "wb") as f:
        f.write(raw)

    if os.path.exists(bad_path):
        try:
            os.remove(bad_path)
        except Exception:
            pass

    return df

# Back-compat alias (prevents NameError if any legacy call remains)
read_csv_resilient_gdrive = read_tabular_resilient_gdrive


# ============================================================
# WFS fetching + caching (GeoJSON)
# ============================================================

def fetch_wfs_layer_cached(
    layer_name: str,
    bbox_lonlat: Tuple[float, float, float, float],
    ttl_days: int = 14,
) -> gpd.GeoDataFrame:
    """
    Fetch a WFS layer as GeoJSON within bbox, cache on disk.
    bbox_lonlat: (min_lon, min_lat, max_lon, max_lat)
    """
    minx, miny, maxx, maxy = map(float, bbox_lonlat)
    key = _stable_hash(f"{layer_name}|{round(minx,5)},{round(miny,5)},{round(maxx,5)},{round(maxy,5)}")
    safe_layer = layer_name.replace(":", "_").replace("/", "_")
    cache_path = os.path.join(WFS_LAYER_CACHE_DIR, f"{safe_layer}_{key}.geojson")

    if os.path.exists(cache_path) and (time.time() - os.path.getmtime(cache_path)) < ttl_days * 86400:
        try:
            return gpd.read_file(cache_path)
        except Exception:
            pass

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": ",".join(resolve_ows_layers(layer_name, service="WFS")),
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
        # WFS 2.0 axis order for EPSG:4326 is often lat,lon:
        "bbox": f"{miny},{minx},{maxy},{maxx},EPSG:4326",
        "count": 50000,
    }

    sess = _requests_session()
    r = sess.get(OWS_BASE, params=params, timeout=60)
    r.raise_for_status()

    with open(cache_path, "wb") as f:
        f.write(r.content)

    try:
        return gpd.read_file(cache_path)
    except Exception:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")


# ============================================================
# Data preparation (SAFE at import time for Dash Pages)
# ============================================================

EV_DATA_ERROR: Optional[str] = None

def _load_ev_dataframe() -> pd.DataFrame:
    global EV_DATA_ERROR
    try:
        EV_DATA_ERROR = None
        return read_tabular_resilient_gdrive(
            EV_GDRIVE_FILE_ID,
            cache_dir=DATA_CACHE_DIR,
            ttl_days=7,
        )
    except Exception as e:
        EV_DATA_ERROR = str(e)
        return pd.DataFrame()

df = _load_ev_dataframe()

if df is None or df.empty:
    gdf_ev = gpd.GeoDataFrame(
        {
            "Latitude": pd.Series(dtype=float),
            "Longitude": pd.Series(dtype=float),
            "country": pd.Series(dtype=str),
            "AvailabilityLabel": pd.Series(dtype=str),
            "Operator": pd.Series(dtype=str),
            "Postcode": pd.Series(dtype=str),
            "dateCreated": pd.Series(dtype="datetime64[ns]"),
            "ROW_ID": pd.Series(dtype=int),
        },
        geometry=gpd.GeoSeries([], crs="EPSG:4326"),
        crs="EPSG:4326",
    )
    country_OPTIONS: List[str] = []
else:
    df = df.copy()

    area_col = (
        "country"
        if "country" in df.columns
        else ("adminArea" if "adminArea" in df.columns else "town")
    )
    df[area_col] = df[area_col].astype(str).str.strip().str.title()

    lat_src = df["latitude"] if "latitude" in df.columns else df.get("Latitude")
    lon_src = df["longitude"] if "longitude" in df.columns else df.get("Longitude")
    df = df.assign(
        Latitude=pd.to_numeric(lat_src, errors="coerce"),
        Longitude=pd.to_numeric(lon_src, errors="coerce"),
    )
    df = df.dropna(subset=["Latitude", "Longitude"]).copy()

    df["country"] = df[area_col].astype(str)

    def classify_availability(s):
        s = str(s).lower().strip()
        if any(k in s for k in ["available", "in service", "operational", "working", "ok", "service"]):
            return True
        if any(k in s for k in ["not operational", "fault", "out of service", "offline", "unavailable", "down"]):
            return False
        return None

    _df_status = df.get("chargeDeviceStatus", pd.Series(index=df.index))
    df["Available"] = _df_status.apply(classify_availability)
    df["AvailabilityLabel"] = df["Available"].map({True: "Operational", False: "Not operational"}).fillna("Unknown")
    df["Operator"] = df.get("deviceControllerName", df.get("Operator", "Unknown"))
    df["Postcode"] = df.get("postcode", df.get("Postcode", "N/A"))
    df["dateCreated"] = pd.to_datetime(df.get("dateCreated", df.get("DateCreated")), errors="coerce", dayfirst=True)

    df["geometry"] = [Point(xy) for xy in zip(df["Longitude"], df["Latitude"])]
    gdf_ev = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326").reset_index(drop=True)
    gdf_ev["ROW_ID"] = gdf_ev.index.astype(int)

    country_OPTIONS = sorted([t for t in gdf_ev["country"].dropna().astype(str).unique() if t])


# ============================================================
# Routing helpers (OSRM + flood union)
# ============================================================

def _requests_session_osrm() -> requests.Session:
    return _requests_session()

def _osrm_try(base_url, sl, so, el, eo, want_steps=True):
    url = f"{base_url}/route/v1/driving/{so},{sl};{eo},{el}"
    params = {
        "overview": "full",
        "geometries": "geojson",
        "alternatives": "false",
        "steps": "true" if want_steps else "false",
    }
    sess = _requests_session_osrm()
    r = sess.get(url, params=params, timeout=20)
    r.raise_for_status()
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "json" not in ctype:
        raise RuntimeError(f"Unexpected OSRM content type: {ctype}")
    data = r.json()
    if not data.get("routes"):
        raise RuntimeError("No routes")
    rt = data["routes"][0]
    coords = rt["geometry"]["coordinates"]
    ln = LineString([(c[0], c[1]) for c in coords])
    dist_m = float(rt["distance"])
    dur_s = float(rt["duration"])
    steps = []
    if want_steps:
        for leg in rt.get("legs", []):
            for st in leg.get("steps", []):
                nm = st.get("name") or ""
                m = st.get("maneuver", {})
                kind = m.get("modifier") or m.get("type") or ""
                t = " ".join([w for w in [kind, nm] if w]).strip()
                if t:
                    steps.append(t)
    return ln, dist_m, dur_s, steps

def osrm_route(sl, so, el, eo):
    for base in ("https://router.project-osrm.org", "https://routing.openstreetmap.de/routed-car"):
        try:
            ln, d, t, steps = _osrm_try(base, sl, so, el, eo, want_steps=True)
            return ln, d, t, steps, base
        except Exception:
            continue
    raise RuntimeError("OSRM routing failed on both endpoints")

def get_flood_union(
    bounds_lonlat: Tuple[float, float, float, float],
    include_live=True,
    include_fraw=True,
    include_fmfp=True,
    pad_m: float = SIM_DEFAULTS["wfs_pad_m"],
):
    """
    bounds_lonlat: (min_lon, min_lat, max_lon, max_lat)
    returns shapely geometry in EPSG:27700 (union), or None.
    """
    bbox = bbox_expand(bounds_lonlat, pad_m)
    chunks = []

    if include_fmfp:
        for lyr in FMFP_WFS.values():
            g = fetch_wfs_layer_cached(lyr, bbox)
            if not g.empty:
                chunks.append(g[["geometry"]])

    if include_fraw:
        for lyr in FRAW_WFS.values():
            g = fetch_wfs_layer_cached(lyr, bbox)
            if not g.empty:
                chunks.append(g[["geometry"]])

    if include_live:
        for lyr in LIVE_WFS.values():
            g = fetch_wfs_layer_cached(lyr, bbox)
            if not g.empty:
                chunks.append(g[["geometry"]])

    if not chunks:
        return None

    G = gpd.GeoDataFrame(pd.concat(chunks, ignore_index=True), geometry="geometry", crs="EPSG:4326")
    try:
        G["geometry"] = G["geometry"].buffer(0)
    except Exception:
        pass
    try:
        G = G.explode(index_parts=False).reset_index(drop=True)
    except Exception:
        pass

    try:
        return G.to_crs("EPSG:27700").union_all()
    except Exception:
        return G.to_crs("EPSG:27700").unary_union


# ============================================================
# OSMnx graph caching
# ============================================================

def _graph_point_cache_path(lat, lon, dist_m):
    return os.path.join(GRAPH_CACHE_DIR, f"pt_{round(lat,5)}_{round(lon,5)}_{int(dist_m)}.graphml")

def _graph_bbox_cache_path(north, south, east, west):
    key = f"bbox_{round(north,5)}_{round(south,5)}_{round(east,5)}_{round(west,5)}.graphml"
    return os.path.join(GRAPH_CACHE_DIR, key)

def _processed_graph_cache_path(start_lat, start_lon, end_lat, end_lon, mode_tag: str) -> str:
    key = (
        f"proc_{mode_tag}_"
        f"{round(float(start_lat), 3)}_{round(float(start_lon), 3)}_"
        f"{round(float(end_lat), 3)}_{round(float(end_lon), 3)}.pkl"
    )
    return os.path.join(GRAPH_CACHE_DIR, key)

def _ox_save_graphml(G, path):
    if not HAS_OSMNX:
        return None
    try:
        return ox.io.save_graphml(G, path)
    except Exception:
        try:
            return ox.save_graphml(G, path)
        except Exception:
            return None

def _ox_load_graphml(path):
    if not HAS_OSMNX:
        return None
    try:
        return ox.io.load_graphml(path)
    except Exception:
        try:
            return ox.load_graphml(path)
        except Exception:
            return None

def graph_from_point_cached(lat, lon, dist_m=15000, ttl_days=30):
    if not HAS_OSMNX:
        raise RuntimeError("OSMnx not installed")
    path = _graph_point_cache_path(lat, lon, dist_m)
    if os.path.exists(path) and (time.time() - os.path.getmtime(path)) < ttl_days * 86400:
        G = _ox_load_graphml(path)
        if G is not None:
            return G
    G = ox.graph_from_point((lat, lon), dist=dist_m, network_type="drive", simplify=True)
    try:
        _ox_save_graphml(G, path)
    except Exception:
        pass
    return G

def graph_from_bbox_cached(north, south, east, west, ttl_days=30):
    if not HAS_OSMNX:
        raise RuntimeError("OSMnx not installed")
    path = _graph_bbox_cache_path(north, south, east, west)
    if os.path.exists(path) and (time.time() - os.path.getmtime(path)) < ttl_days * 86400:
        G = _ox_load_graphml(path)
        if G is not None:
            return G

    try:
        G = ox.graph_from_bbox(north=north, south=south, east=east, west=west, network_type="drive", simplify=True)
    except TypeError:
        try:
            G = ox.graph_from_bbox((north, south, east, west), network_type="drive", simplify=True)
        except TypeError:
            G = ox.graph_from_bbox(north, south, east, west, "drive", True)

    try:
        _ox_save_graphml(G, path)
    except Exception:
        pass
    return G

def _build_graph_bbox(north, south, east, west):
    return graph_from_bbox_cached(north, south, east, west)

def _graph_two_points(sl, so, el, eo, dist_m=15000):
    G1 = graph_from_point_cached(sl, so, dist_m)
    G2 = graph_from_point_cached(el, eo, dist_m)
    try:
        return nx.compose(G1, G2)
    except Exception:
        G = nx.MultiDiGraph()
        G.update(G1)
        G.update(G2)
        return G


# ============================================================
# Risk segmentation
# ============================================================

def segment_route_by_risk(line_wgs84, risk_union_metric, buffer_m=ROUTE_BUFFER_M):
    if risk_union_metric is None:
        return [line_wgs84], []

    try:
        line_m = gpd.GeoSeries([line_wgs84], crs="EPSG:4326").to_crs("EPSG:27700").iloc[0]
        crs_m = "EPSG:27700"
    except Exception:
        line_m = gpd.GeoSeries([line_wgs84], crs="EPSG:4326").to_crs("EPSG:3857").iloc[0]
        crs_m = "EPSG:3857"

    hit = risk_union_metric.buffer(buffer_m)
    try:
        pieces = list(shp_split(line_m, hit.boundary))
    except Exception:
        pieces = [line_m]

    safe_m, risk_m = [], []
    for seg in pieces:
        (risk_m if seg.intersects(hit) else safe_m).append(seg)

    safe = gpd.GeoSeries(safe_m, crs=crs_m).to_crs("EPSG:4326").tolist() if safe_m else []
    risk = gpd.GeoSeries(risk_m, crs=crs_m).to_crs("EPSG:4326").tolist() if risk_m else []
    return safe, risk


# ============================================================
# RCSP optimiser (fixed + de-duplicated)
# ============================================================

def _soc_to_frac(x: float) -> float:
    x = float(x)
    return x / 100.0 if x > 1.0 else x

def rcsp_optimize(
    start_lat: float,
    start_lon: float,
    end_lat: float,
    end_lon: float,
    battery_kwh: float,
    init_soc: float,
    reserve_soc: float,
    target_soc: float,
    kwh_per_km: float,
    chargers_df: pd.DataFrame,
    flood_union_m,
    extreme: bool = False,
    risk_penalty_per_km: Optional[float] = None,
    max_seconds: float = 10.0,
    soc_step: Optional[float] = None,
):
    if not HAS_OSMNX:
        raise RuntimeError("OSMnx not installed")

    total_t0 = time.time()

    # --- graph bbox ---
    minlat, maxlat = sorted([float(start_lat), float(end_lat)])
    minlon, maxlon = sorted([float(start_lon), float(end_lon)])

    south0, north0 = minlat - 0.05, maxlat + 0.05
    west0, east0 = minlon - 0.05, maxlon + 0.05
    diag_km = haversine_km(south0, west0, north0, east0)

    pad = max(0.05, diag_km / 110.0)
    south, north = minlat - pad, maxlat + pad
    west, east = minlon - pad, maxlon + pad

    use_point_graph = (east - west) > MAX_GRAPH_BBOX_DEG or (north - south) > MAX_GRAPH_BBOX_DEG or diag_km > 30.0
    mode_tag = "two_points" if use_point_graph else "bbox"
    proc_path = _processed_graph_cache_path(start_lat, start_lon, end_lat, end_lon, mode_tag)

    G = None
    edges = None
    edges_m = None

    if os.path.exists(proc_path):
        try:
            with open(proc_path, "rb") as f:
                cached = pickle.load(f)
            G = cached.get("G")
            edges = cached.get("edges")
            edges_m = cached.get("edges_m")
        except Exception:
            G = None
            edges = None
            edges_m = None

    if G is None or edges is None or edges_m is None:
        stage_t0 = time.time()
        if use_point_graph:
            G = _graph_two_points(start_lat, start_lon, end_lat, end_lon, dist_m=30000)
        else:
            G = _build_graph_bbox(north, south, east, west)

        if time.time() - total_t0 > 20:
            raise TimeoutError("Graph build too slow")

        G = ox.add_edge_speeds(G)
        G = ox.add_edge_travel_times(G)

        Gm = ox.project_graph(G, to_crs="EPSG:27700")
        edges_m = ox.graph_to_gdfs(Gm, nodes=False, edges=True, fill_edge_geometry=True)
        edges = ox.graph_to_gdfs(G, nodes=False, edges=True, fill_edge_geometry=True)

        try:
            with open(proc_path, "wb") as f:
                pickle.dump({"G": G, "edges": edges, "edges_m": edges_m}, f, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            pass

        print(f"[rcsp_optimize] graph prep: {time.time() - stage_t0:.2f}s (diag_km={diag_km:.1f}, mode={mode_tag})")
    else:
        print(f"[rcsp_optimize] graph prep: cache hit (diag_km={diag_km:.1f}, mode={mode_tag})")

    if time.time() - total_t0 > 25:
        raise TimeoutError("Route optimisation exceeded callback budget")

    # --- risk tag ---
    if flood_union_m is not None:
        buf = EXTREME_BUFFER_M if extreme else 0.0
        edges_m["risk"] = edges_m.geometry.intersects(flood_union_m.buffer(buf))
    else:
        edges_m["risk"] = False

    edges_m["length_m"] = edges_m.geometry.length.astype(float)
    edges_join = edges.join(edges_m[["risk", "length_m"]])

    # --- edge lookup ---
    edges_lookup: Dict[Tuple[Any, Any, Any], Tuple[float, float, bool, Any]] = {}
    for (u, v, k), row in edges_join.iterrows():
        L = float(row.get("length_m", 0.0))
        if not math.isfinite(L) or L <= 0:
            L = float(row.get("length", 0.0)) * 111_000.0
        T = float(row.get("travel_time", L / 13.9))
        R = bool(row.get("risk", False))
        geom = row.get("geometry", None)
        edges_lookup[(u, v, k)] = (L, T, R, geom)

    if not edges_lookup:
        raise RuntimeError("edges_lookup is empty: graph has no usable edges")

    adj: Dict[Any, List[Tuple[Any, Any, float, float, bool, Any]]] = {}
    for (u, v, k), (L, T, R, geom) in edges_lookup.items():
        adj.setdefault(u, []).append((v, k, L, T, R, geom))

    # --- flood penalty scaling (robust) ---
    L_pos = [L for (L, _, _, _) in edges_lookup.values() if L > 0 and math.isfinite(L)]
    d_min = min(L_pos) if L_pos else 1.0
    T_max = sum(T for (_, T, R, _) in edges_lookup.values() if not R)
    lambda_flood = (T_max + 1.0) / d_min

    # --- nearest nodes ---
    try:
        nn = ox.nearest_nodes
    except AttributeError:
        from osmnx.distance import nearest_nodes as nn  # type: ignore

    u0 = nn(G, float(start_lon), float(start_lat))
    v0 = nn(G, float(end_lon), float(end_lat))

    # --- chargers on graph nodes ---
    chargers: Dict[Any, Dict[str, Any]] = {}
    if isinstance(chargers_df, (pd.DataFrame, gpd.GeoDataFrame)) and not chargers_df.empty:
        for _, r in chargers_df.iterrows():
            try:
                nid = nn(G, float(r["Longitude"]), float(r["Latitude"]))
                p_kw = r.get("power_kW", DEFAULT_POWER_KW)
                p_kw = float(p_kw) if pd.notna(p_kw) and float(p_kw) > 0 else DEFAULT_POWER_KW
                chargers[nid] = {
                    "ROW_ID": int(r["ROW_ID"]),
                    "power_kW": p_kw,
                    "operational": str(r.get("AvailabilityLabel", "")) == "Operational",
                }
            except Exception:
                continue

    # --- SoC discretisation ---
    step = float(soc_step or SOC_STEP)
    Q = [round(i * step, 4) for i in range(0, int(1 / step) + 1)]

    def q_to_idx(q: float) -> int:
        return max(0, min(len(Q) - 1, int(round(q / step))))

    init_q = _soc_to_frac(init_soc)
    reserve_q = _soc_to_frac(reserve_soc)
    tgt_q = _soc_to_frac(target_soc)

    if not (0.0 <= reserve_q <= init_q <= 1.0):
        raise ValueError(f"Invalid SoC bounds: reserve={reserve_q}, init={init_q}")

    # (kept for compatibility if you later want to use it explicitly)
    _ = float(
        risk_penalty_per_km
        if risk_penalty_per_km is not None
        else (EXTREME_RISK_PENALTY_PER_KM if extreme else BASE_RISK_PENALTY_PER_KM)
    )

    # --- RCSP label-setting ---
    INF = 1e18
    best: Dict[Tuple[Any, int], float] = {}
    pred: Dict[Tuple[Any, int], Tuple[Any, int, str, Dict[str, Any]]] = {}
    pq: List[Tuple[float, Any, int]] = []

    start_key = (u0, q_to_idx(init_q))
    best[start_key] = 0.0
    heapq.heappush(pq, (0.0, u0, start_key[1]))

    goal: Optional[Tuple[Any, int]] = None
    t0 = time.time()

    while pq:
        if time.time() - t0 > max_seconds:
            raise TimeoutError("RCSP time limit exceeded")

        cost, node, qi = heapq.heappop(pq)
        if best.get((node, qi), INF) < cost:
            continue

        # require reaching target AND reserve
        if node == v0 and Q[qi] >= max(reserve_q, tgt_q):
            goal = (node, qi)
            break

        # drive transitions
        for v, k, L, T, R, geom in adj.get(node, []):
            dq = (L / 1000.0) * float(kwh_per_km) / float(battery_kwh)
            if Q[qi] - dq < reserve_q:
                continue

            qj = q_to_idx(Q[qi] - dq)
            flood_penalty = (lambda_flood * L) if R else 0.0
            c2 = cost + T + flood_penalty

            if c2 < best.get((v, qj), INF):
                best[(v, qj)] = c2
                pred[(v, qj)] = (node, qi, "drive", {"v": v, "k": k})
                heapq.heappush(pq, (c2, v, qj))

        # charge transitions
        ch = chargers.get(node)
        if ch and ch["operational"]:
            p_kw = float(ch["power_kW"])
            for dq in (CHARGE_STEP, 2 * CHARGE_STEP, 3 * CHARGE_STEP):
                qn = min(1.0, Q[qi] + dq)
                if qn <= Q[qi] + 1e-9:
                    continue
                dt = 3600.0 * float(battery_kwh) * (qn - Q[qi]) / max(1e-6, p_kw)
                k2 = (node, q_to_idx(qn))
                c2 = cost + dt
                if c2 < best.get(k2, INF):
                    best[k2] = c2
                    pred[k2] = (node, qi, "charge", {"dt": dt})
                    heapq.heappush(pq, (c2, node, k2[1]))

    if goal is None:
        raise RuntimeError(
            "No feasible RCSP solution (try lowering reserve/target SoC, increasing bbox pad, or using Light mode)."
        )

    # --- reconstruct stops (forward order) + geometry ---
    stops: List[Dict[str, Any]] = []
    coords: List[Tuple[float, float]] = []

    state = goal
    drive_edges: List[Tuple[Any, Any, Any]] = []
    while state in pred:
        prev_node, prev_qi, action, meta = pred[state]
        if action == "charge" and prev_node in chargers:
            ch = chargers[prev_node]
            stops.append({
                "ROW_ID": ch["ROW_ID"],
                "soc_before": Q[prev_qi],
                "soc_after": Q[state[1]],
                "energy_kWh": float(battery_kwh) * (Q[state[1]] - Q[prev_qi]),
                "charge_time_min": float(meta["dt"]) / 60.0,
            })
        elif action == "drive":
            drive_edges.append((prev_node, meta["v"], meta["k"]))
        state = (prev_node, prev_qi)

    stops.reverse()
    drive_edges.reverse()

    for (u, v, k) in drive_edges:
        _, _, _, geom = edges_lookup.get((u, v, k), (None, None, None, None))
        if geom is not None and hasattr(geom, "coords"):
            seg = list(coords_2d(geom.coords))
        else:
            seg = [(G.nodes[u]["x"], G.nodes[u]["y"]), (G.nodes[v]["x"], G.nodes[v]["y"])]

        if coords and seg and seg[0] == coords[-1]:
            seg = seg[1:]
        coords.extend(seg)

    if len(coords) < 2:
        raise RuntimeError("Failed to reconstruct route geometry")

    line = LineString(coords)
    safe_lines, risk_lines = segment_route_by_risk(line, flood_union_m, buffer_m=ROUTE_BUFFER_M)
    total_cost = float(best.get(goal, float("inf")))
    print(f"[rcsp_optimize] total: {time.time() - total_t0:.2f}s")

    return line, safe_lines, risk_lines, stops, total_cost


# ============================================================
# Zones
# ============================================================

def _norm_zone(props: dict, layer_name: str) -> str:
    txt = " ".join([str(v) for v in props.values() if v is not None]).lower()
    if "zone 3" in txt:
        return "Zone 3"
    if "zone 2" in txt:
        return "Zone 2"
    if "zone 1" in txt:
        return "Zone 1"
    if "very low" in txt:
        return "Very Low"
    if "high" in txt:
        return "High"
    if "medium" in txt:
        return "Medium"
    if "low" in txt:
        return "Low"
    return "Unknown"

def fetch_model_zones_gdf(ev_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    bbox = _bbox_for(ev_gdf, pad_m=float(SIM_DEFAULTS.get("wfs_pad_m", 800)))
    chunks = []
    for title, layer in {**FMFP_WFS, **FRAW_WFS}.items():
        g = fetch_wfs_layer_cached(layer, bbox)
        if g.empty:
            continue
        props_df = g.drop(columns=["geometry"], errors="ignore")
        zlabs = [_norm_zone(r.to_dict(), title) for _, r in props_df.iterrows()]
        g = g.assign(
            zone=zlabs,
            color=[ZONE_COLORS.get(z, "#2E7D32") for z in zlabs],
            model=title,
        )
        try:
            g["geometry"] = g["geometry"].buffer(0)
        except Exception:
            pass
        try:
            g = g.explode(index_parts=False).reset_index(drop=True)
        except Exception:
            pass
        chunks.append(g[["zone", "color", "model", "geometry"]])

    if not chunks:
        return gpd.GeoDataFrame(columns=["zone", "color", "model", "geometry"], geometry="geometry", crs="EPSG:4326")

    G = pd.concat(chunks, ignore_index=True)
    return gpd.GeoDataFrame(G, geometry="geometry", crs="EPSG:4326")

def compute_model_zones_for_points(ev_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    zones = fetch_model_zones_gdf(ev_gdf)
    out = ev_gdf[["ROW_ID"]].copy() if ev_gdf is not None and not ev_gdf.empty else pd.DataFrame({"ROW_ID": []})
    out["ZoneLabel"] = "Outside"
    out["ZoneColor"] = ZONE_COLORS["Outside"]

    if zones.empty or ev_gdf is None or ev_gdf.empty:
        return out[["ROW_ID", "ZoneLabel", "ZoneColor"]]

    try:
        ev_m = ev_gdf.to_crs("EPSG:27700")
        zn_m = zones.to_crs("EPSG:27700")
    except Exception:
        ev_m = ev_gdf.to_crs("EPSG:3857")
        zn_m = zones.to_crs("EPSG:3857")

    try:
        joined = gpd.sjoin(ev_m[["ROW_ID", "geometry"]], zn_m, how="left", predicate="within")
    except Exception:
        joined = gpd.sjoin(ev_m[["ROW_ID", "geometry"]], zn_m, how="left", predicate="intersects")

    if joined.empty:
        return out[["ROW_ID", "ZoneLabel", "ZoneColor"]]

    joined["pri"] = joined["zone"].map(_PRI).fillna(_PRI["Unknown"])
    idx = joined.sort_values(["ROW_ID", "pri"]).groupby("ROW_ID", as_index=False).first()
    lut = idx.set_index("ROW_ID")

    out.loc[out["ROW_ID"].isin(lut.index), "ZoneLabel"] = lut["zone"]
    out.loc[out["ROW_ID"].isin(lut.index), "ZoneColor"] = lut["zone"].map(ZONE_COLORS).fillna(ZONE_COLORS["Outside"])
    return out[["ROW_ID", "ZoneLabel", "ZoneColor"]]

def safe_compute_zones() -> pd.DataFrame:
    try:
        return compute_model_zones_for_points(gdf_ev)
    except Exception:
        return pd.DataFrame({"ROW_ID": gdf_ev.get("ROW_ID", pd.Series(dtype=int)), "ZoneLabel": "Outside", "ZoneColor": ZONE_COLORS["Outside"]})

def preload_zones_json() -> str:
    z = safe_compute_zones()
    return z.to_json(orient="records")


# ============================================================
# Folium helpers / renderers
# ============================================================

def add_wms_group(fmap, title_to_layer: dict, visible=True, opacity=0.55):
    for title, layer in title_to_layer.items():
        try:
            resolved_layers = resolve_ows_layers(layer, service="WMS") if isinstance(layer, str) else [str(layer)]
            if not resolved_layers:
                continue
            WmsTileLayer(
                url=OWS_BASE,
                layers=",".join(resolved_layers),
                name=f"{title} (WMS)",
                fmt="image/png",
                transparent=True,
                opacity=opacity,
                version="1.3.0",
                show=visible,
            ).add_to(fmap)
        except Exception:
            pass



def add_fmfp_blue_wfs_group(fmap, bbox_lonlat, visible=True):
    """Render FMfP flood-zone polygons client-side in fixed blue water-style colours."""
    layer_styles = {
        "FMfP – Rivers & Sea": {
            "fillColor": "#4FC3F7",
            "color": "#1E88E5",
            "weight": 1,
            "fillOpacity": 0.35,
        },
        "FMfP – Surface/Small Watercourses": {
            "fillColor": "#81D4FA",
            "color": "#29B6F6",
            "weight": 1,
            "fillOpacity": 0.30,
        },
    }
    for title, layer in FMFP_WFS.items():
        try:
            resolved = resolve_ows_layer(layer, service_type="WFS")
            g = fetch_wfs_layer_cached(resolved, bbox_lonlat)
            if g is None or g.empty:
                continue
            style = layer_styles.get(title, layer_styles["FMfP – Rivers & Sea"])
            folium.GeoJson(
                data=g.__geo_interface__,
                name=f"{title} (blue)",
                show=visible,
                style_function=lambda _feature, style=style: style,
                highlight_function=lambda _feature: {
                    "weight": max(2, int(style.get("weight", 1)) + 1),
                    "fillOpacity": min(0.6, float(style.get("fillOpacity", 0.35)) + 0.1),
                    "color": style.get("color", "#1E88E5"),
                    "fillColor": style.get("fillColor", "#4FC3F7"),
                },
            ).add_to(fmap)
        except Exception:
            continue

def add_base_tiles(m):
    folium.TileLayer(
        tiles="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        name="OpenStreetMap",
        attr="© OpenStreetMap contributors",
        control=True,
        overlay=False,
        max_zoom=19,
    ).add_to(m)
    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
        name="CartoDB Positron",
        attr="© OpenStreetMap contributors, © CARTO",
        control=True,
        overlay=False,
        max_zoom=19,
    ).add_to(m)
    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        name="CartoDB Dark Matter",
        attr="© OpenStreetMap contributors, © CARTO",
        control=True,
        overlay=False,
        max_zoom=19,
    ).add_to(m)
    folium.TileLayer(
        tiles="https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png",
        name="OSM Humanitarian",
        attr="© OpenStreetMap contributors, Tiles courtesy of HOT",
        control=True,
        overlay=False,
        max_zoom=19,
    ).add_to(m)
    folium.TileLayer(
        tiles=(
            "https://server.arcgisonline.com/ArcGIS/rest/services/"
            "World_Imagery/MapServer/tile/{z}/{y}/{x}"
        ),
        name="Esri WorldImagery",
        attr="Tiles © Esri & contributors",
        control=True,
        overlay=False,
        max_zoom=19,
    ).add_to(m)
    folium.TileLayer(
        tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
        name="OpenTopoMap",
        attr="© OpenStreetMap contributors, SRTM; style © OpenTopoMap (CC-BY-SA)",
        control=True,
        overlay=False,
        max_zoom=17,
    ).add_to(m)

def make_beautify_icon(color_hex: str):
    border_map = {"#D32F2F": "#B71C1C", "#FFC107": "#FF8F00", "#2E7D32": "#1B5E20"}
    border = border_map.get(color_hex, "#1B5E20")
    return BeautifyIcon(
        icon="bolt",
        icon_shape="marker",
        background_color=color_hex,
        border_color=border,
        border_width=3,
        text_color="white",
        inner_icon_style="font-size:22px;padding-top:2px;",
    )

def _row_to_tooltip_html(row, title=None):
    s = f"<b>{title or 'Data Point'}</b><br>"
    for k, v in row.items():
        if pd.isna(v) or v is None or str(v).strip() == "":
            continue
        s += f"<b>{k}:</b> {v}<br>"
    return s

def _thin_for_folium(df_: pd.DataFrame, max_points: int = MAX_FOLIUM_POINTS, zone_col: str = "ZoneLabel") -> pd.DataFrame:
    if df_ is None or df_.empty or len(df_) <= max_points:
        return df_
    df_ = df_.copy()
    if zone_col not in df_.columns:
        df_[zone_col] = "Outside"
    out = []
    per_zone = max(1, max_points // max(1, df_[zone_col].nunique()))
    for _, grp in df_.groupby(zone_col):
        out.append(grp if len(grp) <= per_zone else grp.sample(n=per_zone, random_state=42))
    thinned = pd.concat(out, ignore_index=True)
    if len(thinned) > max_points:
        thinned = thinned.sample(n=max_points, random_state=42)
    return thinned

def add_heat_overlay(m, heat_data, vmin=5, vmax=25, opacity=0.55):
    if not heat_data:
        return
    fg = folium.FeatureGroup(name="Heat (UKCP tas)", show=True)
    lon = np.array(heat_data["lon"])
    lat = np.array(heat_data["lat"])
    z = np.array(heat_data["z"])
    for i in range(z.shape[0]):
        for j in range(z.shape[1]):
            val = z[i, j]
            if not np.isfinite(val):
                continue
            folium.CircleMarker(
                location=[float(lat[i, j]), float(lon[i, j])],
                radius=5,
                fill=True,
                fill_color=value_to_hex(float(val), vmin, vmax),
                fill_opacity=opacity,
                color=None,
            ).add_to(fg)
    fg.add_to(m)

def render_map_html_ev(
    df_map,
    show_fraw: bool,
    show_fmfp: bool,
    show_live: bool,
    show_ctx: bool,
    light: bool = False,
    heat_data=None,
):
    if isinstance(df_map, gpd.GeoDataFrame):
        df_plot = pd.DataFrame(df_map.drop(columns=["geometry"], errors="ignore"))
    else:
        df_plot = pd.DataFrame(df_map)

    df_plot = _thin_for_folium(df_plot, max_points=MAX_FOLIUM_POINTS, zone_col="ZoneLabel")

    m = folium.Map(location=[51.6, -3.2], zoom_start=9, tiles=None, control_scale=True)
    add_base_tiles(m)

    red_group = folium.FeatureGroup(name="Chargers: Zone 3 / High (red)", show=True).add_to(m)
    amber_group = folium.FeatureGroup(name="Chargers: Zone 2 / Medium (amber)", show=True).add_to(m)
    green_group = folium.FeatureGroup(name="Chargers: Zone 1 / Low–Outside (green)", show=True).add_to(m)

    red_cluster = MarkerCluster(name="Cluster: Zone 3 / High").add_to(red_group)
    amber_cluster = MarkerCluster(name="Cluster: Zone 2 / Medium").add_to(amber_group)
    green_cluster = MarkerCluster(name="Cluster: Zone 1 / Low–Outside").add_to(green_group)

    Draw(
        export=False,
        position="topleft",
        draw_options={
            "polygon": {"allowIntersection": False, "showArea": True},
            "rectangle": True,
            "polyline": False,
            "circle": False,
            "circlemarker": False,
            "marker": False,
        },
        edit_options={"edit": True},
    ).add_to(m)

    for _, row in df_plot.iterrows():
        zlabel = (row.get("ZoneLabel") or "Outside")
        if zlabel in ("Zone 3", "High"):
            color_hex = ZONE_COLORS["Zone 3"]
            group_cluster = red_cluster
        elif zlabel in ("Zone 2", "Medium"):
            color_hex = ZONE_COLORS["Zone 2"]
            group_cluster = amber_cluster
        else:
            color_hex = ZONE_COLORS["Outside"]
            group_cluster = green_cluster

        lat = row.get("Latitude")
        lon = row.get("Longitude")
        if pd.isna(lat) or pd.isna(lon):
            continue

        title = f"{row.get('Operator','')} ({row.get('country','')})"
        try:
            tooltip_html = _row_to_tooltip_html(row, title=title)
            tooltip_obj = folium.Tooltip(tooltip_html, sticky=True)
        except Exception:
            tooltip_obj = title

        group_cluster.add_child(
            folium.Marker([float(lat), float(lon)], tooltip=tooltip_obj, icon=make_beautify_icon(color_hex))
        )

    if show_fraw:
        add_wms_group(m, FRAW_WMS, True, 0.50)
    if show_fmfp:
        add_fmfp_blue_wfs_group(m, _bbox_for(gdf_ev if gdf_ev is not None and not gdf_ev.empty else None), visible=True)
    if show_ctx:
        add_wms_group(m, CONTEXT_WMS, False, 0.45)
    if show_live:
        add_wms_group(m, LIVE_WMS, True, 0.65)

    if heat_data:
        add_heat_overlay(m, heat_data, vmin=5, vmax=25, opacity=0.55)

    folium.LayerControl(collapsed=True).add_to(m)

    legend_html = (
        f"<div style='position: fixed; bottom:20px; left:20px; z-index:9999; "
        f"background:white; padding:10px 12px; border:1px solid #ccc; border-radius:6px; font-size:13px;'>"
        f"<b>Chargers by Flood Model Zone</b>"
        f"<div style='margin-top:6px'><span style='display:inline-block;width:12px;height:12px;"
        f"background:{ZONE_COLORS['Zone 3']};margin-right:6px;border:1px solid #555;'></span> Zone 3 / High</div>"
        f"<div><span style='display:inline-block;width:12px;height:12px;"
        f"background:{ZONE_COLORS['Zone 2']};margin-right:6px;border:1px solid #555;'></span> Zone 2 / Medium</div>"
        f"<div><span style='display:inline-block;width:12px;height:12px;"
        f"background:{ZONE_COLORS['Outside']};margin-right:6px;border:1px solid #555;'></span> "
        f"Zone 1 / Low–Very Low / Outside</div>"
        f"</div>"
    )
    m.get_root().html.add_child(folium.Element(legend_html))
    return m.get_root().render()

def render_map_html_route(
    full_line: LineString,
    route_safe: List[LineString],
    route_risk: List[LineString],
    start: Tuple[float, float],
    end: Tuple[float, float],
    chargers: List[Dict[str, Any]],
    all_chargers_df: Optional[pd.DataFrame] = None,
    animate: bool = True,
    speed_kmh: float = 50,
    show_fraw: bool = True,
    show_fmfp: bool = True,
    show_live: bool = True,
    show_ctx: bool = True,
):
    m = folium.Map(
        location=[(start[0] + end[0]) / 2, (start[1] + end[1]) / 2],
        zoom_start=11,
        tiles=None,
        control_scale=True,
    )
    add_base_tiles(m)

    if all_chargers_df is not None and not all_chargers_df.empty:
        df_all = _thin_for_folium(pd.DataFrame(all_chargers_df), max_points=MAX_ROUTE_POINTS, zone_col="ZoneLabel")

        red_group = folium.FeatureGroup(name="All Chargers: Zone 3 / High (red)", show=True).add_to(m)
        amber_group = folium.FeatureGroup(name="All Chargers: Zone 2 / Medium (amber)", show=True).add_to(m)
        green_group = folium.FeatureGroup(name="All Chargers: Zone 1 / Low–Outside (green)", show=True).add_to(m)

        red_cluster = MarkerCluster(name="Cluster: Zone 3 / High").add_to(red_group)
        amber_cluster = MarkerCluster(name="Cluster: Zone 2 / Medium").add_to(amber_group)
        green_cluster = MarkerCluster(name="Cluster: Zone 1 / Low–Outside").add_to(green_group)

        for _, row in df_all.iterrows():
            zlabel = (row.get("ZoneLabel") or "Outside")
            if zlabel in ("Zone 3", "High"):
                color_hex = ZONE_COLORS["Zone 3"]
                group_cluster = red_cluster
            elif zlabel in ("Zone 2", "Medium"):
                color_hex = ZONE_COLORS["Zone 2"]
                group_cluster = amber_cluster
            else:
                color_hex = ZONE_COLORS["Outside"]
                group_cluster = green_cluster

            lat = row.get("Latitude")
            lon = row.get("Longitude")
            if pd.isna(lat) or pd.isna(lon):
                continue

            title = f"{row.get('Operator','')} ({row.get('country','')})"
            try:
                tooltip_html = _row_to_tooltip_html(row, title=title)
                tooltip_obj = folium.Tooltip(tooltip_html, sticky=True)
            except Exception:
                tooltip_obj = title

            group_cluster.add_child(
                folium.Marker([float(lat), float(lon)], tooltip=tooltip_obj, icon=make_beautify_icon(color_hex))
            )

    def add_lines(lines, color, name):
        if not lines:
            return
        fg = folium.FeatureGroup(name=name).add_to(m)
        for ln in lines:
            coords = [(lat, lon) for lon, lat in coords_2d(ln.coords)]
            folium.PolyLine(coords, color=color, weight=6, opacity=0.9).add_to(fg)

    coords_full = [(lat, lon) for lon, lat in coords_2d(full_line.coords)]
    folium.PolyLine(coords_full, color="#999999", weight=3, opacity=0.5, tooltip="Planned route").add_to(m)

    add_lines(route_safe, "#2b8cbe", "Route – safe")
    add_lines(route_risk, "#e31a1c", "Route – flood risk")

    folium.Marker(start, tooltip="Start", icon=folium.Icon(color="green")).add_to(m)
    folium.Marker(end, tooltip="End", icon=folium.Icon(color="blue")).add_to(m)

    cluster = MarkerCluster(name="Planned route stops").add_to(m)
    for st in chargers or []:
        try:
            row = gdf_ev.loc[gdf_ev["ROW_ID"].eq(int(st.get("ROW_ID", -1)))].iloc[0]
        except Exception:
            continue
        zlabel = (row.get("ZoneLabel") or "Outside")
        if zlabel in ("Zone 3", "High"):
            color_hex = ZONE_COLORS["Zone 3"]
        elif zlabel in ("Zone 2", "Medium"):
            color_hex = ZONE_COLORS["Zone 2"]
        else:
            color_hex = ZONE_COLORS["Outside"]

        title = f"{row.get('Operator','')} ({row.get('country','')})"
        try:
            tooltip_html = _row_to_tooltip_html(row, title=title)
            tooltip_obj = folium.Tooltip(tooltip_html, sticky=True)
        except Exception:
            tooltip_obj = title

        folium.Marker(
            [float(row["Latitude"]), float(row["Longitude"])],
            tooltip=tooltip_obj,
            icon=make_beautify_icon(color_hex),
        ).add_to(cluster)

    if show_fraw:
        add_wms_group(m, FRAW_WMS, visible=True, opacity=0.60)
    if show_fmfp:
        add_fmfp_blue_wfs_group(m, bbox_expand(full_line.bounds, SIM_DEFAULTS["wfs_pad_m"]), visible=True)
    if show_ctx:
        add_wms_group(m, CONTEXT_WMS, visible=True, opacity=0.45)
    if show_live:
        add_wms_group(m, LIVE_WMS, visible=True, opacity=0.65)

    folium.LayerControl(collapsed=True).add_to(m)
    m.get_root().html.add_child(folium.Element("""
    <div style="position: fixed; bottom:20px; left:20px; z-index:9999; background:white;
                padding:10px 12px; border:1px solid #ccc; border-radius:6px; font-size:13px;">
      <b>Charger icon colour — Flood model zone</b>
      <div style="margin-top:6px"><span style="display:inline-block;width:12px;height:12px;background:#D32F2F;
           margin-right:6px;"></span> Red: Zone 3 / High</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:#FFC107;
           margin-right:6px;"></span> Amber: Zone 2 / Medium</div>
      <div><span style="display:inline-block;width:12px;height:12px;background:#2E7D32;
           margin-right:6px;"></span> Green: Zone 1 / Low–Very Low / Outside</div>
    </div>
    """))
    return m.get_root().render()

def render_map_html_ev_3d(df_map=None, start=None, end=None, route_full=None, route_safe=None, route_risk=None):
    if not HAS_PYDECK or not MAPBOX_API_KEY:
        return "<html><body>3D mode unavailable (pydeck/Mapbox missing).</body></html>"
    return "<html><body>3D map placeholder.</body></html>"


# ============================================================
# Journey reporting + KML export
# ============================================================

def build_route_statistics(line: LineString, safe_lines, risk_lines, stops, total_cost, batt_kwh, kwhkm):
    dist_km = float(line.length) * 111.0  # degree->km approx
    safe_km = sum(float(ln.length) * 111.0 for ln in (safe_lines or []))
    risk_km = sum(float(ln.length) * 111.0 for ln in (risk_lines or []))
    energy_kwh = dist_km * float(kwhkm)
    drive_time_min = (dist_km / 50.0) * 60.0
    return {
        "distance_km": round(dist_km, 2),
        "safe_km": round(safe_km, 2),
        "risk_km": round(risk_km, 2),
        "energy_kwh": round(energy_kwh, 2),
        "drive_time_min": round(drive_time_min, 1),
        "n_stops": int(len(stops or [])),
        "total_cost": float(total_cost),
    }

def build_journey_recommendations(stops, line: LineString, kwhkm: float):
    if not stops:
        return []
    recs = []
    dist_km = float(line.length) * 111.0
    for i, st in enumerate(stops, 1):
        try:
            row = gdf_ev.loc[gdf_ev["ROW_ID"].eq(int(st["ROW_ID"]))].iloc[0]
        except Exception:
            continue
        recs.append({
            "stop": i,
            "operator": str(row.get("Operator", "")),
            "postcode": str(row.get("Postcode", "")),
            "zone": str(row.get("ZoneLabel", "Outside")),
            "charge_kwh": round(float(st.get("energy_kWh", 0.0)), 2),
            "charge_min": int(round(float(st.get("charge_time_min", 0.0)))),
            "soc_after": int(round(100.0 * float(st.get("soc_after", 0.0)))),
            "distance_remaining_km": round(max(0.0, dist_km * (1.0 - i / max(1, len(stops)))), 1),
        })
    return recs

def build_kml(route_data: Dict[str, Any]) -> str:
    route = route_data.get("route", [])
    if not route:
        return ""
    coords = " ".join([f"{p['lon']},{p['lat']},0" for p in route])
    start = route_data.get("start", {})
    end = route_data.get("end", {})
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
  <Placemark><name>Start</name><Point><coordinates>{start.get('lon')},{start.get('lat')},0</coordinates></Point></Placemark>
  <Placemark><name>End</name><Point><coordinates>{end.get('lon')},{end.get('lat')},0</coordinates></Point></Placemark>
  <Placemark>
    <name>EV route</name>
    <LineString><tessellate>1</tessellate><coordinates>{coords}</coordinates></LineString>
  </Placemark>
</Document>
</kml>
"""


# ============================================================
# Layout (Dash Pages requires module-level `layout`)
# ============================================================

layout = html.Div(
    [
        html.H1(
            "C) Electric Vehicle (EV) Chargers & Flood Overlays and EV Travel Planning",
            style={"margin": "24px 7px 8px", "fontSize": "50px"},
        ),

        # --- explainer boxes ---
        html.Div(
            [
                html.Div(
                    [
                        html.H3("EV Chargers & Flood Overlays", style={"marginBottom": "10px"}),
                        dcc.Markdown(
                            """
This panel provides an overview of **public EV charging infrastructure** and **flood hazard overlays**, supporting informed and risk-aware EV travel planning.

### 🚗 EV Chargers
Charging points displayed on the map can be filtered by:
- **Country(s)** and **Country contains**
- **Operational status** (operational, not operational, unknown)

Only chargers marked as *operational* are considered during route optimisation.

### 🌊 Flood Overlays
The following flood layers can be visualised:
- **FRAW** – Flood Risk Assessment Wales  
- **FMfP** – Flood Map for Planning  
- **Live warnings** – near real-time alerts  
- **Context** – additional geographic layers

These overlays provide situational awareness; **only segmentation uses the union unless you enable flood-union routing**.

### ⚙️ Start-up mode
- **Light mode (fast start)** loads data quickly using conventional routing.
- For **energy-feasible and flood-aware optimisation**, Light mode should be **unticked**.
"""
                        ),
                    ],
                    style={
                        "border": "1px solid #cfe2f3",
                        "borderRadius": "12px",
                        "padding": "20px",
                        "backgroundColor": "#eaf3fb",
                        "fontSize": "18px",
                    },
                ),
                html.Div(
                    [
                        html.H3("Journey Simulator (EV Travel Planning)", style={"marginBottom": "10px"}),
                        dcc.Markdown(
                            "Deterministic EV routing with optional flood-risk segmentation. Full optimisation requires **OSMnx**."
                        ),
                        html.Div(
                            [
                                html.H4("How to Use the Journey Simulator", style={"marginTop": "16px"}),
                                dcc.Markdown(
                                    """
1. Enter start and end (lat, lon).  
2. Set battery + SoC parameters.  
3. Untick “Light mode” for full RCSP optimisation (requires OSMnx).  
4. Click **Optimise**.  
5. Use **Download KML** to export.
"""
                                ),
                            ],
                            style={
                                "marginTop": "12px",
                                "padding": "14px",
                                "borderRadius": "10px",
                                "backgroundColor": "#e6f7f5",
                                "fontSize": "25px",
                            },
                        ),
                    ],
                    style={
                        "border": "1px solid #f6c28b",
                        "borderRadius": "12px",
                        "padding": "20px",
                        "backgroundColor": "#fff2e6",
                        "fontSize": "25px",
                    },
                ),
            ],
            style={
                "display": "grid",
                "gridTemplateColumns": "repeat(auto-fit, minmax(480px, 1fr))",
                "gap": "22px",
                "marginTop": "16px",
                "marginBottom": "24px",
                "width": "100%",
            },
        ),

        html.H1("EV Chargers & Flood Overlays"),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Country(s)"),
                        dcc.Dropdown(
                            id="f-country",
                            options=[{"label": t, "value": t} for t in country_OPTIONS],
                            value=[],
                            multi=True,
                            placeholder="All countries",
                        ),
                    ],
                    style={"minWidth": "230px"},
                ),
                html.Div(
                    [
                        html.Label("Country contains"),
                        dcc.Input(id="f-country-like", type="text", placeholder="substring", debounce=True),
                    ],
                    style={"minWidth": "220px"},
                ),
                html.Div(
                    [
                        html.Label("Operational"),
                        dcc.Checklist(
                            id="f-op",
                            options=[
                                {"label": "Operational", "value": "op"},
                                {"label": "Not operational", "value": "down"},
                                {"label": "Unknown", "value": "unk"},
                            ],
                            value=["op", "down", "unk"],
                            inputStyle={"marginRight": "6px"},
                        ),
                    ],
                    style={"minWidth": "320px"},
                ),
                html.Div(
                    [
                        html.Label("Show overlays"),
                        dcc.Checklist(
                            id="layers",
                            options=[
                                {"label": "FRAW", "value": "fraw"},
                                {"label": "FMfP", "value": "fmfp"},
                                {"label": "Live warnings", "value": "live"},
                                {"label": "Context", "value": "ctx"},
                            ],
                            value=["fraw", "fmfp"],
                            inputStyle={"marginRight": "6px"},
                        ),
                    ],
                    style={"minWidth": "360px"},
                ),
                html.Div(
                    [
                        html.Label("Start-up mode"),
                        dcc.Checklist(
                            id="light",
                            options=[{"label": "Light mode (fast start)", "value": "on"}],
                            value=["on"],
                        ),
                    ],
                    style={"minWidth": "260px"},
                ),
                html.Button(
                    "Compute/Update zones",
                    id="btn-zones",
                    n_clicks=0,
                    style={"height": "38px", "marginLeft": "8px"},
                ),
            ],
            style={
                "display": "flex",
                "gap": "12px",
                "alignItems": "end",
                "flexWrap": "wrap",
                "margin": "6px 0 12px",
            },
        ),

        html.H1("Journey Simulator (EV Travel Planning)"),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Start (lat, lon)"),
                        dcc.Input(id="sla", type="number", value=SIM_DEFAULTS["start_lat"], step=0.001, style={"width": "45%"}),
                        dcc.Input(id="slo", type="number", value=SIM_DEFAULTS["start_lon"], step=0.001, style={"width": "45%", "marginLeft": "4px"}),
                    ],
                    style={"minWidth": "230px"},
                ),
                html.Div(
                    [
                        html.Label("End (lat, lon)"),
                        dcc.Input(id="ela", type="number", value=SIM_DEFAULTS["end_lat"], step=0.001, style={"width": "45%"}),
                        dcc.Input(id="elo", type="number", value=SIM_DEFAULTS["end_lon"], step=0.001, style={"width": "45%", "marginLeft": "4px"}),
                    ],
                    style={"minWidth": "260px"},
                ),
                html.Div(
                    [
                        html.Label("Battery size (kWh)"),
                        dcc.Input(id="batt", type="number", value=float(SIM_DEFAULTS["battery_kwh"]), step=1, min=20, max=120, style={"width": "100%"}),
                    ],
                    style={"minWidth": "180px"},
                ),
            ],
            style={"display": "flex", "gap": "12px", "alignItems": "flex-start", "flexWrap": "wrap"},
        ),

        html.Div(
            [
                html.Div(
                    [
                        html.Label("Initial SoC"),
                        dcc.Slider(id="si", min=0.1, max=1.0, step=0.05, value=0.90),
                        html.Div(id="si-label", style={"textAlign": "right", "fontSize": "12px", "color": "#666"}, children="90%"),
                    ],
                    style={"minWidth": "220px"},
                ),
                html.Div(
                    [
                        html.Label("Reserve SoC"),
                        dcc.Slider(id="sres", min=0.05, max=0.30, step=0.05, value=0.10),
                        html.Div(id="sres-label", style={"textAlign": "right", "fontSize": "12px", "color": "#666"}, children="10%"),
                    ],
                    style={"minWidth": "220px"},
                ),
                html.Div(
                    [
                        html.Label("Target SoC"),
                        dcc.Slider(id="stgt", min=0.5, max=1.0, step=0.05, value=0.80),
                        html.Div(id="stgt-label", style={"textAlign": "right", "fontSize": "12px", "color": "#666"}, children="80%"),
                    ],
                    style={"minWidth": "220px"},
                ),
                html.Div(
                    [
                        html.Label("Consumption (kWh/km)"),
                        dcc.Slider(id="kwhkm", min=0.10, max=0.30, step=0.005, value=0.20),
                        html.Div(id="kwhkm-label", style={"textAlign": "right", "fontSize": "12px", "color": "#666"}, children="0.20 kWh/km"),
                    ],
                    style={"minWidth": "260px"},
                ),
                html.Div(
                    [
                        html.Label("Max charge power (kW)"),
                        dcc.Input(id="pmax", type="number", value=120.0, step=5, min=20, max=350, style={"width": "100%"}),
                    ],
                    style={"minWidth": "180px"},
                ),
            ],
            style={"display": "grid", "gridTemplateColumns": "repeat(3, minmax(220px, 1fr))", "gap": "12px", "marginTop": "8px"},
        ),

        html.Div(
            [
                dcc.Checklist(
                    id="show_leg_details",
                    options=[{"label": "Show per-leg details", "value": "details"}],
                    value=["details"],
                    inline=True,
                ),
                dcc.RadioItems(
                    id="units",
                    options=[{"label": "km / kWh / %", "value": "metric"}, {"label": "miles / kWh / %", "value": "imperial"}],
                    value="metric",
                    inline=True,
                    style={"marginLeft": "16px"},
                ),
            ],
            style={"display": "flex", "alignItems": "center", "gap": "12px", "flexWrap": "wrap"},
        ),

        html.Div(
            [
                html.Button("Optimise", id="simulate", n_clicks=0, style={"marginTop": "10px"}),
                html.Button("Download KML", id="btn-kml", n_clicks=0, style={"marginLeft": "8px", "marginTop": "10px"}),
            ],
            style={"display": "flex", "gap": "8px"},
        ),

        html.Div(
            id="status",
            style={"marginTop": "10px"},
            children=(
                dcc.Markdown(f"**EV data load issue:** {EV_DATA_ERROR}") if EV_DATA_ERROR else ""
            ),
        ),
        html.Div(id="explain", style={"marginTop": "10px", "whiteSpace": "pre-line"}),

        html.Div(
            [
                html.Label("Map mode"),
                dcc.RadioItems(
                    id="map-mode",
                    options=[{"label": "2D (Folium)", "value": "2d"}, {"label": "3D (beta)", "value": "3d"}],
                    value="2d",
                    inline=True,
                ),
            ],
            style={"marginTop": "8px"},
        ),

        dcc.Loading(
            html.Iframe(
                id="map",
                srcDoc="<html><body style='font-family:sans-serif;padding:10px'>Loading…</body></html>",
                style={"width": "100%", "height": "99vh", "border": "1px solid #ddd", "borderRadius": "8px"},
            )
        ),

        html.Div(id="itinerary", style={"marginTop": "12px"}),

        dcc.Store(id="zones-json", data=preload_zones_json()),
        dcc.Store(id="store-route"),
        dcc.Store(id="heat-store"),
        dcc.Download(id="dl-kml"),
    ]
)


# ============================================================
# Zones recompute callback (so the button actually works)
# ============================================================

@callback(
    Output("zones-json", "data"),
    Input("btn-zones", "n_clicks"),
    prevent_initial_call=True,
)
def _recompute_zones(_n):
    z = safe_compute_zones()
    return z.to_json(orient="records")


# ============================================================
# Single callback (ONE decorator only)
# ============================================================

@callback(
    Output("map", "srcDoc"),
    Output("itinerary", "children"),
    Output("store-route", "data"),
    Input("f-country", "value"),
    Input("f-country-like", "value"),
    Input("f-op", "value"),
    Input("layers", "value"),
    Input("light", "value"),
    Input("zones-json", "data"),
    Input("simulate", "n_clicks"),
    State("sla", "value"),
    State("slo", "value"),
    State("ela", "value"),
    State("elo", "value"),
    State("batt", "value"),
    State("si", "value"),
    State("sres", "value"),
    State("stgt", "value"),
    State("kwhkm", "value"),
    State("pmax", "value"),
    State("show_leg_details", "value"),
    State("units", "value"),
    State("map-mode", "value"),
    State("heat-store", "data"),
)
def _update_map(
    countrys,
    country_like,
    op_vals,
    layers_vals,
    light_vals,
    zones_json,
    sim_clicks,
    sla,
    slo,
    ela,
    elo,
    batt,
    si,
    sres,
    stgt,
    kwhkm,
    pmax,
    show_leg_details,
    units,
    map_mode,
    heat_store,
):
    d = gdf_ev.copy()

    # zones merge
    try:
        zextra = pd.read_json(io.StringIO(zones_json)) if zones_json and zones_json != "[]" else None
    except Exception:
        zextra = None

    if zextra is None or zextra.empty or not {"ROW_ID", "ZoneLabel", "ZoneColor"}.issubset(zextra.columns):
        zones_df = pd.DataFrame({"ROW_ID": gdf_ev.get("ROW_ID", pd.Series(dtype=int)), "ZoneLabel": "Outside", "ZoneColor": ZONE_COLORS["Outside"]})
    else:
        zones_df = zextra[["ROW_ID", "ZoneLabel", "ZoneColor"]].copy()

    if not d.empty:
        d = d.merge(zones_df, on="ROW_ID", how="left")
        d["ZoneLabel"] = d["ZoneLabel"].fillna("Outside")
        d["ZoneColor"] = d["ZoneColor"].fillna(ZONE_COLORS["Outside"])

    # filters
    if countrys and not d.empty:
        d = d[d["country"].isin(countrys)]
    if country_like and not d.empty:
        s = str(country_like).strip().lower()
        if s:
            d = d[d["country"].str.lower().str.contains(s, na=False)]

    op_vals = set(op_vals or [])
    if not d.empty and op_vals and len(op_vals) < 3:
        mask = pd.Series(False, index=d.index)
        if "op" in op_vals:
            mask |= d["AvailabilityLabel"].eq("Operational")
        if "down" in op_vals:
            mask |= d["AvailabilityLabel"].eq("Not operational")
        if "unk" in op_vals:
            mask |= d["AvailabilityLabel"].eq("Unknown") | d["AvailabilityLabel"].isna()
        d = d[mask]

    layers_vals = set(layers_vals or [])
    show_fraw = "fraw" in layers_vals
    show_fmfp = "fmfp" in layers_vals
    show_live = "live" in layers_vals
    show_ctx = "ctx" in layers_vals

    light = "on" in (light_vals or [])

    itinerary_children = html.Div()
    route_store: Dict[str, Any] = {}

    route_chargers = d[d["AvailabilityLabel"].eq("Operational")].copy() if not d.empty else pd.DataFrame()

    if sim_clicks:
        try:
            flood_union_m = None
            if (not light) and ENABLE_ROUTE_FLOOD_UNION:
                bounds = (
                    min(float(slo), float(elo)),
                    min(float(sla), float(ela)),
                    max(float(slo), float(elo)),
                    max(float(sla), float(ela)),
                )
                flood_union_m = get_flood_union(
                    bounds,
                    include_live=True,
                    include_fraw=True,
                    include_fmfp=True,
                    pad_m=(SIM_DEFAULTS["wfs_pad_m_fast"] if FAST_MODE_DEFAULT else SIM_DEFAULTS["wfs_pad_m"]),
                )

            # fast path
            if light or not HAS_OSMNX:
                line, dist_m, dur_s, step_text, _src = osrm_route(float(sla), float(slo), float(ela), float(elo))
                safe_lines, risk_lines = segment_route_by_risk(line, flood_union_m, buffer_m=ROUTE_BUFFER_M)

                html_str = render_map_html_route(
                    full_line=line,
                    route_safe=safe_lines,
                    route_risk=risk_lines,
                    start=(float(sla), float(slo)),
                    end=(float(ela), float(elo)),
                    chargers=[],
                    all_chargers_df=route_chargers,
                    animate=False,
                    speed_kmh=45,
                    show_fraw=show_fraw,
                    show_fmfp=show_fmfp,
                    show_live=show_live,
                    show_ctx=show_ctx,
                )

                msg = [f"**Routing plan (fast/OSRM)** — {dist_m/1000.0:.1f} km • {dur_s/3600.0:.2f} h"]
                if step_text:
                    msg.append("---")
                    msg.extend([f"- {t}" for t in step_text[:12]])
                itinerary_children = dcc.Markdown("\n".join(msg))

                coords_latlng = [{"lat": lat, "lon": lon} for lon, lat in coords_2d(line.coords)]
                route_store = {
                    "start": {"lat": float(sla), "lon": float(slo)},
                    "end": {"lat": float(ela), "lon": float(elo)},
                    "route": coords_latlng,
                    "stops": [],
                    "created_ts": time.time(),
                }
                return html_str, itinerary_children, route_store

            # full RCSP
            line, safe_lines, risk_lines, stops, total_cost = rcsp_optimize(
                float(sla),
                float(slo),
                float(ela),
                float(elo),
                float(batt),
                float(si),
                float(sres),
                float(stgt),
                float(kwhkm),
                route_chargers if not route_chargers.empty else gdf_ev,
                flood_union_m,
                extreme=False,
                max_seconds=10.0,
                soc_step=(SIM_DEFAULTS["soc_step_fast"] if light else SIM_DEFAULTS["soc_step_normal"]),
            )

            if map_mode == "3d":
                html_str = render_map_html_ev_3d(
                    df_map=None,
                    start=(float(sla), float(slo)),
                    end=(float(ela), float(elo)),
                    route_full=line,
                    route_safe=safe_lines,
                    route_risk=risk_lines,
                )
            else:
                html_str = render_map_html_route(
                    full_line=line,
                    route_safe=safe_lines,
                    route_risk=risk_lines,
                    start=(float(sla), float(slo)),
                    end=(float(ela), float(elo)),
                    chargers=stops,
                    all_chargers_df=route_chargers,
                    animate=False,
                    speed_kmh=45,
                    show_fraw=show_fraw,
                    show_fmfp=show_fmfp,
                    show_live=show_live,
                    show_ctx=show_ctx,
                )

            stats = build_route_statistics(line, safe_lines, risk_lines, stops, total_cost, batt, kwhkm)
            recs = build_journey_recommendations(stops, line, float(kwhkm))

            rows = [
                "### 🚗 Journey summary",
                f"- **Total distance:** {stats['distance_km']} km",
                f"- **Estimated drive time:** {stats['drive_time_min']} min",
                f"- **Energy required:** {stats['energy_kwh']} kWh",
                f"- **Flood-safe distance:** {stats['safe_km']} km",
                f"- **Flood-risk distance:** {stats['risk_km']} km",
                f"- **Charging stops:** {stats['n_stops']}",
            ]
            if recs:
                rows.append("\n### 🔌 Charging recommendations")
                for r in recs:
                    rows.append(
                        f"**Stop {r['stop']}** — {r['operator']} ({r['postcode']})  \n"
                        f"- Flood zone: **{r['zone']}**  \n"
                        f"- Charge **+{r['charge_kwh']} kWh** (~{r['charge_min']} min) → **{r['soc_after']}% SoC**  \n"
                        f"- Distance remaining: **{r['distance_remaining_km']} km**"
                    )

            itinerary_children = dcc.Markdown("\n\n".join(rows))

            coords_latlng = [{"lat": lat, "lon": lon} for lon, lat in coords_2d(line.coords)]
            route_store = {
                "start": {"lat": float(sla), "lon": float(slo)},
                "end": {"lat": float(ela), "lon": float(elo)},
                "route": coords_latlng,
                "stops": stops,
                "statistics": stats,
                "created_ts": time.time(),
            }
            return html_str, itinerary_children, route_store

        except TimeoutError as e:
            itinerary_children = dcc.Markdown(f"**Routing timeout:** {e}")
            html_str = render_map_html_ev(d, show_fraw, show_fmfp, show_live, show_ctx, light=light, heat_data=heat_store)
            return html_str, itinerary_children, {}

        except Exception as e:
            itinerary_children = dcc.Markdown(f"**Routing error:** {e}")
            html_str = render_map_html_ev(d, show_fraw, show_fmfp, show_live, show_ctx, light=light, heat_data=heat_store)
            return html_str, itinerary_children, {}

    # no optimise yet
    if map_mode == "3d":
        html_str = render_map_html_ev_3d(d)
    else:
        html_str = render_map_html_ev(d, show_fraw, show_fmfp, show_live, show_ctx, light=light, heat_data=heat_store)

    return html_str, itinerary_children, route_store


@callback(
    Output("dl-kml", "data"),
    Input("btn-kml", "n_clicks"),
    State("store-route", "data"),
    prevent_initial_call=True,
)
def _download_kml(_n, route_data):
    if not route_data or not (route_data.get("route") and route_data.get("start") and route_data.get("end")):
        return dash.no_update
    kml = build_kml(route_data)
    return {
        "content": kml,
        "filename": "ev_journey.kml",
        "type": "application/vnd.google-earth.kml+xml",
    }
