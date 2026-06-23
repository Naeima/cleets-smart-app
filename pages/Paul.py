from dash import html, dcc, Input, Output, State, ctx, callback, register_page 
from dash import dash_table
import dash_bootstrap_components as dbc

import folium
from folium.plugins import HeatMap, MarkerCluster
import pandas as pd
import numpy as np
import requests
import re
import io
import json
import time
from functools import lru_cache
from pathlib import Path
from typing import Optional
from shapely.geometry import shape

# Dash page registration
register_page(__name__, path="/Paul", name="Paul EV Dashboard")
# ============================================================
# CLEETS EV dashboard
# Folium plotting version inspired by thrust_one.py
# ============================================================


# ---------------------------
# Data sources from thrust_one.py
# ---------------------------
DATA_SOURCE = "https://drive.google.com/uc?export=download&id=1nEq37_ZILPI3GIj2QyBXfJ390iJadjur"          # EV population
VEH0105_URL = "https://drive.google.com/uc?export=download&id=1MqF57lLua8HSEFOYV0V2lnZmy5fiKGMP"           # Veh0105 All vehicles
DATA_SOURCE_LSOA = "https://drive.google.com/uc?export=download&id=1wM0pxGBn67vCDpLdZmeyGYZgq59IsoVU"     # LSOA EV keepership
WIMD_URL = "https://drive.google.com/uc?export=download&id=1BZfi7MAKYXWJ8a2dd4BJHbOo-X5zEUIT"             # WIMD
CHARGE_URL = "https://drive.google.com/uc?export=download&id=1FLnVRHaKya7nKd1FgObPSeuTKV1zJ3Q_"           # EV chargers
COMBINED_DATASET_URL = "https://drive.google.com/uc?export=download&id=14Mdz9M1xcIApHxawggGdxQBPnfeosF8A" # Combined dataset

LAD_FS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Authority_Districts_May_2024_Boundaries_UK_BGC/FeatureServer"
LAD_LAYER = "0"

LSOA_FS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_2021_EW_BFE_V10_RUC/FeatureServer"
LSOA_LAYER = "3"

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

groups = ["Output", "Vehicles", "Chargers", "Equity", "Comparisons"]

layer_options = {
    "Output": [
        {"label": "Map", "value": "map"},
        {"label": "Trends", "value": "trends"},
    ],
    "Vehicles": [
        {"label": "All", "value": "All"},
        {"label": "Plug-in", "value": "Plug-in"},
        {"label": "Battery Electric", "value": "Battery Electric"},
        {"label": "Ultra Low Emissions", "value": "Ultra Low Emissions"},
    ],
    "Chargers": [
        {"label": "Public Chargers", "value": "public_chargers"},
        {"label": "Fast Chargers", "value": "fast_chargers"},
        {"label": "Ultra-Fast Chargers", "value": "ultra_fast_chargers"},
        {"label": "Charging Hubs", "value": "charging_hubs"},
    ],
    "Equity": [
        {"label": "Accessibility Index", "value": "accessibility_index"},
        {"label": "Income Deprivation", "value": "income_deprivation"},
        {"label": "Rural Charging Access", "value": "rural_charging_access"},
        {"label": "Charging Inequality Gap", "value": "charging_inequality_gap"},
    ],
    "Comparisons": [
        {"label": "Percent (%)", "value": "Percent (%)"},
        {"label": "Income Deprivation", "value": "Income Deprivation"},
        {"label": "Plug-in (%)", "value": "Plug-in (%)"},
        {"label": "Battery Electric (%)", "value": "Battery Electric (%)"},
        {"label": "Ultra Low Emissions (%)", "value": "Ultra Low Emissions (%)"},
    ],
}

FOLIUM_COLOURS = {
    "Vehicles": "YlGnBu",
    "Chargers": "YlOrRd",
    "Equity": "RdYlGn",
    "Comparisons": "PuBuGn",
}

# ============================================================
# Generic helpers
# ============================================================

def google_drive_direct_url(url: str) -> str:
    m = re.search(r"drive\.google\.com/file/d/([^/]+)", str(url))
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    return str(url).replace("/view?usp=drive_link", "").replace("/view?usp=sharing", "")


def _download(url: str) -> tuple[bytes, str]:
    url = google_drive_direct_url(url)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    ctype = (r.headers.get("Content-Type") or "").lower()
    if b"<html" in r.content[:500].lower() and b"google" in r.content[:500].lower():
        raise RuntimeError("Google Drive returned HTML instead of data. Make the file public: Anyone with the link can view.")
    return r.content, ctype


def load_data(url: str) -> pd.DataFrame:
    data, ctype = _download(url)

    if "spreadsheetml" in ctype or "excel" in ctype:
        return pd.read_excel(io.BytesIO(data))

    try:
        return pd.read_csv(io.BytesIO(data))
    except Exception:
        return pd.read_excel(io.BytesIO(data))


def pick_col(cols, candidates) -> Optional[str]:
    low = {str(c).strip().lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
    return None


def first_matching_code_column(df: pd.DataFrame, pattern: str) -> Optional[str]:
    for c in df.columns:
        sample = df[c].dropna().astype(str).str.strip().head(500)
        if sample.str.match(pattern, na=False).any():
            return c
    return None


def parse_num(x):
    if pd.isna(x):
        return np.nan

    s = str(x).strip()
    if re.match(r"^[A-Z]\d{2}\d{5,}$", s):
        return np.nan

    if s.lower() in {"", "na", "n/a", "null", "none", "[z]", "[x]", "[c]", "..", ".", "-", "—"}:
        return np.nan

    s = s.replace(",", "").replace("%", "").strip()
    m = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)
    return float(m.group(0)) if m else np.nan


def to_int(x):
    v = parse_num(x)
    if pd.isna(v):
        return np.nan
    return int(v)


def available_quarters(df: pd.DataFrame) -> list[str]:
    qs = []
    for c in df.columns:
        cs = str(c).strip()
        if re.match(r"^\d{4}\s*Q[1-4]$", cs, flags=re.I):
            qs.append(cs)

    def key(q):
        m = re.match(r"^(\d{4})\s*Q([1-4])$", q, flags=re.I)
        return (int(m.group(1)), int(m.group(2))) if m else (0, 0)

    return sorted(set(qs), key=key, reverse=True)


def latest_quarter_col(df: pd.DataFrame) -> Optional[str]:
    qs = available_quarters(df)
    if qs:
        return qs[0]

    numeric_cols = []
    for c in df.columns:
        if df[c].apply(parse_num).notna().sum() > 0:
            numeric_cols.append(c)

    return numeric_cols[-1] if numeric_cols else None


def sql_in(field: str, values: list[str]) -> str:
    vals = ["'" + str(v).replace("'", "''") + "'" for v in values]
    return f"{field} IN ({', '.join(vals)})"


def pick_field(fields, cands):
    low = {str(c).lower(): c for c in fields}
    for c in cands:
        if c in low:
            return low[c]
    return None


def arcgis_pjson(url: str) -> dict:
    r = requests.get(url, params={"f": "pjson"}, timeout=60)
    r.raise_for_status()
    return r.json()


def arcgis_query_geojson(fs: str, layer: str, where: str, out_fields: str = "*", page: int = 2000) -> dict:
    url = f"{fs}/{layer}/query"
    feats, offset = [], 0

    while True:
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "true",
            "outSR": 4326,
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": page,
        }

        # ArcGIS can be flaky, so retry lightly.
        last_error = None
        for attempt in range(4):
            try:
                r = requests.post(url, data=params, timeout=180)
                if r.status_code in {502, 503, 504}:
                    time.sleep(1.2 * (attempt + 1))
                    continue
                r.raise_for_status()
                data = r.json()
                break
            except Exception as e:
                last_error = e
                time.sleep(1.2 * (attempt + 1))
        else:
            raise last_error

        batch = data.get("features", [])
        feats.extend(batch)

        if len(batch) < page:
            break

        offset += page

    return {"type": "FeatureCollection", "features": feats}


def arcgis_query_geojson_in_chunks(fs: str, layer: str, field: str, values: list[str], out_fields: str = "*", chunk_size: int = 200) -> dict:
    all_feats = []
    values = [str(v) for v in values if pd.notna(v)]

    for i in range(0, len(values), chunk_size):
        gj = arcgis_query_geojson(
            fs,
            layer,
            where=sql_in(field, values[i:i + chunk_size]),
            out_fields=out_fields,
        )
        all_feats.extend(gj.get("features", []))

    return {"type": "FeatureCollection", "features": all_feats}


# ============================================================
# Boundary loaders
# ============================================================

@lru_cache(maxsize=1)
def lad_meta():
    meta = arcgis_pjson(f"{LAD_FS}/{LAD_LAYER}")
    fields = [f["name"] for f in meta.get("fields", [])]
    code_field = pick_field(fields, ["lad24cd", "lad23cd", "lad22cd", "lad21cd", "ladcd"])
    name_field = pick_field(fields, ["lad24nm", "lad23nm", "lad22nm", "lad21nm", "ladnm"])
    return code_field, name_field


@lru_cache(maxsize=1)
def lsoa_meta():
    meta = arcgis_pjson(f"{LSOA_FS}/{LSOA_LAYER}")
    fields = [f["name"] for f in meta.get("fields", [])]
    code_field = pick_field(fields, ["lsoa21cd", "lsoa11cd", "lsoacd"])
    name_field = pick_field(fields, ["lsoa21nm", "lsoa11nm", "lsoanm"]) or code_field
    return code_field, name_field


@lru_cache(maxsize=16)
def lad_geojson_for_codes(codes_tuple: tuple[str, ...]):
    code_field, name_field = lad_meta()
    out_fields = ",".join([x for x in [code_field, name_field] if x])
    return arcgis_query_geojson_in_chunks(
        LAD_FS,
        LAD_LAYER,
        field=code_field,
        values=list(codes_tuple),
        out_fields=out_fields,
        chunk_size=200,
    ), code_field, name_field


@lru_cache(maxsize=16)
def lsoa_geojson_for_codes(codes_tuple: tuple[str, ...]):
    code_field, name_field = lsoa_meta()
    out_fields = ",".join([x for x in [code_field, name_field] if x])
    return arcgis_query_geojson_in_chunks(
        LSOA_FS,
        LSOA_LAYER,
        field=code_field,
        values=list(codes_tuple),
        out_fields=out_fields,
        chunk_size=200,
    ), code_field, name_field


# ============================================================
# Data loaders
# ============================================================

@lru_cache(maxsize=4)
def load_ev_lad_df():
    df = load_data(DATA_SOURCE)
    df.columns = [str(c).strip() for c in df.columns]
    return df


@lru_cache(maxsize=4)
def load_veh0105_df():
    df = load_data(VEH0105_URL)
    df.columns = [str(c).strip() for c in df.columns]
    return df


@lru_cache(maxsize=4)
def load_ev_lsoa_df():
    df = load_data(DATA_SOURCE_LSOA)
    df.columns = [str(c).strip() for c in df.columns]
    return df


@lru_cache(maxsize=4)
def load_wimd_df():
    df = load_data(WIMD_URL)
    df.columns = [str(c).strip() for c in df.columns]
    return df


@lru_cache(maxsize=4)
def load_charge_df():
    df = load_data(CHARGE_URL)
    df.columns = [str(c).strip() for c in df.columns]
    return df


@lru_cache(maxsize=4)
def load_combined_df():
    df = load_data(COMBINED_DATASET_URL)
    df.columns = [str(c).strip() for c in df.columns]
    return df


# ============================================================
# Dataset preparation
# ============================================================

def prepare_vehicle_data(selection: str, geo_level: str = "LAD") -> tuple[pd.DataFrame, str]:
    """Vehicles → All is wired to the Veh0105 dataset.

    Other vehicle categories are intentionally disabled until their own
    matching source files are wired.
    """
    if selection != "All":
        return pd.DataFrame(), geo_level

    if geo_level == "LSOA":
        df = load_ev_lsoa_df().copy()
        code_col = pick_col(df.columns, [
            "ons code", "ons_code", "area code", "area_code",
            "lsoa code", "lsoa_code", "lsoa21cd", "lsoa11cd",
            "geography code", "geo code"
        ]) or first_matching_code_column(df, r"^(E01|W01)\d+")
        name_col = pick_col(df.columns, [
            "ons geography", "ons_geography", "area name", "area_name",
            "lsoa name", "lsoa21nm", "lsoa11nm", "geography", "name"
        ])
        quarter_col = latest_quarter_col(df)

        if code_col is None or quarter_col is None:
            return pd.DataFrame(), "LSOA"

        out = df[[code_col] + ([name_col] if name_col else []) + [quarter_col]].copy()
        out.columns = ["area_code"] + (["area_name"] if name_col else []) + ["value"]

        if "area_name" not in out.columns:
            out["area_name"] = out["area_code"]

        out["area_code"] = out["area_code"].astype(str).str.strip()
        out["area_name"] = out["area_name"].astype(str).str.strip()
        out["value"] = out["value"].apply(parse_num)
        # England and Wales LSOA codes begin E01 and W01.
        out = out[out["area_code"].str.match(r"^(E01|W01)", na=False)].dropna(subset=["value"])
        out = out.groupby(["area_code", "area_name"], as_index=False)["value"].sum()
        out["metric"] = f"All vehicle count ({quarter_col})"
        out["geography"] = "LSOA"

        return out.head(200), "LSOA"

    # LAD / LA view: use Veh0105 directly.
    df = load_veh0105_df().copy()

    code_col = pick_col(df.columns, [
        "ons code", "ons_code", "area code", "area_code",
        "local authority code", "lad code", "lad_code", "geography code"
    ])
    name_col = pick_col(df.columns, [
        "ons geography", "ons_geography", "area name", "area_name",
        "local authority", "lad name", "lad_name", "geography", "name"
    ])

    # Prefer the latest quarter column in Veh0105, e.g. 2025 Q4.
    quarter_col = latest_quarter_col(df)

    if code_col is None:
        code_col = first_matching_code_column(df, r"^(E06|E07|E08|E09|W06)\d+")

    if code_col is None or quarter_col is None:
        return pd.DataFrame(), "LAD"

    # If the file has multiple rows per geography, keep all and aggregate.
    out = df[[code_col] + ([name_col] if name_col else []) + [quarter_col]].copy()
    out.columns = ["area_code"] + (["area_name"] if name_col else []) + ["value"]

    if "area_name" not in out.columns:
        out["area_name"] = out["area_code"]

    out["area_code"] = out["area_code"].astype(str).str.strip()
    out["area_name"] = out["area_name"].astype(str).str.strip()
    out["value"] = out["value"].apply(parse_num)

    # Veh0105 is a LAD-coded table. England LAD codes include E06, E07, E08 and E09; Wales LAD codes start W06.
    out = out[out["area_code"].str.match(r"^(E06|E07|E08|E09|W06)", na=False)].dropna(subset=["value"])

    if out.empty:
        return pd.DataFrame(), "LAD"

    out = out.groupby(["area_code", "area_name"], as_index=False)["value"].sum()
    out["metric"] = f"Veh0105 All vehicles ({quarter_col})"
    out["geography"] = "LAD"

    return out, "LAD"

def prepare_wimd_lsoa(selection: str) -> tuple[pd.DataFrame, str]:
    df = load_wimd_df().copy()

    # Original WIMD long format.
    if {"Area code", "Domain", "Data values"}.issubset(set(df.columns)):
        code_col = "Area code"
        domain_col = "Domain"
        value_col = "Data values"
        df[domain_col] = df[domain_col].astype(str)

        if selection == "income_deprivation":
            df = df[df[domain_col].str.contains("income", case=False, na=False)]
        elif selection == "accessibility_index":
            df = df[df[domain_col].str.contains("access|services", case=False, na=False)]

        out = pd.DataFrame({
            "area_code": df[code_col].astype(str).str.strip(),
            "area_name": df[code_col].astype(str).str.strip(),
            "value": df[value_col].apply(parse_num),
        })
    else:
        code_col = pick_col(df.columns, ["lsoa21cd", "lsoa11cd", "lsoa code", "area code", "geography code", "code"]) or first_matching_code_column(df, r"^W01\d+")
        name_col = pick_col(df.columns, ["lsoa21nm", "lsoa11nm", "lsoa name", "area name", "name"])
        if selection == "income_deprivation":
            value_col = pick_col(df.columns, ["deprivation[0,1]", "deprivation", "deprivation score", "income_rank", "income decile", "score", "%"])
        else:
            value_col = latest_quarter_col(df) or pick_col(df.columns, ["value", "score", "rank"])

        if code_col is None or value_col is None:
            return pd.DataFrame(), "LSOA"

        out = df[[code_col] + ([name_col] if name_col else []) + [value_col]].copy()
        out.columns = ["area_code"] + (["area_name"] if name_col else []) + ["value"]
        if "area_name" not in out.columns:
            out["area_name"] = out["area_code"]

    out["area_code"] = out["area_code"].astype(str).str.strip()
    out["area_name"] = out["area_name"].astype(str).str.strip()
    out["value"] = out["value"].apply(parse_num)
    out = out[out["area_code"].str.match(r"^W01", na=False)].dropna(subset=["value"])

    out = out.groupby(["area_code", "area_name"], as_index=False)["value"].mean()
    out["metric"] = selection
    out["geography"] = "LSOA"

    # Keep the LSOA plot responsive. Increase this limit when needed.
    out = out.head(2500)

    return out, "LSOA"


def prepare_charger_data(selection: str) -> tuple[pd.DataFrame, str]:
    df = load_charge_df().copy()

    lat_col = pick_col(df.columns, ["latitude", "lat", "y", "y_wgs84", "y_coordinate"])
    lon_col = pick_col(df.columns, ["longitude", "lon", "lng", "long", "x", "x_wgs84", "x_coordinate"])

    if lat_col and lon_col:
        text_cols = [c for c in df.columns if df[c].dtype == object]
        if selection in {"fast_chargers", "ultra_fast_chargers", "charging_hubs"} and text_cols:
            text = df[text_cols].astype(str).agg(" ".join, axis=1)
            if selection == "fast_chargers":
                df = df[text.str.contains("fast|rapid", case=False, regex=True, na=False)]
            elif selection == "ultra_fast_chargers":
                df = df[text.str.contains("ultra", case=False, na=False)]
            elif selection == "charging_hubs":
                df = df[text.str.contains("hub", case=False, na=False)]

        out = df.copy()
        out["lat"] = pd.to_numeric(out[lat_col], errors="coerce")
        out["lon"] = pd.to_numeric(out[lon_col], errors="coerce")
        out = out.dropna(subset=["lat", "lon"])
        out["area_code"] = ""
        out["area_name"] = "EV charger"
        out["value"] = 1
        out["metric"] = selection
        out["geography"] = "POINT"

        # Keep markers responsive.
        return out.head(2500), "POINT"

    # Fallback if charger source is area-coded.
    code_col = pick_col(df.columns, ["local authority code", "area code", "lad code", "ons code", "code"])
    name_col = pick_col(df.columns, ["local authority", "area name", "lad name", "geography", "name"])
    value_col = pick_col(df.columns, ["value", "count", "charger_count", "number of devices", "devices", "charging devices"])

    if code_col is None or value_col is None:
        return pd.DataFrame(), "LAD"

    out = df[[code_col] + ([name_col] if name_col else []) + [value_col]].copy()
    out.columns = ["area_code"] + (["area_name"] if name_col else []) + ["value"]
    if "area_name" not in out.columns:
        out["area_name"] = out["area_code"]

    out["area_code"] = out["area_code"].astype(str).str.strip()
    out["area_name"] = out["area_name"].astype(str).str.strip()
    out["value"] = out["value"].apply(parse_num)
    out = out[out["area_code"].str.match(r"^(E06|E07|E08|E09|W06)", na=False)].dropna(subset=["value"])
    out = out.groupby(["area_code", "area_name"], as_index=False)["value"].sum()
    out["metric"] = selection
    out["geography"] = "LAD"

    return out, "LAD"


def prepare_comparison_data(selection: str) -> tuple[pd.DataFrame, str]:
    df = load_combined_df().copy()

    code_col = pick_col(df.columns, ["area_code", "area code", "ons code", "lad code", "lsoa code", "geography code", "code"])
    name_col = pick_col(df.columns, ["area_name", "area name", "ons geography", "geography", "name"])

    if code_col is None:
        return pd.DataFrame(), "LAD"

    candidates = {
        "Percent (%)": ["percent", "%", "percentage"],
        "Income Deprivation": ["income_decile", "income_rank", "deprivation", "income"],
        "Plug-in (%)": ["plug", "plugin", "plug_in"],
        "Battery Electric (%)": ["battery", "bev"],
        "Ultra Low Emissions (%)": ["ultra", "ulev"],
    }

    value_col = None
    for c in df.columns:
        c_low = str(c).lower()
        if any(tok in c_low for tok in candidates.get(selection, [])):
            if df[c].apply(parse_num).notna().sum() > 0:
                value_col = c
                break

    if value_col is None:
        value_col = latest_quarter_col(df) or pick_col(df.columns, ["value", "count"])

    if value_col is None:
        return pd.DataFrame(), "LAD"

    out = df[[code_col] + ([name_col] if name_col else []) + [value_col]].copy()
    out.columns = ["area_code"] + (["area_name"] if name_col else []) + ["value"]
    if "area_name" not in out.columns:
        out["area_name"] = out["area_code"]

    out["area_code"] = out["area_code"].astype(str).str.strip()
    out["area_name"] = out["area_name"].astype(str).str.strip()
    out["value"] = out["value"].apply(parse_num)
    out = out.dropna(subset=["value"])

    if out["area_code"].str.match(r"^W01|^E01", na=False).any():
        out = out[out["area_code"].str.match(r"^W01|^E01", na=False)].head(2500)
        geo = "LSOA"
    else:
        out = out[out["area_code"].str.match(r"^(E06|E07|E08|E09|W06)", na=False)]
        geo = "LAD"

    out = out.groupby(["area_code", "area_name"], as_index=False)["value"].mean()
    out["metric"] = selection
    out["geography"] = geo

    return out, geo


def prepare_dataset(group: str, selection: str, geo_level: str = "LAD") -> tuple[pd.DataFrame, str]:
    if group == "Vehicles":
        return prepare_vehicle_data(selection, geo_level=geo_level)

    if group == "Chargers":
        return prepare_charger_data(selection)

    if group == "Equity":
        return prepare_wimd_lsoa(selection)

    if group == "Comparisons":
        return prepare_comparison_data(selection)

    return pd.DataFrame(), "NONE"


# ============================================================
# Folium map builders
# ============================================================

def add_base_layers(m: folium.Map):
    folium.TileLayer("cartodbpositron", name="CartoDB Positron", show=True).add_to(m)
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap", show=False).add_to(m)
    folium.TileLayer("cartodbdark_matter", name="CartoDB Dark", show=False).add_to(m)


def add_css(m: folium.Map):
    css = """
    <style>
    .leaflet-tooltip {
        font-size: 20px !important;
        font-weight: 700 !important;
        line-height: 1.35 !important;
        padding: 10px 12px !important;
    }
    .leaflet-control-layers {
        font-size: 16px !important;
    }
    </style>
    """
    m.get_root().html.add_child(folium.Element(css))


def add_choropleth(m: folium.Map, df: pd.DataFrame, geo_level: str, group: str, selection: str):
    if df.empty:
        return

    codes = tuple(sorted(df["area_code"].dropna().astype(str).unique()))

    if geo_level == "LSOA":
        gj, code_field, name_field = lsoa_geojson_for_codes(codes)
    else:
        gj, code_field, name_field = lad_geojson_for_codes(codes)

    if not gj.get("features"):
        return

    colour = FOLIUM_COLOURS.get(group, "YlGnBu")

    layer_name = f"{group}: {selection}"

    folium.Choropleth(
        geo_data=gj,
        data=df,
        columns=["area_code", "value"],
        key_on=f"feature.properties.{code_field}",
        fill_color=colour,
        fill_opacity=0.72,
        line_opacity=0.8,
        line_weight=0.8,
        nan_fill_color="#f0f0f0",
        legend_name=layer_name,
        name=layer_name,
        show=True,
    ).add_to(m)

    lookup = df.set_index("area_code").to_dict(orient="index")

    hover_gj = {"type": "FeatureCollection", "features": []}
    for feat in gj.get("features", []):
        props0 = feat.get("properties", {})
        code = str(props0.get(code_field))
        row = lookup.get(code)
        if row is None:
            continue

        value = row.get("value")
        tooltip_props = {
            "Area": row.get("area_name") or props0.get(name_field) or code,
            "Value": f"{float(value):,.0f}" if pd.notna(value) else "NA",
            "Metric": row.get("metric", selection),
            "Geography": geo_level,
        }

        hover_gj["features"].append({
            "type": "Feature",
            "geometry": feat["geometry"],
            "properties": tooltip_props,
        })

    folium.GeoJson(
        hover_gj,
        name=f"Hover: {layer_name}",
        style_function=lambda x: {
            "fillOpacity": 0.0,
            "weight": 0.2,
            "color": "#111111",
        },
        highlight_function=lambda x: {
            "weight": 3,
            "color": "#111111",
            "fillOpacity": 0.12,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["Area", "Value", "Metric", "Geography"],
            aliases=["Area:", "Value:", "Metric:", "Level:"],
            sticky=True,
            labels=True,
            style=(
                "background-color:white; color:black; font-size:22px; "
                "font-weight:700; padding:10px; border:2px solid #444; border-radius:6px;"
            ),
        ),
    ).add_to(m)

    try:
        bounds = [shape(f["geometry"]).bounds for f in gj.get("features", [])]
        if bounds:
            minx = min(b[0] for b in bounds)
            miny = min(b[1] for b in bounds)
            maxx = max(b[2] for b in bounds)
            maxy = max(b[3] for b in bounds)
            m.fit_bounds([[miny, minx], [maxy, maxx]])
    except Exception:
        pass


def add_points(m: folium.Map, df: pd.DataFrame, group: str, selection: str):
    if df.empty or "lat" not in df.columns or "lon" not in df.columns:
        return

    cluster = MarkerCluster(name=f"{group}: {selection}").add_to(m)
    heat_points = []

    for _, row in df.iterrows():
        lat = float(row["lat"])
        lon = float(row["lon"])
        heat_points.append([lat, lon, 1])

        tooltip = (
            f"<b>{selection}</b><br>"
            f"Latitude: {lat:.5f}<br>"
            f"Longitude: {lon:.5f}"
        )

        folium.CircleMarker(
            location=[lat, lon],
            radius=5,
            color="#991b1b",
            weight=1,
            fill=True,
            fill_color="#fecdd3",
            fill_opacity=0.75,
            tooltip=tooltip,
        ).add_to(cluster)

    if heat_points:
        HeatMap(
            heat_points,
            name=f"Heatmap: {selection}",
            radius=18,
            blur=22,
            min_opacity=0.2,
            show=False,
        ).add_to(m)


def build_map(group: Optional[str], selection: Optional[str], area_filter: Optional[str] = None, geo_level: str = "LAD"):
    m = folium.Map(location=[52.7, -2.8], zoom_start=6, tiles=None)
    add_base_layers(m)
    add_css(m)

    if not group or not selection:
        folium.Marker(
            [52.7, -2.8],
            tooltip="Select one dataset from the sidebar.",
        ).add_to(m)
        folium.LayerControl(collapsed=False).add_to(m)
        return m.get_root().render(), [], "No dataset selected."

    df, geo_level = prepare_dataset(group, selection, geo_level=geo_level)

    if group == "Vehicles" and selection != "All" and df.empty:
        folium.Marker(
            [52.7, -2.8],
            tooltip=f"{selection} is not plotted yet. Wire a separate {selection} dataset first.",
        ).add_to(m)
        folium.LayerControl(collapsed=False).add_to(m)
        note = (
            f"Selected dataset: Vehicles → {selection}. "
            "This option is intentionally disabled until a separate matching dataset is wired. Vehicles → All uses Veh0105."
        )
        return m.get_root().render(), [], note

    if area_filter and "area_name" in df.columns:
        df = df[df["area_name"].astype(str).eq(str(area_filter))].copy()

    if geo_level == "POINT":
        add_points(m, df, group, selection)
    elif geo_level in {"LAD", "LSOA"}:
        add_choropleth(m, df, geo_level, group, selection)

    folium.LayerControl(collapsed=False).add_to(m)

    note = f"Selected dataset: {group} → {selection}. Geography: {geo_level}. Records: {len(df)}. Coverage: England and Wales LADs where present in Veh0105."

    if group == "Vehicles" and selection == "All" and geo_level == "LAD" and len(df) < 250:
        note += " Warning: fewer LADs than expected; check that the Veh0105 file contains all England and Wales LAD rows."

    return m.get_root().render(), df.to_dict("records"), note


# ============================================================
# UI
# ============================================================

def dataset_selector(group):
    return dcc.RadioItems(
        id=f"{group.lower()}-selection",
        options=layer_options[group],
        value=None,
        labelStyle={"display": "block", "marginBottom": "8px"},
        inputStyle={"marginRight": "8px"},
    )


sidebar = html.Div(
    [
        html.Img(
            src="/assets/cleets_logo.png",
            style={"width": "100%", "maxWidth": "300px", "display": "block", "margin": "0 auto"},
        ),

        html.Div(
            [
                html.H4(
                    "Electric Vehicle Equity Maps",
                    style={
                        "fontWeight": "bold",
                        "textAlign": "center",
                        "fontSize": "23px",
                        "marginBottom": "10px",
                        "color": "#003D7A",
                    },
                ),
                html.P(
                    "Please use this map to study battery electric vehicles trends across England and Wales.",
                    style={"textAlign": "left", "fontSize": "20px", "marginBottom": "0px", "color": "#333333"},
                ),
            ],
            style={
                "backgroundColor": "#E8F4FD",
                "border": "1px solid #B6D7F2",
                "borderRadius": "8px",
                "padding": "15px",
                "marginBottom": "20px",
                "boxShadow": "0 1px 3px rgba(0,0,0,0.1)",
            },
        ),

        html.Div(
            [
                html.Button("Download selected data", id="download-selected-btn", className="btn btn-primary btn-sm"),
                html.Button("Share map", id="share-map-btn", className="btn btn-secondary btn-sm", style={"marginLeft": "8px"}),
            ],
            style={"marginBottom": "12px"},
        ),

        dbc.Switch(
            id="geo-level-switch",
            label="LSOA view",
            value=False,
            style={"marginBottom": "12px"},
        ),

        dcc.Download(id="download-selected"),
        dcc.Download(id="download-map-html"),

        html.Hr(),

        html.Div(
            id="selected-dataset-display",
            children="No dataset selected.",
            style={
                "backgroundColor": "#F8FAFC",
                "border": "1px solid #CBD5E1",
                "borderRadius": "6px",
                "padding": "8px",
                "fontSize": "18px",
                "marginBottom": "10px",
            },
        ),

        dbc.Accordion(
            [
                dbc.AccordionItem(
                    [
                        html.Div(
                            "Select one dataset",
                            style={"fontWeight": "bold", "fontSize": "13px", "marginBottom": "8px"},
                        ),
                        dataset_selector(g),

                        html.Div(
                            f"Filter {g}",
                            style={
                                "fontWeight": "bold",
                                "fontSize": "16px",
                                "marginTop": "14px",
                                "marginBottom": "6px",
                            },
                        ),
                        dcc.Dropdown(
                            id=f"{g.lower()}-filter",
                            options=[],
                            placeholder=f"Select {g} area",
                            clearable=True,
                        ),
                    ],
                    title=g,
                )
                for g in groups
            ],
            always_open=True,
        ),

        html.Div(
            [
                html.Div("CLEETS Global Center", style={"fontWeight": "bold", "marginBottom": "6px"}),
                html.Div("Accessibility", style={"marginBottom": "4px"}),
                html.Div("Terms", style={"marginBottom": "4px"}),
                html.Div("License"),
            ],
            style={
                "backgroundColor": "#000000",
                "color": "#FFFFFF",
                "padding": "12px",
                "borderRadius": "6px",
                "marginTop": "14px",
                "lineHeight": "1.2",
            },
        ),

        dcc.Store(id="current-table-data", data=[]),
    ],
    style={"width": "520px", "height": "100vh", "overflowY": "auto", "padding": "16px"},
)


layout = html.Div(
    [
        sidebar,

        html.Div(
            [
                html.Iframe(
                    id="map-frame",
                    srcDoc="",
                    style={"width": "100%", "height": "78vh", "border": "0"},
                ),

                html.Div(
                    [
                        html.H5("Dataset Table"),
                        html.Div(
                            id="map-level-note",
                            style={"fontSize": "12px", "color": "#555", "marginBottom": "6px"},
                        ),
                        dash_table.DataTable(
                            id="data-table",
                            columns=[],
                            data=[],
                            page_size=10,
                            filter_action="native",
                            sort_action="native",
                            style_table={"height": "18vh", "overflowY": "auto", "overflowX": "auto"},
                            style_cell={
                                "fontSize": "18px",
                                "textAlign": "left",
                                "padding": "6px",
                                "minWidth": "120px",
                                "maxWidth": "260px",
                                "whiteSpace": "normal",
                            },
                            style_header={"fontWeight": "bold", "backgroundColor": "#E8F4FD"},
                        ),
                    ],
                    style={"padding": "10px"},
                ),
            ],
            style={"width": "100%"},
        ),
    ],
    style={"display": "flex"},
)


# ============================================================
# Callbacks
# ============================================================

def active_selection(output_value, vehicles_value, chargers_value, equity_value, comparisons_value):
    if output_value:
        return "Output", output_value
    if vehicles_value:
        return "Vehicles", vehicles_value
    if chargers_value:
        return "Chargers", chargers_value
    if equity_value:
        return "Equity", equity_value
    if comparisons_value:
        return "Comparisons", comparisons_value
    return None, None


@callback(
    Output("output-selection", "value"),
    Output("vehicles-selection", "value"),
    Output("chargers-selection", "value"),
    Output("equity-selection", "value"),
    Output("comparisons-selection", "value"),
    Input("output-selection", "value"),
    Input("vehicles-selection", "value"),
    Input("chargers-selection", "value"),
    Input("equity-selection", "value"),
    Input("comparisons-selection", "value"),
    prevent_initial_call=True,
)
def enforce_single_dataset(output_value, vehicles_value, chargers_value, equity_value, comparisons_value):
    trigger = ctx.triggered_id

    if trigger == "output-selection":
        return output_value, None, None, None, None
    if trigger == "vehicles-selection":
        return None, vehicles_value, None, None, None
    if trigger == "chargers-selection":
        return None, None, chargers_value, None, None
    if trigger == "equity-selection":
        return None, None, None, equity_value, None
    if trigger == "comparisons-selection":
        return None, None, None, None, comparisons_value

    return output_value, vehicles_value, chargers_value, equity_value, comparisons_value


@callback(
    Output("map-frame", "srcDoc"),
    Output("data-table", "data"),
    Output("data-table", "columns"),
    Output("vehicles-filter", "options"),
    Output("chargers-filter", "options"),
    Output("equity-filter", "options"),
    Output("comparisons-filter", "options"),
    Output("current-table-data", "data"),
    Output("map-level-note", "children"),
    Output("selected-dataset-display", "children"),
    Input("output-selection", "value"),
    Input("vehicles-selection", "value"),
    Input("chargers-selection", "value"),
    Input("equity-selection", "value"),
    Input("comparisons-selection", "value"),
    Input("vehicles-filter", "value"),
    Input("chargers-filter", "value"),
    Input("equity-filter", "value"),
    Input("comparisons-filter", "value"),
    Input("geo-level-switch", "value"),
)
def update_map(
    output_value,
    vehicles_value,
    chargers_value,
    equity_value,
    comparisons_value,
    vehicles_filter,
    chargers_filter,
    equity_filter,
    comparisons_filter,
    geo_switch,
):
    group, selection = active_selection(
        output_value,
        vehicles_value,
        chargers_value,
        equity_value,
        comparisons_value,
    )

    area_filter = {
        "Vehicles": vehicles_filter,
        "Chargers": chargers_filter,
        "Equity": equity_filter,
        "Comparisons": comparisons_filter,
    }.get(group)

    requested_geo_level = "LSOA" if geo_switch else "LAD"
    map_html, rows, note = build_map(group, selection, area_filter, geo_level=requested_geo_level)

    columns = [{"name": c, "id": c} for c in rows[0].keys()] if rows else []

    area_options = {}
    for row in rows:
        area = row.get("area_name")
        if area:
            area_options[str(area)] = {"label": str(area), "value": str(area)}

    area_options = sorted(area_options.values(), key=lambda x: x["label"])

    vehicles_options = area_options if group == "Vehicles" else []
    chargers_options = area_options if group == "Chargers" else []
    equity_options = area_options if group == "Equity" else []
    comparisons_options = area_options if group == "Comparisons" else []

    display = (
        html.Div([html.Strong("Selected dataset: "), html.Span(f"{group} → {selection}")])
        if group
        else "No dataset selected."
    )

    return (
        map_html,
        rows,
        columns,
        vehicles_options,
        chargers_options,
        equity_options,
        comparisons_options,
        rows,
        note,
        display,
    )


@callback(
    Output("download-selected", "data"),
    Input("download-selected-btn", "n_clicks"),
    State("current-table-data", "data"),
    prevent_initial_call=True,
)
def download_selected_data(n_clicks, rows):
    return {
        "content": pd.DataFrame(rows or []).to_csv(index=False),
        "filename": "selected_ev_dashboard_data.csv",
        "type": "text/csv",
    }


@callback(
    Output("download-map-html", "data"),
    Input("share-map-btn", "n_clicks"),
    State("map-frame", "srcDoc"),
    prevent_initial_call=True,
)
def share_map(n_clicks, map_html):
    return {
        "content": map_html or "",
        "filename": "shared_ev_equity_map.html",
        "type": "text/html",
    }


if __name__ == "__main__":
    app = Dash(__name__)
    app.layout = layout
    app.run_server(debug=True)