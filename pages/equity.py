from dash import Dash, html, dcc, Input, Output, State, ctx, callback
from dash import dash_table, register_page
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


register_page(__name__, path="/equity")


# Data sources:
# ---------------------------
# Vehicles Local Authority Districts (LADs) Level. 
VEH0105_URL = "https://drive.google.com/uc?export=download&id=1MqF57lLua8HSEFOYV0V2lnZmy5fiKGMP"        # All vehicles
VEH0141_URL = "https://drive.google.com/uc?export=download&id=1ubuFUSkL4Yqz1Dv8s5mkJttseXIxPfrZ"        # Plug-in 
VEH0132_URL = "https://drive.google.com/uc?export=download&id=1mGM0qG6MmH4bxvz8KL9NZuiJzPDqPFHi"        # Ultra Low Emissions


# Vehicles Lower Supper Output Area (LSOA) Level. 
VEH0125_URL = "https://drive.google.com/uc?export=download&id=1w-626GyUeVdULmB0aYhxImMnx6UuTbtq"        # All vehicles
VEH0145_URL = "https://drive.google.com/file/d/1jSc3swFMecXG7fOutp3CO-VzvaYpPDC5/view?usp=sharing"      # Plug-in
VEH0135_URL = "https://drive.google.com/uc?export=download&id=1i40mJbxIe65CTjzlouUZi48Ge3jDOlfa"        # Ultra Low Emissions


# Income deprivation Lower Supper Output Area (LSOA) Level(England and Wales)
WIMD_Wales = "https://drive.google.com/file/d/1K-PbySgovyzpFnnoY9exfDHpGXB1dLjT" # income deprivation Wales 
IMD_England =  "https://drive.google.com/file/d/1EXlkYrw--ueX1dzRSTUfzYxF5Gfk350W"  # income deprivation England                                                                  # The LSOA with a rank of 1 is the most deprived and the LSOA with a rank of 33,755 is the least deprived.

# Population England and Wales 

population = "https://drive.google.com/file/d/15kwuuNg6ZgdECrJDipg8b3H49cvi7vnS" # MYE2: Persons by single year of age and sex for local authorities in England and Wales, mid-2024						
population_density = "https://drive.google.com/file/d/1nSbbxY8_hvV47OnZwBWo3-N-HuMdmL_-" # MYE5: Population density for local authorities in England and Wales, mid-2011 to mid-2024				


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

# Soft ColorBrewer palettes supported by folium.Choropleth.
FOLIUM_COLOURS = {
    "Vehicles": "BuGn",
    "Chargers": "YlOrBr",
    "Equity": "YlGn",
    "Comparisons": "PuBu",
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
    """Download CSV/XLSX bytes, including Google Drive public-link confirm pages.

    Google Drive can return an HTML interstitial instead of the file. This
    function retries with the confirm token when possible and raises a clear
    message otherwise.
    """
    url = google_drive_direct_url(url)

    session = requests.Session()
    r = session.get(url, timeout=120)
    r.raise_for_status()

    ctype = (r.headers.get("Content-Type") or "").lower()
    head = r.content[:1000].lower()

    # Google Drive large-file / virus-scan confirmation page.
    if b"<html" in head and b"google" in head:
        token = None
        for key, value in r.cookies.items():
            if key.startswith("download_warning"):
                token = value
                break

        if token:
            r = session.get(url, params={"confirm": token}, timeout=120)
            r.raise_for_status()
            ctype = (r.headers.get("Content-Type") or "").lower()
            head = r.content[:1000].lower()

    if b"<html" in head and b"google" in head:
        raise RuntimeError(
            "Google Drive returned an HTML page instead of a data file. "
            "Please set the file permission to 'Anyone with the link can view', "
            "or replace this URL with a direct CSV/XLSX download link."
        )

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

def _load_clean(url: str) -> pd.DataFrame:
    df = load_data(url)
    df.columns = [str(c).strip() for c in df.columns]
    return df


# Vehicle datasets: LAD / Local Authority level.
@lru_cache(maxsize=4)
def load_veh0105_lad_df():
    # All vehicles
    return _load_clean(VEH0105_URL)


@lru_cache(maxsize=4)
def load_veh0141_lad_df():
    # Plug-in vehicles
    return _load_clean(VEH0141_URL)


@lru_cache(maxsize=4)
def load_veh0132_lad_df():
    # Ultra Low Emissions vehicles
    return _load_clean(VEH0132_URL)


# Vehicle datasets: LSOA level.
@lru_cache(maxsize=4)
def load_veh0125_lsoa_df():
    # All vehicles
    return _load_clean(VEH0125_URL)


@lru_cache(maxsize=4)
def load_veh0145_lsoa_df():
    # Plug-in vehicles
    return _load_clean(VEH0145_URL)


@lru_cache(maxsize=4)
def load_veh0135_lsoa_df():
    # Ultra Low Emissions vehicles
    return _load_clean(VEH0135_URL)


# Backwards-compatible aliases for the Vehicles panel only.
@lru_cache(maxsize=4)
def load_ev_lad_df():
    return load_veh0105_lad_df()


@lru_cache(maxsize=4)
def load_veh0105_df():
    return load_veh0105_lad_df()


@lru_cache(maxsize=4)
def load_ev_lsoa_df():
    return load_veh0125_lsoa_df()


# Non-vehicle panels are intentionally not mapped to vehicle files.
# Add real WIMD/charger/comparison URLs here when those datasets are available.
@lru_cache(maxsize=4)
def load_wimd_df():
    """Income deprivation sources for England and Wales at LSOA level."""
    frames = []
    for url in [WIMD_Wales, IMD_England]:
        try:
            frames.append(_load_clean(url))
        except Exception:
            pass
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


@lru_cache(maxsize=4)
def load_charge_df():
    return pd.DataFrame()


@lru_cache(maxsize=4)
def load_combined_df():
    return pd.DataFrame()


# ============================================================
# Dataset preparation
# ============================================================

def prepare_vehicle_data(selection: str, geo_level: str = "LAD") -> tuple[pd.DataFrame, str]:
    """Wire each Vehicles radio option to its corresponding LAD or LSOA source."""
    vehicle_sources = {
        "LAD": {
            "All": (load_veh0105_lad_df, "Veh0105 All vehicles"),
            "Plug-in": (load_veh0141_lad_df, "Veh0141 Plug-in vehicles"),
            "Ultra Low Emissions": (load_veh0132_lad_df, "Veh0132 Ultra Low Emissions vehicles"),
        },
        "LSOA": {
            "All": (load_veh0125_lsoa_df, "Veh0125 All vehicles"),
            "Plug-in": (load_veh0145_lsoa_df, "Veh0145 Plug-in vehicles"),
            "Ultra Low Emissions": (load_veh0135_lsoa_df, "Veh0135 Ultra Low Emissions vehicles"),
        },
    }

    if selection == "Battery Electric":
        return pd.DataFrame(), geo_level

    source = vehicle_sources.get(geo_level, vehicle_sources["LAD"]).get(selection)
    if source is None:
        return pd.DataFrame(), geo_level

    loader, metric_label = source
    df = loader().copy()

    if geo_level == "LSOA":
        code_pattern = r"^(E01|W01)\d+"
        code_candidates = [
            "ons code", "ons_code", "area code", "area_code",
            "lsoa code", "lsoa_code", "lsoa21cd", "lsoa11cd",
            "geography code", "geo code", "code"
        ]
        name_candidates = [
            "ons geography", "ons_geography", "area name", "area_name",
            "lsoa name", "lsoa21nm", "lsoa11nm", "geography", "name"
        ]
    else:
        geo_level = "LAD"
        code_pattern = r"^(E06|E07|E08|E09|W06)\d+"
        code_candidates = [
            "ons code", "ons_code", "area code", "area_code",
            "local authority code", "lad code", "lad_code", "geography code", "code"
        ]
        name_candidates = [
            "ons geography", "ons_geography", "area name", "area_name",
            "local authority", "lad name", "lad_name", "geography", "name"
        ]

    code_col = pick_col(df.columns, code_candidates) or first_matching_code_column(df, code_pattern)
    name_col = pick_col(df.columns, name_candidates)
    quarter_col = latest_quarter_col(df)

    if code_col is None or quarter_col is None:
        return pd.DataFrame(), geo_level

    out = df[[code_col] + ([name_col] if name_col else []) + [quarter_col]].copy()
    out.columns = ["area_code"] + (["area_name"] if name_col else []) + ["value"]

    if "area_name" not in out.columns:
        out["area_name"] = out["area_code"]

    out["area_code"] = out["area_code"].astype(str).str.strip()
    out["area_name"] = out["area_name"].astype(str).str.strip()
    out["value"] = out["value"].apply(parse_num)
    out = out[out["area_code"].str.match(code_pattern, na=False)].dropna(subset=["value"])

    if out.empty:
        return pd.DataFrame(), geo_level

    agg = "sum" if selection in {"All", "Plug-in", "Ultra Low Emissions"} else "mean"
    out = out.groupby(["area_code", "area_name"], as_index=False)["value"].agg(agg)
    out["metric"] = f"{metric_label} ({quarter_col})"
    out["geography"] = geo_level

    # LSOA boundary rendering is heavy; cap rows for responsiveness.
    if geo_level == "LSOA":
        out = out.head(2500)

    return out, geo_level

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
    out = out[out["area_code"].str.match(r"^(E01|W01)", na=False)].dropna(subset=["value"])

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


def prepare_comparison_data(selection: str, geo_level: str = "LAD") -> tuple[pd.DataFrame, str]:
    """Wire comparison radio options to derived datasets where source files exist."""
    if selection == "Income Deprivation":
        return prepare_wimd_lsoa("income_deprivation")

    if selection == "Battery Electric (%)":
        return pd.DataFrame(), geo_level

    all_df, geo = prepare_vehicle_data("All", geo_level=geo_level)
    if selection in {"Percent (%)", "Plug-in (%)"}:
        part_df, geo = prepare_vehicle_data("Plug-in", geo_level=geo_level)
        metric_name = "Plug-in vehicles as percentage of all vehicles"
    elif selection == "Ultra Low Emissions (%)":
        part_df, geo = prepare_vehicle_data("Ultra Low Emissions", geo_level=geo_level)
        metric_name = "Ultra Low Emissions vehicles as percentage of all vehicles"
    else:
        return pd.DataFrame(), geo_level

    if all_df.empty or part_df.empty:
        return pd.DataFrame(), geo

    base = all_df[["area_code", "area_name", "value"]].rename(columns={"value": "all_value"})
    part = part_df[["area_code", "value"]].rename(columns={"value": "part_value"})
    out = base.merge(part, on="area_code", how="inner")
    out["value"] = np.where(out["all_value"] > 0, (out["part_value"] / out["all_value"]) * 100, np.nan)
    out = out.dropna(subset=["value"])[["area_code", "area_name", "value"]]
    out["metric"] = metric_name
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
        return prepare_comparison_data(selection, geo_level=geo_level)

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


def _colour_ramp_for_group(group: str) -> str:
    """Approximate the selected Folium ColorBrewer palette as a CSS gradient."""
    ramps = {
        "Vehicles": "linear-gradient(to right, #f7fcfd, #e5f5f9, #ccece6, #99d8c9, #66c2a4)",
        "Chargers": "linear-gradient(to right, #ffffe5, #fff7bc, #fee391, #fec44f, #fe9929)",
        "Equity": "linear-gradient(to right, #ffffe5, #f7fcb9, #d9f0a3, #addd8e, #78c679)",
        "Comparisons": "linear-gradient(to right, #fff7fb, #ece7f2, #d0d1e6, #a6bddb, #74a9cf)",
    }
    return ramps.get(group, ramps["Vehicles"])


def add_hover_value_legend(m: folium.Map, df: pd.DataFrame, group: str, selection: str, hover_layer_name: str):
    """Add an ONS-style top-right legend whose marker moves on feature hover."""
    if df.empty or "value" not in df.columns:
        return

    vals = pd.to_numeric(df["value"], errors="coerce").dropna()
    if vals.empty:
        return

    vmin = float(vals.min())
    vmax = float(vals.max())
    vmean = float(vals.mean())
    initial_pct = 50.0 if vmax == vmin else max(0.0, min(100.0, ((vmean - vmin) / (vmax - vmin)) * 100.0))
    metric = str(df["metric"].iloc[0]) if "metric" in df.columns and len(df) else selection
    ramp = _colour_ramp_for_group(group)

    html_block = f"""
    <div id="ons-hover-legend" style="
        position: fixed;
        bottom: 86px;
        left: 20px;
        z-index: 9999;
        width: 400px;
        background: rgba(255,255,255,0.96);
        border: 1px solid #b1b4b6;
        box-shadow: 0 2px 6px rgba(0,0,0,0.22);
        padding: 12px 14px 10px 14px;
        font-family: Arial, sans-serif;
        color: #0b0c0c;
        pointer-events: none;
    ">
        <div style="font-size:14px; font-weight:700; margin-bottom:2px;">{group}: {selection}</div>
        <div style="font-size:12px; margin-bottom:10px; color:#505a5f; line-height:1.25;">{metric}</div>
        <div id="ons-hover-area" style="font-size:13px; font-weight:700; min-height:17px; margin-bottom:4px;">Hover over an area</div>
        <div id="ons-hover-value" style="font-size:18px; font-weight:800; margin-bottom:6px;">Mean: {vmean:,.0f}</div>
        <div style="position:relative; height:30px; margin:0 2px 3px 2px;">
            <div style="position:absolute; left:0; right:0; top:12px; height:12px; background:{ramp}; border:1px solid #6b7280;"></div>
            <div id="ons-hover-marker" style="position:absolute; left:{initial_pct:.2f}%; top:0; transform:translateX(-50%); width:3px; height:28px; background:#0b0c0c; box-shadow:0 0 0 1px rgba(255,255,255,0.9);"></div>
        </div>
        <div style="display:flex; justify-content:space-between; font-size:12px; color:#0b0c0c;">
            <span>{vmin:,.0f}</span><span>{vmax:,.0f}</span>
        </div>
    </div>

    <script>
    (function() {{
        var vmin = {vmin};
        var vmax = {vmax};
        var vmean = {vmean};
        var initialPct = {initial_pct};
        var marker = document.getElementById('ons-hover-marker');
        var valueBox = document.getElementById('ons-hover-value');
        var areaBox = document.getElementById('ons-hover-area');

        function fmt(x) {{
            if (x === null || x === undefined || isNaN(x)) return 'NA';
            return Number(x).toLocaleString(undefined, {{maximumFractionDigits: 0}});
        }}
        function pct(x) {{
            if (vmax === vmin) return 50;
            return Math.max(0, Math.min(100, ((x - vmin) / (vmax - vmin)) * 100));
        }}
        function setLegend(area, value) {{
            if (!marker || !valueBox || !areaBox) return;
            marker.style.left = pct(value) + '%';
            areaBox.textContent = area || 'Selected area';
            valueBox.textContent = 'Value: ' + fmt(value);
        }}
        function resetLegend() {{
            if (!marker || !valueBox || !areaBox) return;
            marker.style.left = initialPct + '%';
            areaBox.textContent = 'Hover over an area';
            valueBox.textContent = 'Mean: ' + fmt(vmean);
        }}
        setTimeout(function() {{
            var hoverLayer = {hover_layer_name};
            if (!hoverLayer || !hoverLayer.eachLayer) return;
            hoverLayer.eachLayer(function(layer) {{
                layer.on('mouseover', function(e) {{
                    var props = e.target && e.target.feature ? e.target.feature.properties : {{}};
                    setLegend(props.Area, Number(props.ValueRaw));
                }});
                layer.on('mouseout', resetLegend);
            }});
        }}, 0);
    }})();
    </script>
    """
    m.get_root().html.add_child(folium.Element(html_block))

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

# ------------------------------------------------------------
# Choropleth layer
# ------------------------------------------------------------
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
        # legend_name="",
        name=layer_name,
        show=True,
    ).add_to(m)

    lookup = df.set_index("area_code").to_dict(orient="index") # I commented out the hover because the bar displayed with the slider is enough, unless suggested otherwise. 

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
            "ValueRaw": float(value) if pd.notna(value) else None,
            "Metric": row.get("metric", selection),
            "Geography": geo_level,
        }

        hover_gj["features"].append({
            "type": "Feature",
            "geometry": feat["geometry"],
            "properties": tooltip_props,
        })

    hover_layer = folium.GeoJson(
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

    add_hover_value_legend(m, df, group, selection, hover_layer.get_name())

    try:
        bounds = [shape(f["geometry"]).bounds for f in gj.get("features", [])]
        if bounds:
            minx = min(b[0] for b in bounds)
            miny = min(b[1] for b in bounds)
            maxx = max(b[2] for b in bounds)
            maxy = max(b[3] for b in bounds)
            # Use a wider overview for LAD/LA and a closer view for LSOA.
            if geo_level == "LSOA":
                m.fit_bounds([[miny, minx], [maxy, maxx]], padding=(5, 5), max_zoom=12)
            else:
                m.fit_bounds([[miny, minx], [maxy, maxx]], padding=(20, 20), max_zoom=8)
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
    def make_base_map(level: str):
        # LAD/LA: low zoom national/regional overview. LSOA: higher zoom local detail.
        if level == "LSOA":
            return folium.Map(location=[52.7, -2.8], zoom_start=10, min_zoom=8, tiles=None)
        return folium.Map(location=[52.7, -2.8], zoom_start=6, min_zoom=5, tiles=None)

    if not group or not selection:
        m = make_base_map("LAD")
        add_base_layers(m)
        add_css(m)
        folium.Marker(
            [52.7, -2.8],
            tooltip="Select one dataset from the sidebar.",
        ).add_to(m)
        folium.LayerControl(collapsed=False).add_to(m)
        return m.get_root().render(), [], "No dataset selected."

    try:
        df, geo_level = prepare_dataset(group, selection, geo_level=geo_level)
    except Exception as e:
        m = make_base_map(geo_level)
        add_base_layers(m)
        add_css(m)
        msg = f"Could not load {group} → {selection}: {e}"
        folium.Marker([52.7, -2.8], tooltip=msg).add_to(m)
        folium.LayerControl(collapsed=False).add_to(m)
        return m.get_root().render(), [], msg

    m = make_base_map(geo_level)
    add_base_layers(m)
    add_css(m)

    if group == "Vehicles" and selection == "Battery Electric" and df.empty:
        folium.Marker(
            [52.7, -2.8],
            tooltip="Battery Electric is not wired because no matching BEV-only source URL is defined.",
        ).add_to(m)
        folium.LayerControl(collapsed=False).add_to(m)
        note = "Selected dataset: Vehicles → Battery Electric. No matching BEV-only source URL is currently defined."
        return m.get_root().render(), [], note

    if group in {"Equity", "Chargers", "Comparisons"} and df.empty:
        folium.Marker(
            [52.7, -2.8],
            tooltip=f"{group} → {selection} has no source URL wired yet.",
        ).add_to(m)
        folium.LayerControl(collapsed=False).add_to(m)
        note = f"Selected dataset: {group} → {selection}. No matching source URL is currently defined, so this panel is not mapped to a vehicle dataset."
        return m.get_root().render(), [], note

    if area_filter and "area_name" in df.columns:
        df = df[df["area_name"].astype(str).eq(str(area_filter))].copy()

    if geo_level == "POINT":
        add_points(m, df, group, selection)
    elif geo_level in {"LAD", "LSOA"}:
        add_choropleth(m, df, geo_level, group, selection)

    folium.LayerControl(collapsed=False).add_to(m)

    note = f"Selected dataset: {group} → {selection}. Geography: {geo_level}. Records: {len(df)}. Coverage: England and Wales where matching geography codes are present in the selected dataset."

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
                    "Electric Vehicle Equity Mapping Dashboard",
                    style={
                        "fontWeight": "bold",
                        "textAlign": "center",
                        "fontSize": "23px",
                        "marginBottom": "10px",
                        "color": "#003D7A",
                    },
                ),
                html.P(
                    "Use this dashboard to explore electric vehicle uptake, equity indicators, and spatial patterns across England and Wales.",
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

        dcc.Loading(
            id="main-loading",
            type="circle",
            fullscreen=False,
            parent_style={
                "width": "100%",
                "position": "relative",
                "minHeight": "100vh",
            },
            style={
                "position": "absolute",
                "top": "50%",
                "left": "50%",
                "transform": "translate(-50%, -50%) scale(2.4)",
                "zIndex": 10000,
            },
            children=html.Div(
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


