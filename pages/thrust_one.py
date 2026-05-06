# ============================================================
# CLEETS-SMART Dashboard B: Thrust One (BEV + WIMD + Charging)
# ============================================================
# Dash page that renders the Folium map from thrust_one.py in an iframe,
# with controls similar to the Weather Forecaster dashboard.
# ============================================================

from __future__ import annotations

import io
import re
import time
import warnings
from functools import lru_cache
from typing import Dict, Optional

import numpy as np
import pandas as pd
import requests
import folium
from folium.plugins import HeatMap, MarkerCluster
from branca.element import Template, MacroElement

from dash import html, dcc, Input, Output, State, callback, register_page

warnings.filterwarnings("ignore")

# ---------------------------
# Data sources
# ---------------------------
DATA_SOURCE = "https://drive.google.com/uc?export=download&id=1nEq37_ZILPI3GIj2QyBXfJ390iJadjur" # EV population
DATA_SOURCE_LSOA = "https://drive.google.com/uc?export=download&id=1wM0pxGBn67vCDpLdZmeyGYZgq59IsoVU" # LSOA EV keepership
WIMD_URL = "https://drive.google.com/uc?export=download&id=1WHn5mXXJrndHhESXDWZk_QAA9Tp4XyiC" # Income Deprivation
CHARGE_URL = "https://drive.google.com/uc?export=download&id=1FLnVRHaKya7nKd1FgObPSeuTKV1zJ3Q_" # EV charger Count UK
EV_COUNTS_URL = "https://drive.google.com/uc?export=download&id=1FLnVRHaKya7nKd1FgObPSeuTKV1zJ3Q_" # EV charger Count UK
COMBINED_DATASET_URL = "https://drive.google.com/uc?export=download&id=14Mdz9M1xcIApHxawggGdxQBPnfeosF8A" # Combined dataset

# ONS lookup: LSOA21 -> LTLA22 (used if WIMD is LSOA-level)
LSOA_TO_LAD_FS = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
    "LSOA_2021_to_Ward_to_Lower_Tier_Local_Authority_May_2022_Lookup_for_England_2022/FeatureServer"
)
LSOA_TO_LAD_LAYER = "0"

# ONS lookup: LSOA21 -> Westminster Parliamentary Constituency July 2024 (best fit; England & Wales)
LSOA_TO_PCON_FS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA21_PCON24_LAD21_EW_LU/FeatureServer"
LSOA_TO_PCON_LAYER = "0"

# LAD boundaries
LAD_FS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Authority_Districts_May_2024_Boundaries_UK_BGC/FeatureServer"
LAD_LAYER = "0"

# LSOA boundaries (England & Wales LSOA 2021 BFE)
LSOA_FS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_2021_EW_BFE_V10_RUC/FeatureServer"
LSOA_LAYER = "3"

# Westminster Parliamentary Constituencies boundaries (July 2024, UK BGC)
PCON_FS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Westminster_Parliamentary_Constituencies_July_2024_Boundaries_UK_BGC/FeatureServer"
PCON_LAYER = "0"

# Register as a page (like weather.py)
register_page(__name__, path="/thrust_one")


def back_button():
    return html.Div(
        children=[
            html.A(
                "← Back to Home",
                href="/",
                style={
                    "textDecoration": "none",
                    "fontWeight": "600",
                    "padding": "8px 14px",
                    "border": "1px solid #ccc",
                    "borderRadius": "8px",
                    "backgroundColor": "#f8f9fa",
                    "color": "#333",
                    "boxShadow": "0 1px 3px rgba(0,0,0,0.12)",
                },
            )
        ],
        style={
            "position": "absolute",
            "top": "20px",
            "right": "30px",
            "zIndex": "1000",
        },
    )

# ---------------------------
# Helpers
# ---------------------------
def google_drive_direct_url(url: str) -> str:
    """Convert Google Drive share/preview links into direct-download URLs."""
    m = re.search(r"drive\.google\.com/file/d/([^/]+)", str(url))
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    return str(url).replace("/view?usp=drive_link", "").replace("/view?usp=sharing", "")


def _download(url: str) -> tuple[bytes, str]:
    url = google_drive_direct_url(url)
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    content_type = (r.headers.get("Content-Type") or "").lower()
    return r.content, content_type


def load_data(path_or_url: str) -> pd.DataFrame:
    lower = path_or_url.lower()

    if not lower.startswith("http"):
        if lower.endswith(".csv"):
            return pd.read_csv(path_or_url)
        if lower.endswith(".tsv") or lower.endswith(".txt"):
            return pd.read_csv(path_or_url, sep="\t")
        if lower.endswith(".xlsx") or lower.endswith(".xls"):
            return pd.read_excel(path_or_url)
        if lower.endswith(".parquet"):
            return pd.read_parquet(path_or_url)
        raise ValueError(f"Unsupported local file type: {path_or_url}")

    data, content_type = _download(path_or_url)

    if "spreadsheetml" in content_type or "ms-excel" in content_type:
        return pd.read_excel(io.BytesIO(data))

    if "text/csv" in content_type or content_type.startswith("text/") or "csv" in content_type:
        try:
            return pd.read_csv(io.BytesIO(data))
        except Exception:
            return pd.read_csv(io.BytesIO(data), sep="\t")

    try:
        return pd.read_excel(io.BytesIO(data))
    except Exception:
        return pd.read_csv(io.BytesIO(data))


def pick_col(cols, candidates):
    low = {str(c).strip().lower(): c for c in cols}
    for cand in candidates:
        if cand in low:
            return low[cand]
    return None


def to_int(x):
    """Parse integer-like published counts safely.

    DfT/DVLA tables can contain disclosure-control or suppression tokens
    such as [z], [c], [x], '..', '-', etc. Treat these as missing rather
    than raising an exception during Dash page import.
    """
    if pd.isna(x):
        return np.nan
    s = str(x).replace(",", "").strip()
    if not s:
        return np.nan
    if s.lower() in {"[z]", "[c]", "[x]", "[s]", "..", ".", "-", "—", "na", "n/a", "null", "none"}:
        return np.nan
    try:
        return int(float(s))
    except Exception:
        return np.nan


def row_to_html_table(row: pd.Series) -> str:
    cols = [c for c in row.index if c not in {"__lat", "__lon"}]
    rows = []
    for c in cols:
        v = row.get(c)
        if pd.isna(v):
            v = ""
        rows.append(
            f"<tr><th style='text-align:right; padding:2px 6px;'>{c}</th>"
            f"<td style='padding:2px 6px;'>{str(v)}</td></tr>"
        )
    return (
        "<div style='max-width:420px; max-height:260px; overflow:auto;'>"
        "<table style='border-collapse:collapse; width:100%; font-size:18px;'>"
        + "".join(rows) +
        "</table></div>"
    )


def parse_num(x):
    # Robust numeric parsing (commas, %, suppression tokens),
    # and NEVER treat geography codes (W06000001 etc.) as numbers.
    if pd.isna(x):
        return np.nan
    s = str(x).strip()

    # avoid parsing geography codes as numbers (W06..., W01..., E01..., etc.)
    if re.match(r"^[A-Z]\d{2}\d{5,}$", s):
        return np.nan

    if s == "" or s.lower() in {"na", "n/a", "null", "none", "not available"}:
        return np.nan
    if s in {"[z]", "[x]", "..", ".", "*", "-", "—"}:
        return np.nan

    s2 = s.replace(",", "").replace("%", "").strip()
    m = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s2)
    if not m:
        return np.nan
    try:
        return float(m.group(0))
    except Exception:
        return np.nan




def wimd_rank_to_decile(values_by_code: Dict[str, float]) -> Dict[str, int]:
    """Convert WIMD ranks to ordered deprivation deciles.

    Decile 1 = the most deprived 10% within the mapped geography/domain;
    Decile 10 = the least deprived 10%. Lower WIMD ranks are more deprived.
    The calculation uses ordinal rank position, not numeric distance between ranks.
    """
    clean = {k: float(v) for k, v in values_by_code.items() if v is not None and np.isfinite(v)}
    if not clean:
        return {}
    ordered = sorted(clean.items(), key=lambda kv: kv[1])
    n = len(ordered)
    out: Dict[str, int] = {}
    for i, (code, _rank_value) in enumerate(ordered, start=1):
        decile = int(np.ceil(i * 10 / n))
        out[code] = max(1, min(10, decile))
    return out

def arcgis_pjson(url: str) -> dict:
    r = requests.get(url, params={"f": "pjson"}, timeout=60)
    r.raise_for_status()
    out = r.json()
    if isinstance(out, dict) and "error" in out:
        raise RuntimeError(f"ArcGIS error at {url}: {out['error']}")
    return out


def _arcgis_request(method: str, url: str, *, params: dict, timeout: int = 120, retries: int = 4) -> requests.Response:
    """ArcGIS Online is occasionally flaky (502/503/504). We retry a few times."""
    last_exc = None
    for i in range(retries):
        try:
            if method.upper() == "POST":
                r = requests.post(url, data=params, timeout=timeout)
            else:
                r = requests.get(url, params=params, timeout=timeout)
            # Retry transient gateway/service errors
            if r.status_code in {502, 503, 504}:
                time.sleep(1.2 * (i + 1))
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            time.sleep(1.2 * (i + 1))
    raise last_exc


def arcgis_query_geojson(fs: str, layer: str, where: str, out_fields: str, page: int = 2000) -> dict:
    """Paged GeoJSON query (POST to avoid URL-length limits)."""
    feats, offset = [], 0
    url = f"{fs}/{layer}/query"
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
        r = _arcgis_request("POST", url, params=params, timeout=180)
        out = r.json()
        if "error" in out:
            raise RuntimeError(f"ArcGIS query error: {out['error']}")
        batch = out.get("features", [])
        feats.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return {"type": "FeatureCollection", "features": feats}


def arcgis_query_geojson_in_chunks(
    fs: str, layer: str, field: str, values: list[str], out_fields: str, *,
    chunk_size: int = 200,
) -> dict:
    """GeoJSON query for a long IN (...) list, chunked to avoid huge payloads."""
    all_feats = []
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i + chunk_size]
        where = sql_in(field, chunk)
        gj = arcgis_query_geojson(fs, layer, where=where, out_fields=out_fields)
        all_feats.extend(gj.get("features", []))
    return {"type": "FeatureCollection", "features": all_feats}


def arcgis_query_table(fs: str, layer: str, where: str, out_fields: str, page: int = 2000) -> pd.DataFrame:
    rows, offset = [], 0
    url = f"{fs}/{layer}/query"
    while True:
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": page,
        }
        r = _arcgis_request("POST", url, params=params, timeout=120)
        out = r.json()
        if "error" in out:
            raise RuntimeError(f"ArcGIS query error: {out['error']}")
        feats = out.get("features", [])
        rows.extend([f.get("attributes", {}) for f in feats])
        if len(feats) < page:
            break
        offset += page
    return pd.DataFrame(rows)


def sql_in(field, values):
    esc = [str(v).replace("'", "''") for v in values]
    quoted = ["'" + v + "'" for v in esc]
    return f"{field} IN ({', '.join(quoted)})"


def pick_field(fields, cands):
    low = {c.lower(): c for c in fields}
    for k in cands:
        if k in low:
            return low[k]
    return None


@lru_cache(maxsize=4)
def load_bev_lad_df() -> pd.DataFrame:
    df = load_data(DATA_SOURCE)
    df.columns = [str(c).strip() for c in df.columns]
    return df


@lru_cache(maxsize=4)
def load_bev_lsoa_df() -> pd.DataFrame:
    df = load_data(DATA_SOURCE_LSOA)
    df.columns = [str(c).strip() for c in df.columns]
    return df


@lru_cache(maxsize=4)
def load_wimd_df() -> pd.DataFrame:
    wimd = load_data(WIMD_URL)
    wimd.columns = [str(c).strip() for c in wimd.columns]
    return wimd


@lru_cache(maxsize=2)
def load_charge_df() -> pd.DataFrame:
    cps = load_data(CHARGE_URL)
    cps.columns = [str(c).strip() for c in cps.columns]
    return cps


@lru_cache(maxsize=2)
def load_ev_counts_df() -> pd.DataFrame:
    df = load_data(EV_COUNTS_URL)
    df.columns = [str(c).strip() for c in df.columns]
    return df


@lru_cache(maxsize=2)
def lad_geojson_for_codes(codes_tuple: tuple[str, ...]) -> tuple[dict, str, Optional[str]]:
    lad_meta = arcgis_pjson(f"{LAD_FS}/{LAD_LAYER}")
    lad_fields = [f["name"] for f in lad_meta.get("fields", [])]

    geo_code_field = pick_field(lad_fields, ["lad24cd", "lad23cd", "lad22cd", "lad21cd", "lad20cd", "lad19cd", "lad18cd", "ladcd"])
    geo_name_field = pick_field(lad_fields, ["lad24nm", "lad23nm", "lad22nm", "lad21nm", "lad20nm", "ladnm"])
    if geo_code_field is None:
        raise ValueError(f"Couldn't infer LAD code field. LAD fields include: {lad_fields[:30]} ...")

    gj = arcgis_query_geojson(
        LAD_FS,
        LAD_LAYER,
        sql_in(geo_code_field, list(codes_tuple)),
        out_fields=",".join([geo_code_field] + ([geo_name_field] if geo_name_field else [])),
    )
    return gj, geo_code_field, geo_name_field

@lru_cache(maxsize=4)
def lsoa_geojson_for_codes(codes_tuple: tuple[str, ...]) -> tuple[dict, str, Optional[str]]:
    lsoa_meta = arcgis_pjson(f"{LSOA_FS}/{LSOA_LAYER}")
    lsoa_fields = [f["name"] for f in lsoa_meta.get("fields", [])]

    lsoa_code_field = pick_field(lsoa_fields, ["lsoa21cd", "lsoa11cd", "lsoacd"])
    lsoa_name_field = pick_field(lsoa_fields, ["lsoa21nm", "lsoa11nm", "lsoanm"]) or lsoa_code_field
    if lsoa_code_field is None:
        raise ValueError(f"Couldn't infer LSOA code field. LSOA fields include: {lsoa_fields[:30]} ...")

    gj = arcgis_query_geojson_in_chunks(
        LSOA_FS,
        LSOA_LAYER,
        field=lsoa_code_field,
        values=list(codes_tuple),
        out_fields=",".join([lsoa_code_field] + ([lsoa_name_field] if lsoa_name_field else [])),
        chunk_size=200,
    )
    return gj, lsoa_code_field, lsoa_name_field


@lru_cache(maxsize=4)
def constituency_geojson_for_codes(codes_tuple: tuple[str, ...]) -> tuple[dict, str, Optional[str]]:
    pcon_meta = arcgis_pjson(f"{PCON_FS}/{PCON_LAYER}")
    pcon_fields = [f["name"] for f in pcon_meta.get("fields", [])]

    pcon_code_field = pick_field(pcon_fields, ["pcon24cd", "pcon23cd", "pcon22cd", "pconcd"])
    pcon_name_field = pick_field(pcon_fields, ["pcon24nm", "pcon23nm", "pcon22nm", "pconnm"])
    if pcon_code_field is None:
        raise ValueError(f"Couldn't infer constituency code field. Fields include: {pcon_fields[:40]} ...")

    gj = arcgis_query_geojson_in_chunks(
        PCON_FS,
        PCON_LAYER,
        field=pcon_code_field,
        values=list(codes_tuple),
        out_fields=",".join([pcon_code_field] + ([pcon_name_field] if pcon_name_field else [])),
        chunk_size=200,
    )
    return gj, pcon_code_field, pcon_name_field


def _first_matching_code_column(df: pd.DataFrame, pattern: str) -> Optional[str]:
    for col in df.columns:
        s = df[col].astype(str).str.strip()
        if s.str.match(pattern, na=False).sum() > 0:
            return col
    return None


def prepare_constituency_ev_counts(ev_counts_df: pd.DataFrame) -> tuple[Dict[str, float], Dict[str, str], Optional[str]]:
    """Return EV charger counts keyed by Westminster parliamentary constituency code."""
    evc = ev_counts_df.copy()
    evc.columns = [str(c).strip() for c in evc.columns]

    code_col = pick_col(evc.columns, [
        "parliamentary constituency code", "constituency code", "pcon24cd",
        "pcon code", "pcon_code", "westminster parliamentary constituency code",
        "area code", "geography code", "ons code", "code",
    ]) or _first_matching_code_column(evc, r"^(E14|W07|S14|N06)\d+")

    name_col = pick_col(evc.columns, [
        "parliamentary constituency", "constituency", "pcon24nm", "pcon name",
        "pcon_name", "westminster parliamentary constituency", "area name",
        "geography", "geography name", "name",
    ])

    key_col = pick_col(evc.columns, ["key", "metric", "measure", "indicator", "variable"])
    value_col = pick_col(evc.columns, [
        "value", "count", "ev chargers", "ev charger count", "number of devices",
        "devices", "charging devices", "public charging devices",
    ])
    date_col = pick_col(evc.columns, ["date", "period", "reference date", "month", "quarter"])

    if code_col is None:
        raise ValueError(f"Could not find a parliamentary constituency code column in EV counts. Columns: {evc.columns.tolist()}")

    evc[code_col] = evc[code_col].astype(str).str.strip()
    evc = evc[evc[code_col].str.match(r"^(E14|W07|S14|N06)\d+", na=False)].copy()
    if evc.empty:
        raise ValueError("EV counts file has no rows with 2024 parliamentary constituency codes (expected E14/W07/S14/N06 prefixes).")

    if key_col is not None:
        evc[key_col] = evc[key_col].astype(str).str.strip()
        filtered = evc[evc[key_col].str.lower().str.contains("charger|device", regex=True, na=False)].copy()
        if not filtered.empty:
            evc = filtered

    if value_col is None:
        candidates = []
        for col in evc.columns:
            if col in {code_col, name_col, key_col, date_col}:
                continue
            vals = evc[col].apply(parse_num)
            if vals.notna().sum() > 0:
                candidates.append((vals.notna().sum(), col))
        if not candidates:
            raise ValueError(f"Could not find a numeric EV charger count column. Columns: {evc.columns.tolist()}")
        value_col = sorted(candidates, reverse=True)[0][1]

    evc["__value_num"] = evc[value_col].apply(parse_num)
    evc = evc.dropna(subset=["__value_num"]).copy()

    date_label = None
    if date_col is not None:
        evc["__date"] = pd.to_datetime(evc[date_col], errors="coerce", dayfirst=True)
        if evc["__date"].notna().any():
            latest_date = evc["__date"].max()
            evc = evc[evc["__date"].eq(latest_date)].copy()
            date_label = latest_date.strftime("%d %B %Y")

    counts = evc.groupby(code_col)["__value_num"].sum().to_dict()
    names = {}
    if name_col is not None:
        names = evc.dropna(subset=[name_col]).drop_duplicates(code_col).set_index(code_col)[name_col].astype(str).to_dict()
    return counts, names, date_label




@lru_cache(maxsize=4)
def lsoa_to_pcon_lookup_df() -> pd.DataFrame:
    lookup = arcgis_query_table(
        LSOA_TO_PCON_FS,
        LSOA_TO_PCON_LAYER,
        where="LSOA21CD LIKE 'E01%' OR LSOA21CD LIKE 'W01%'",
        out_fields="LSOA21CD,PCON24CD,PCON24NM",
    )
    lookup.columns = [str(c).strip() for c in lookup.columns]
    required = {"LSOA21CD", "PCON24CD"}
    missing = required - set(lookup.columns)
    if missing:
        raise ValueError(f"LSOA to PCON lookup missing columns: {missing}. Found: {lookup.columns.tolist()}")
    lookup["LSOA21CD"] = lookup["LSOA21CD"].astype(str).str.strip()
    lookup["PCON24CD"] = lookup["PCON24CD"].astype(str).str.strip()
    if "PCON24NM" in lookup.columns:
        lookup["PCON24NM"] = lookup["PCON24NM"].astype(str).str.strip()
    return lookup.drop_duplicates("LSOA21CD")


def prepare_constituency_bev_counts(quarter: str) -> tuple[Dict[str, float], Dict[str, str]]:
    df = load_bev_lsoa_df().copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Robust LSOA code detection. The source files have appeared with several
    # different names, including ONS Code, Area code, and LSOA21CD.
    code_col = pick_col(df.columns, [
        "ons code", "ons_code", "onscode",
        "area code", "area_code", "areacode",
        "lsoa code", "lsoa_code", "lsoacode",
        "lsoa21cd", "lsoa 2021 code", "lower layer super output area code",
        "geography code", "geography_code", "geo code", "geo_code",
    ])

    if code_col is None:
        # Last-resort inference: choose the first column containing E01/W01 LSOA codes.
        for c in df.columns:
            sample = df[c].dropna().astype(str).str.strip().head(250)
            if sample.str.match(r"^(E01|W01)\d+", na=False).any():
                code_col = c
                break

    if code_col is None:
        raise ValueError(f"Could not detect LSOA code column in the LSOA BEV dataset. Found columns: {df.columns.tolist()}")

    # Robust quarter detection: accepts both '2025 Q4' and '2025Q4'.
    quarter_col = _quarter_col(df, quarter)

    df[code_col] = df[code_col].astype(str).str.strip()
    df["__bev"] = df[quarter_col].apply(to_int)
    df = df[df[code_col].str.match(r"^(E01|W01)\d+", na=False)].dropna(subset=["__bev"]).copy()
    if df.empty:
        raise ValueError(
            f"No usable E01/W01 LSOA rows found in the BEV dataset for constituency aggregation. "
            f"Detected LSOA code column: {code_col!r}."
        )

    lookup = lsoa_to_pcon_lookup_df()
    merged = df.merge(lookup, left_on=code_col, right_on="LSOA21CD", how="inner")
    merged = merged.dropna(subset=["PCON24CD", "__bev"]).copy()
    if merged.empty:
        raise ValueError(
            f"LSOA BEV rows did not match the LSOA-to-PCON lookup. "
            f"Detected LSOA code column: {code_col!r}."
        )

    bev_by_pcon = merged.groupby("PCON24CD")["__bev"].sum(min_count=1).astype(float).to_dict()
    names = {}
    if "PCON24NM" in merged.columns:
        names = merged.dropna(subset=["PCON24NM"]).drop_duplicates("PCON24CD").set_index("PCON24CD")["PCON24NM"].astype(str).to_dict()
    return bev_by_pcon, names

def _quarter_key_label(q: str) -> str:
    """Normalise quarter names: '2025Q4' and '2025 Q4' both become '2025 Q4'."""
    m = re.match(r"^\s*(\d{4})\s*Q([1-4])\s*$", str(q), flags=re.I)
    if not m:
        return str(q).strip()
    return f"{m.group(1)} Q{m.group(2)}"


def _normalise_quarter_token(q: str) -> str:
    return re.sub(r"\s+", "", str(q).strip().lower())


def available_quarters(df: pd.DataFrame) -> list[str]:
    qs = []
    for c in df.columns:
        cs = str(c).strip()
        if re.match(r"^\d{4}\s*Q[1-4]$", cs, flags=re.I):
            qs.append(_quarter_key_label(cs))

    def _key(q):
        m = re.match(r"^(\d{4})\sQ([1-4])$", q)
        if not m:
            return (0, 0)
        return (int(m.group(1)), int(m.group(2)))

    return [q for q in sorted(set(qs), key=_key, reverse=True)]


# ---------------------------
# Downloadable combined dataset
# ---------------------------
def _detect_lsoa_code_col(df: pd.DataFrame) -> str:
    code_col = pick_col(df.columns, [
        "ons code", "ons_code", "onscode",
        "area code", "area_code", "areacode",
        "lsoa code", "lsoa_code", "lsoacode",
        "lsoa21cd", "lsoa 2021 code", "lower layer super output area code",
        "geography code", "geography_code", "geo code", "geo_code",
    ])
    if code_col is None:
        for c in df.columns:
            sample = df[c].dropna().astype(str).str.strip().head(500)
            if sample.str.match(r"^(E01|W01)\d+", na=False).any():
                code_col = c
                break
    if code_col is None:
        raise ValueError(f"Could not detect LSOA code column. Columns: {df.columns.tolist()}")
    return code_col


def _quarter_col(df: pd.DataFrame, quarter: str) -> str:
    target = _normalise_quarter_token(quarter)
    for c in df.columns:
        if _normalise_quarter_token(c) == target:
            return c
    available = [str(c).strip() for c in df.columns if re.match(r"^\d{4}\s*Q[1-4]$", str(c).strip(), flags=re.I)]
    raise ValueError(f"Quarter {quarter!r} not found. Available quarter columns: {available}")



def _income_deprivation_by_lsoa() -> pd.DataFrame:
    wimd = load_wimd_df().copy()
    wimd.columns = [str(c).strip() for c in wimd.columns]
    required = {"Area code", "Domain", "Data values"}
    missing = required - set(wimd.columns)
    if missing:
        raise ValueError(f"WIMD missing columns: {missing}. Found: {wimd.columns.tolist()}")

    wimd["Area code"] = wimd["Area code"].astype(str).str.strip()
    wimd["Domain"] = wimd["Domain"].astype(str).str.strip()
    wimd = wimd[(wimd["Domain"].str.lower() == "income") & (wimd["Area code"].str.match(r"^W01", na=False))].copy()
    wimd["income_rank"] = wimd["Data values"].apply(parse_num)
    wimd = wimd.dropna(subset=["income_rank"])
    deciles = wimd_rank_to_decile(dict(zip(wimd["Area code"], wimd["income_rank"])))
    wimd["income_decile"] = wimd["Area code"].map(deciles)
    return wimd[["Area code", "income_rank", "income_decile"]].rename(columns={"Area code": "LSOA"})


def _charger_counts_by_lsoa(lsoa_codes: list[str]) -> Dict[str, int]:
    """Assign charging-point coordinates to Welsh LSOA polygons and count points.

    Uses geopandas if available; otherwise falls back to a shapely loop.
    """
    if not lsoa_codes:
        return {}
    cps = load_charge_df().copy()
    cps.columns = [str(c).strip() for c in cps.columns]
    lat_col = pick_col(cps.columns, ["latitude", "lat", "y", "y_wgs84", "y_coordinate"])
    lon_col = pick_col(cps.columns, ["longitude", "lon", "lng", "long", "x", "x_wgs84", "x_coordinate"])
    if lat_col is None or lon_col is None:
        return {}

    cps["__lat"] = pd.to_numeric(cps[lat_col], errors="coerce")
    cps["__lon"] = pd.to_numeric(cps[lon_col], errors="coerce")
    cps = cps.dropna(subset=["__lat", "__lon"])
    if cps.empty:
        return {}

    gj, geo_code_field, _ = lsoa_geojson_for_codes(tuple(sorted(set(lsoa_codes))))

    try:
        import geopandas as gpd
        from shapely.geometry import Point
        polys = gpd.GeoDataFrame.from_features(gj["features"], crs="EPSG:4326")
        if geo_code_field not in polys.columns:
            # GeoPandas may flatten properties differently depending on version.
            polys[geo_code_field] = [f.get("properties", {}).get(geo_code_field) for f in gj["features"]]
        pts = gpd.GeoDataFrame(
            cps[["__lat", "__lon"]].copy(),
            geometry=[Point(xy) for xy in zip(cps["__lon"], cps["__lat"])],
            crs="EPSG:4326",
        )
        joined = gpd.sjoin(pts, polys[[geo_code_field, "geometry"]], how="left", predicate="within")
        return joined.dropna(subset=[geo_code_field]).groupby(geo_code_field).size().astype(int).to_dict()
    except Exception:
        from shapely.geometry import Point, shape
        polys = [(str(f.get("properties", {}).get(geo_code_field)), shape(f["geometry"])) for f in gj.get("features", [])]
        counts: Dict[str, int] = {}
        for _, r in cps.iterrows():
            pt = Point(float(r["__lon"]), float(r["__lat"]))
            for code, poly in polys:
                if poly.contains(pt):
                    counts[code] = counts.get(code, 0) + 1
                    break
        return counts


def build_combined_dataset(quarter: str, geo_level: str = "LSOA") -> pd.DataFrame:
    """Create an analysis-ready table combining BEV ownership, EV chargers, and income deprivation.

    Geography options:
      - LSOA: Welsh LSOAs, spatially joined charger points, WIMD Income rank/decile.
      - LAD: Welsh LADs, charger counts from EV_COUNTS_URL where available, WIMD aggregated by median LSOA rank.
      - PCON: England/Wales constituencies for BEV + charger counts; Welsh WIMD added where LSOAs map to PCON.
    """
    geo_level = (geo_level or "LSOA").strip().upper()

    if geo_level == "PCON":
        ev_counts_df = load_ev_counts_df().copy()
        ev_counts_by_pcon, pcon_names_chg, charger_date = prepare_constituency_ev_counts(ev_counts_df)
        bev_by_pcon, pcon_names_bev = prepare_constituency_bev_counts(quarter)

        # Welsh income deprivation aggregated from LSOA -> PCON using median rank, then deciles within available Welsh PCONs.
        inc = _income_deprivation_by_lsoa()
        lookup = lsoa_to_pcon_lookup_df()[["LSOA21CD", "PCON24CD", "PCON24NM"]].copy()
        w = inc.merge(lookup, left_on="LSOA", right_on="LSOA21CD", how="inner")
        pcon_income = w.groupby("PCON24CD", as_index=False).agg(income_rank_median=("income_rank", "median"))
        pcon_income_dec = wimd_rank_to_decile(dict(zip(pcon_income["PCON24CD"], pcon_income["income_rank_median"])))
        pcon_income["income_decile"] = pcon_income["PCON24CD"].map(pcon_income_dec)

        codes = sorted(set(bev_by_pcon) | set(ev_counts_by_pcon) | set(pcon_income["PCON24CD"]))
        names = {}
        names.update(pcon_names_bev)
        names.update(pcon_names_chg)
        if "PCON24NM" in lookup.columns:
            names.update(lookup.drop_duplicates("PCON24CD").set_index("PCON24CD")["PCON24NM"].astype(str).to_dict())

        out = pd.DataFrame({"area_code": codes})
        out["area_name"] = out["area_code"].map(names)
        out["geography"] = "PCON"
        out["quarter"] = quarter
        out["bev_count"] = out["area_code"].map(bev_by_pcon)
        out["charger_count"] = out["area_code"].map(ev_counts_by_pcon)
        out = out.merge(pcon_income.rename(columns={"PCON24CD": "area_code"}), on="area_code", how="left")
        out["charger_reference_date"] = charger_date

    elif geo_level == "LAD":
        bev = load_bev_lad_df().copy()
        bev.columns = [str(c).strip() for c in bev.columns]
        code_col = pick_col(bev.columns, ["ons code", "ons_code", "onscode", "area code", "lad code"])
        name_col = pick_col(bev.columns, ["ons geography", "ons_geography", "onsgeography", "area name", "lad name"])
        qcol = _quarter_col(bev, quarter)
        bev[code_col] = bev[code_col].astype(str).str.strip()
        bev = bev[bev[code_col].str.match(r"^W06", na=False)].copy()
        bev["bev_count"] = bev[qcol].apply(to_int)
        out = bev[[code_col] + ([name_col] if name_col else []) + ["bev_count"]].rename(columns={code_col: "area_code", name_col: "area_name" if name_col else code_col})

        # Charger counts at LAD from EV_COUNTS_URL.
        evc = load_ev_counts_df().copy()
        evc.columns = [str(c).strip() for c in evc.columns]
        ccode = pick_col(evc.columns, ["local authority code", "local_authority_code", "lad code", "lad_code", "area code"])
        ckey = pick_col(evc.columns, ["key", "metric", "measure", "indicator"])
        cval = pick_col(evc.columns, ["value", "count", "ev chargers", "charging devices"])
        cdate = pick_col(evc.columns, ["date", "period", "reference date"])
        charger_date = None
        if ccode and cval:
            evc[ccode] = evc[ccode].astype(str).str.strip()
            evc["__value_num"] = evc[cval].apply(parse_num)
            evc = evc[evc[ccode].str.match(r"^W06", na=False)].dropna(subset=["__value_num"]).copy()
            if ckey:
                filtered = evc[evc[ckey].astype(str).str.lower().str.contains("charger|device", na=False)].copy()
                if not filtered.empty:
                    evc = filtered
            if cdate:
                evc["__date"] = pd.to_datetime(evc[cdate], errors="coerce", dayfirst=True)
                if evc["__date"].notna().any():
                    latest = evc["__date"].max()
                    charger_date = latest.strftime("%Y-%m-%d")
                    evc = evc[evc["__date"].eq(latest)]
            chargers = evc.groupby(ccode)["__value_num"].sum().rename("charger_count").reset_index().rename(columns={ccode: "area_code"})
            out = out.merge(chargers, on="area_code", how="left")
        else:
            out["charger_count"] = np.nan
            charger_date = None

        # WIMD LSOA -> LAD median rank.
        inc = _income_deprivation_by_lsoa()
        lsoa_meta = arcgis_pjson(f"{LSOA_TO_LAD_FS}/{LSOA_TO_LAD_LAYER}")
        fields = [f["name"] for f in lsoa_meta.get("fields", [])]
        lsoa_field = pick_field(fields, ["lsoa21cd", "lsoa11cd", "lsoacd"])
        lad_field = pick_field(fields, ["ltla22cd", "lad22cd", "lad23cd", "lad24cd", "ladcd"])
        lookup = arcgis_query_table(LSOA_TO_LAD_FS, LSOA_TO_LAD_LAYER, where=f"{lsoa_field} LIKE 'W01%'", out_fields=','.join([lsoa_field, lad_field]))
        income_lad = inc.merge(lookup, left_on="LSOA", right_on=lsoa_field, how="inner")
        income_lad = income_lad.groupby(lad_field, as_index=False).agg(income_rank_median=("income_rank", "median"))
        lad_deciles = wimd_rank_to_decile(dict(zip(income_lad[lad_field], income_lad["income_rank_median"])))
        income_lad["income_decile"] = income_lad[lad_field].map(lad_deciles)
        out = out.merge(income_lad.rename(columns={lad_field: "area_code"}), on="area_code", how="left")
        out["geography"] = "LAD"
        out["quarter"] = quarter
        out["charger_reference_date"] = charger_date

    else:
        # LSOA default: Welsh LSOAs only, because WIMD is Wales-only in this file.
        bev = load_bev_lsoa_df().copy()
        bev.columns = [str(c).strip() for c in bev.columns]
        code_col = _detect_lsoa_code_col(bev)
        name_col = pick_col(bev.columns, ["ons geography", "ons_geography", "onsgeography", "area name", "lsoa name"])
        qcol = _quarter_col(bev, quarter)
        bev[code_col] = bev[code_col].astype(str).str.strip()
        bev = bev[bev[code_col].str.match(r"^W01", na=False)].copy()
        bev["bev_count"] = bev[qcol].apply(to_int)
        out_cols = [code_col] + ([name_col] if name_col else []) + ["bev_count"]
        out = bev[out_cols].rename(columns={code_col: "area_code", name_col: "area_name" if name_col else code_col})
        # Ensure LSOA names always exist for tooltips/display.
        if "area_name" not in out.columns or out["area_name"].isna().all():
            out["area_name"] = out["area_code"]
        out = out.dropna(subset=["bev_count"]).copy()
        inc = _income_deprivation_by_lsoa().rename(columns={"LSOA": "area_code"})
        out = out.merge(inc, on="area_code", how="left")
        charger_counts = _charger_counts_by_lsoa(out["area_code"].dropna().astype(str).unique().tolist())
        out["charger_count"] = out["area_code"].map(charger_counts).fillna(0).astype(int)
        out["geography"] = "LSOA"
        out["quarter"] = quarter
        out["charger_reference_date"] = "point dataset snapshot"

    out["bev_count"] = pd.to_numeric(out.get("bev_count"), errors="coerce")
    out["charger_count"] = pd.to_numeric(out.get("charger_count"), errors="coerce")
    out["chargers_per_1000_bev"] = np.where(
        (out["bev_count"].notna()) & (out["bev_count"] > 0) & (out["charger_count"].notna()),
        1000.0 * out["charger_count"] / out["bev_count"],
        np.nan,
    )

    preferred = [
        "geography", "area_code", "area_name", "quarter", "bev_count", "charger_count",
        "chargers_per_1000_bev", "income_rank", "income_rank_median", "income_decile",
        "charger_reference_date",
    ]
    cols = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    return out[cols].sort_values(["geography", "area_code"]).reset_index(drop=True)




def add_user_instruction_box(m: folium.Map, *, geo_level: str, quarter: str) -> None:
    """Add a concise on-map instruction panel for non-technical users."""
    geo_label = {
        "PCON": "Westminster Parliamentary Constituency",
        "LAD": "Local Authority District",
        "LSOA": "Lower Layer Super Output Area",
    }.get(str(geo_level).upper(), str(geo_level).upper())

    html0 = f"""
    <div style="position: fixed; top: 18px; left: 18px; z-index: 9999;
                width: 430px; max-height: 360px; overflow-y: auto;
                background: rgba(255,255,255,0.94); border: 2px solid #555;
                border-radius: 10px; padding: 12px 14px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.25);
                font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
                font-size: 20px; line-height: 1.45;">
      <div style="font-weight: 900; font-size: 22px; margin-bottom: 6px;">How to read this map</div>
      <div><b>Geography:</b> {geo_label}</div>
      <div><b>Default layer:</b> Income deprivation decile, where 1 = most deprived and 10 = least deprived.</div>
      <div><b>EV information:</b> hover over an area to see BEV keepership ({quarter}), EV charger count, chargers per 1,000 BEVs, and infrastructure status.</div>
      <div><b>EV charger markers:</b> small transparent red circles show constituency-level charger counts. Toggle them in the layer control.</div>
      <div><b>Zoom detail:</b> zoom in and enable <i>LSOA detail</i> to inspect Welsh LSOA boundaries, local BEV values, income decile, and assigned charger counts.</div>
      <div style="margin-top: 6px; color: #555;">Use the layer control on the right to switch between policy layers.</div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(html0))


def add_common_map_css(m: folium.Map) -> None:
    """Consistent readable font sizes for layer control, legends, tooltips and popups."""
    css_fonts = """
{% macro html(this, kwargs) %}
<style>
.leaflet-control-layers { font-size: 13px !important; }
.leaflet-control-layers label { font-size: 13px !important; line-height: 1.2 !important; }
.leaflet-tooltip { font-size: 16px !important; line-height: 1.35 !important; padding: 7px 9px !important; }
.leaflet-popup-content { font-size: 15px !important; line-height: 1.35 !important; }
.legend, .leaflet-control .legend { font-size: 13px !important; line-height: 1.2 !important; padding: 7px 8px !important; }
.legend .caption, .leaflet-control .legend .caption { font-size: 14px !important; font-weight: 700 !important; }
.legend-scale ul li, .leaflet-control .legend-scale ul li { font-size: 12px !important; }
</style>
{% endmacro %}
"""
    macro_fonts = MacroElement()
    macro_fonts._template = Template(css_fonts)
    m.get_root().add_child(macro_fonts)


def add_lsoa_detail_layer(m: folium.Map, *, quarter: str) -> None:
    """Add a Welsh LSOA detail layer for deep zoom inspection.

    The layer is off by default because it is visually dense. Users can switch it
    on after zooming in to inspect local EV keepership, charger counts assigned
    to LSOAs, and WIMD Income deprivation decile.
    """
    try:
        lsoa_df = build_combined_dataset(quarter=quarter, geo_level="LSOA")
        lsoa_df = lsoa_df.dropna(subset=["area_code"]).copy()
        codes = lsoa_df["area_code"].astype(str).unique().tolist()
        if not codes:
            return
        gj_lsoa, lsoa_code_field, lsoa_name_field = lsoa_geojson_for_codes(tuple(sorted(codes)))

        # Build a one-row-per-LSOA lookup for tooltip/detail rendering.
        # Pandas requires the index to be unique when using orient="index";
        # duplicate LSOA rows can occur after joins, so aggregate defensively.
        lsoa_df = lsoa_df.copy()
        lsoa_df["area_code"] = lsoa_df["area_code"].astype(str).str.strip()

        def _first_valid(series):
            non_null = series.dropna()
            return non_null.iloc[0] if not non_null.empty else np.nan

        agg_spec = {c: _first_valid for c in lsoa_df.columns if c != "area_code"}
        lsoa_df = lsoa_df.groupby("area_code", as_index=False).agg(agg_spec)
        lookup = lsoa_df.set_index("area_code", drop=True).to_dict(orient="index")

        detail_gj = {"type": "FeatureCollection", "features": []}
        for feat in gj_lsoa.get("features", []):
            props0 = feat.get("properties") or {}
            code = str(props0.get(lsoa_code_field))
            r = lookup.get(code, {})
            name = props0.get(lsoa_name_field) or r.get("area_name") or code
            bev = r.get("bev_count")
            chg = r.get("charger_count")
            dec = r.get("income_decile")
            ratio = r.get("chargers_per_1000_bev")
            props = {
                "LSOA": name,
                "BEV keepership": f"{int(bev):,}" if pd.notna(bev) else "Suppressed / no data",
                "EV chargers assigned to LSOA": f"{int(chg):,}" if pd.notna(chg) else "0",
                "Chargers per 1,000 BEVs": f"{float(ratio):.2f}" if pd.notna(ratio) else "NA",
                "Income deprivation decile": f"Decile {int(dec)}" if pd.notna(dec) else "NA",
            }
            detail_gj["features"].append({"type": "Feature", "geometry": feat["geometry"], "properties": props})

        fg = folium.FeatureGroup(name="LSOA detail: BEV + chargers + income", show=False)
        folium.GeoJson(
            detail_gj,
            name="LSOA detail boundaries",
            style_function=lambda x: {"fillOpacity": 0.02, "weight": 0.7, "color": "#111111", "fillColor": "#ffffff"},
            highlight_function=lambda x: {"weight": 2.0, "color": "#000000", "fillOpacity": 0.08},
            tooltip=folium.GeoJsonTooltip(
                fields=["LSOA", "BEV keepership", "EV chargers assigned to LSOA", "Chargers per 1,000 BEVs", "Income deprivation decile"],
                aliases=["LSOA:", f"BEV keepership ({quarter}):", "EV chargers:", "Chargers per 1,000 BEVs:", "Income deprivation decile:"],
                sticky=True,
                labels=True,
                style=("background-color: white; color: black; font-size: 22px; font-weight: 600; "
                       "padding: 10px; border: 2px solid #444; border-radius: 6px;"),
            ),
        ).add_to(fg)
        fg.add_to(m)
    except Exception as e:
        note = f"""
        <div style="position: fixed; bottom: 34px; right: 34px; z-index:9999;
                    background:white; border:1px solid #777; border-radius:6px;
                    padding:8px 10px; font-size:18px; max-width:360px;">
          LSOA detail layer could not be loaded: {str(e)}
        </div>
        """
        m.get_root().html.add_child(folium.Element(note))

def build_thrust_one_map(
    quarter: str,
    *,
    geo_level: str = "LAD",  # 'LAD' or 'LSOA'
    default_wimd_domain: Optional[str] = "Income",
    show_charging: bool = True,
    show_centroids: bool = False,
) -> str:
    from shapely.geometry import shape

    # ---- BEV prep ----
    geo_level = (geo_level or "LAD").strip().upper()
    if geo_level not in {"LAD", "LSOA", "PCON"}:
        geo_level = "LAD"

    if geo_level == "PCON":
        ev_counts_df = load_ev_counts_df().copy()
        ev_counts_by_pcon, pcon_name_by_code_chargers, ev_count_date_label = prepare_constituency_ev_counts(ev_counts_df)
        bev_by_pcon, pcon_name_by_code_bev = prepare_constituency_bev_counts(quarter)

        # Income WIMD only: aggregate Welsh LSOA Income ranks to PCON, then convert to 1-10 deciles.
        # Decile 1 = most deprived; decile 10 = least deprived.
        inc = _income_deprivation_by_lsoa()
        lookup_income = lsoa_to_pcon_lookup_df()[["LSOA21CD", "PCON24CD", "PCON24NM"]].copy()
        w_income = inc.merge(lookup_income, left_on="LSOA", right_on="LSOA21CD", how="inner")
        pcon_income = w_income.groupby("PCON24CD", as_index=False).agg(
            income_rank_median=("income_rank", "median")
        )
        income_rank_by_pcon = dict(zip(pcon_income["PCON24CD"], pcon_income["income_rank_median"]))
        income_decile_by_pcon = wimd_rank_to_decile(income_rank_by_pcon)

        # Union of constituencies: charger counts, BEV keepership, and Welsh Income WIMD all remain visible where available.
        codes = sorted(set(ev_counts_by_pcon.keys()) | set(bev_by_pcon.keys()) | set(income_decile_by_pcon.keys()))
        gj, geo_code_field, geo_name_field = constituency_geojson_for_codes(tuple(codes))

        centroid_pts, bev_heat_pts, charger_heat_pts = [], [], []
        for feat in gj.get("features", []):
            code0 = str((feat.get("properties") or {}).get(geo_code_field))
            c = shape(feat["geometry"]).centroid
            centroid_pts.append([c.y, c.x])
            bev_v = bev_by_pcon.get(code0)
            chg_v = ev_counts_by_pcon.get(code0)
            if bev_v is not None and np.isfinite(bev_v):
                bev_heat_pts.append([c.y, c.x, float(bev_v)])
            if chg_v is not None and np.isfinite(chg_v):
                charger_heat_pts.append([c.y, c.x, float(chg_v)])

        if centroid_pts:
            lats = [p[0] for p in centroid_pts]
            lons = [p[1] for p in centroid_pts]
            m = folium.Map(location=[float(np.mean(lats)), float(np.mean(lons))], zoom_start=6, tiles=None)
        else:
            m = folium.Map(location=[54.5, -3.0], zoom_start=6, tiles=None)

        folium.TileLayer("cartodbpositron", name="CartoDB Positron", show=True).add_to(m)
        folium.TileLayer("OpenStreetMap", name="OSM (default)", show=False).add_to(m)
        folium.TileLayer("cartodbdark_matter", name="CartoDB Dark", show=False).add_to(m)
        folium.TileLayer(
            tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
            attr="© OpenTopoMap (CC-BY-SA)",
            name="OpenTopoMap",
            show=False,
        ).add_to(m)

        evcmini = pd.DataFrame({"Area code": list(ev_counts_by_pcon.keys()), "val": list(ev_counts_by_pcon.values())})
        evc_legend = "Public EV chargers by Westminster parliamentary constituency"
        if ev_count_date_label:
            evc_legend += f" ({ev_count_date_label})"
        folium.Choropleth(
            geo_data=gj,
            data=evcmini,
            columns=["Area code", "val"],
            key_on=f"feature.properties.{geo_code_field}",
            name="EV chargers by parliamentary constituency",
            fill_color="YlOrRd",
            fill_opacity=0.42,
            line_color="#636363",
            line_opacity=0.7,
            line_weight=0.8,
            nan_fill_color="#f0f0f0",
            legend_name=evc_legend,
            show=show_charging,
        ).add_to(m)

        if bev_by_pcon:
            bevmini = pd.DataFrame({"Area code": list(bev_by_pcon.keys()), "val": list(bev_by_pcon.values())})
            folium.Choropleth(
                geo_data=gj,
                data=bevmini,
                columns=["Area code", "val"],
                key_on=f"feature.properties.{geo_code_field}",
                name=f"BEV keepership by LSOA 2021 ({quarter})",
                fill_color="YlGnBu",
                fill_opacity=0.50,
                line_color="#525252",
                line_opacity=0.55,
                line_weight=0.7,
                nan_fill_color="#f0f0f0",
                legend_name=f"BEV keepership by Westminster parliamentary constituency ({quarter}; LSOA to PCON best-fit aggregation)",
                show=False,
            ).add_to(m)

        if income_decile_by_pcon:
            income_dmini = pd.DataFrame({
                "Area code": list(income_decile_by_pcon.keys()),
                "decile": list(income_decile_by_pcon.values()),
            })
            folium.Choropleth(
                geo_data=gj,
                data=income_dmini,
                columns=["Area code", "decile"],
                key_on=f"feature.properties.{geo_code_field}",
                name="Income deprivation decile (1=most deprived, 10=least deprived)",
                fill_color="RdYlGn",
                fill_opacity=0.78,
                line_color="#303030",
                line_opacity=0.85,
                line_weight=1.25,
                bins=list(range(1, 12)),
                nan_fill_color="#f0f0f0",
                legend_name="Income deprivation decile (1 = most deprived; 10 = least deprived)",
                show=True,
            ).add_to(m)

        if bev_heat_pts:
            HeatMap(
                bev_heat_pts,
                radius=22,
                blur=28,
                max_zoom=8,
                min_opacity=0.18,
                name=f"BEV keepership intensity ({quarter})",
                show=False,
            ).add_to(m)

        if show_charging and charger_heat_pts:
            HeatMap(
                charger_heat_pts,
                radius=25,
                blur=30,
                max_zoom=8,
                min_opacity=0.18,
                name="EV charger count intensity",
                show=False,
            ).add_to(m)

        # Explicit constituency-centroid markers for EV chargers.
        # These make infrastructure provision visible even when choropleth layers overlap.
        if show_charging and ev_counts_by_pcon:
            charger_marker_layer = folium.FeatureGroup(name="EV chargers: constituency markers", show=True)
            for feat in gj.get("features", []):
                props0 = feat.get("properties") or {}
                code0 = str(props0.get(geo_code_field))
                charger_v = ev_counts_by_pcon.get(code0)
                if charger_v is None or not np.isfinite(charger_v):
                    continue
                c = shape(feat["geometry"]).centroid
                name0 = (props0.get(geo_name_field, code0) if geo_name_field else code0)
                folium.CircleMarker(
                    location=[c.y, c.x],
                    radius=4,
                    color="#d62728",
                    weight=1,
                    opacity=0.45,
                    fill=True,
                    fill_color="#d62728",
                    fill_opacity=0.28,
                    tooltip=f"{name0}<br>EV chargers: {int(charger_v):,}",
                ).add_to(charger_marker_layer)
            charger_marker_layer.add_to(m)

        hover_gj = {"type": "FeatureCollection", "features": []}
        combined_names = {}
        combined_names.update(pcon_name_by_code_bev)
        combined_names.update(pcon_name_by_code_chargers)
        if "lookup_income" in locals() and "PCON24NM" in lookup_income.columns:
            combined_names.update(
                lookup_income.drop_duplicates("PCON24CD")
                .set_index("PCON24CD")["PCON24NM"].astype(str).to_dict()
            )
        for feat in gj.get("features", []):
            props0 = feat.get("properties") or {}
            code0 = str(props0.get(geo_code_field))
            name0 = combined_names.get(code0, props0.get(geo_name_field, code0))
            charger_v = ev_counts_by_pcon.get(code0)
            bev_v = bev_by_pcon.get(code0)
            ratio = np.nan
            if charger_v is not None and bev_v is not None and np.isfinite(charger_v) and np.isfinite(bev_v) and bev_v > 0:
                ratio = 1000.0 * float(charger_v) / float(bev_v)
            income_decile_v = income_decile_by_pcon.get(code0)
            income_rank_v = income_rank_by_pcon.get(code0)
            if np.isfinite(ratio):
                if ratio < 5:
                    infrastructure_status = "⚠ Undersupplied"
                elif ratio < 10:
                    infrastructure_status = "Moderate provision"
                else:
                    infrastructure_status = "Well served"
            else:
                infrastructure_status = "Unknown"

            props = {
                "Constituency": name0,
                "EV chargers": f"{int(charger_v):,}" if (charger_v is not None and np.isfinite(charger_v)) else "NA",
                f"BEV keepership ({quarter})": f"{int(bev_v):,}" if (bev_v is not None and np.isfinite(bev_v)) else "NA",
                "Chargers per 1,000 BEVs": f"{ratio:.2f}" if np.isfinite(ratio) else "NA",
                "Infrastructure status": infrastructure_status,
                "Income deprivation decile": f"Decile {income_decile_v}" if income_decile_v is not None else "NA",
                "Income rank median": f"{income_rank_v:,.0f}" if (income_rank_v is not None and np.isfinite(income_rank_v)) else "NA",
            }
            hover_gj["features"].append({"type": "Feature", "geometry": feat["geometry"], "properties": props})

        folium.GeoJson(
            hover_gj,
            name="Hover (chargers + BEV ownership)",
            style_function=lambda x: {"fillOpacity": 0.0, "weight": 0.0, "color": "transparent"},
            tooltip=folium.GeoJsonTooltip(
                fields=["Constituency", "EV chargers", f"BEV keepership ({quarter})", "Chargers per 1,000 BEVs", "Infrastructure status", "Income deprivation decile", "Income rank median"],
                aliases=["Constituency:", "EV chargers:", f"BEV keepership ({quarter}):", "Chargers per 1,000 BEVs:", "Infrastructure status:", "Income deprivation decile:", "Income rank median:"],
                style=(
                    "background-color: white; "
                    "color: black; "
                    "font-size: 22px; "
                    "font-weight: 600; "
                    "padding: 10px; "
                    "border: 2px solid #444; "
                    "border-radius: 6px;"
                ),
                sticky=True,
                labels=True,
            ),
        ).add_to(m)

        if show_centroids:
            label_layer = folium.FeatureGroup(name="Constituency labels", show=False)
            for feat in gj.get("features", []):
                code0 = str((feat.get("properties") or {}).get(geo_code_field))
                charger_v = ev_counts_by_pcon.get(code0)
                bev_v = bev_by_pcon.get(code0)
                if (charger_v is None or not np.isfinite(charger_v)) and (bev_v is None or not np.isfinite(bev_v)):
                    continue
                c = shape(feat["geometry"]).centroid
                name0 = combined_names.get(code0, (feat.get("properties") or {}).get(geo_name_field, code0))
                parts = []
                if charger_v is not None and np.isfinite(charger_v):
                    parts.append(f"{int(charger_v):,} chargers")
                if bev_v is not None and np.isfinite(bev_v):
                    parts.append(f"{int(bev_v):,} BEVs")
                folium.Marker(
                    location=[c.y, c.x],
                    icon=folium.DivIcon(html=f"""
                        <div style="
                            font-size: 18px;
                            font-weight: 800;
                            color: #111;
                            background: rgba(255,255,255,0.82);
                            border: 1px solid #444;
                            border-radius: 6px;
                            padding: 2px 5px;
                            white-space: nowrap;
                            box-shadow: 0 1px 4px rgba(0,0,0,0.25);
                        ">
                            {' | '.join(parts)}
                        </div>
                    """),
                    tooltip=f"{name0}: " + "; ".join(parts),
                ).add_to(label_layer)
            label_layer.add_to(m)

        # Deep-zoom detail for Welsh LSOAs: off by default, enabled from the layer control.
        add_lsoa_detail_layer(m, quarter=quarter)

        add_user_instruction_box(m, geo_level=geo_level, quarter=quarter)
        add_common_map_css(m)

        # ---- Missing-data note for suppressed BEV ownership ----
        missing_css = """
{% macro html(this, kwargs) %}
<div style="
    position: fixed;
    bottom: 34px;
    left: 34px;
    z-index: 9999;
    background: white;
    border: 2px solid #777;
    border-radius: 6px;
    padding: 10px 12px;
    font-size: 24px;
    line-height: 1.45;
    box-shadow: 0 1px 5px rgba(0,0,0,0.25);
">
    <div style="font-weight: 700; margin-bottom: 4px;">Missing data</div>
    <div><span style="display:inline-block;width:18px;height:12px;background:#f0f0f0;border:1px solid #999;margin-right:7px;"></span>
    Suppressed / no BEV keepership data</div>
</div>
{% endmacro %}
"""
        missing_macro = MacroElement()
        missing_macro._template = Template(missing_css)
        m.get_root().add_child(missing_macro)

        folium.LayerControl(collapsed=False).add_to(m)
        return m._repr_html_()

    df = (load_bev_lsoa_df() if geo_level == "LSOA" else load_bev_lad_df()).copy()
    df.columns = [str(c).strip() for c in df.columns]

    if geo_level == "LSOA":
        code_col = _detect_lsoa_code_col(df)
    else:
        code_col = pick_col(df.columns, ["ons code", "ons_code", "onscode", "area code", "area_code", "lad code", "lad_code", "geography code"])
    name_col = pick_col(df.columns, ["ons geography", "ons_geography", "onsgeography", "area name", "area_name", "lsoa name", "lad name", "geography name"])
    if code_col is None:
        raise ValueError(f"Could not detect geography code column in BEV dataset. Found columns: {df.columns.tolist()}")

    quarter_col = _quarter_col(df, quarter)
    df["value"] = df[quarter_col].apply(to_int)

    code_pat = r"^W01" if geo_level == "LSOA" else r"^W06"
    df[code_col] = df[code_col].astype(str).str.strip()
    if name_col is not None:
        df[name_col] = df[name_col].astype(str).str.strip()

    df = df[df[code_col].str.match(code_pat)].dropna(subset=["value"])

    codes = df[code_col].astype(str).unique().tolist()
    if not codes:
        raise ValueError(f"No Welsh {geo_level} codes found in BEV dataset after filtering (expected {code_pat}).")

    name_by_code = dict(zip(df[code_col].astype(str), (df[name_col].astype(str) if name_col else df[code_col].astype(str))))
    bev_by_code = dict(zip(df[code_col].astype(str), df["value"].astype(float)))
    codes_set = set(codes)

    # ---- boundaries ----
    if geo_level == "LSOA":
        gj, geo_code_field, geo_name_field = lsoa_geojson_for_codes(tuple(codes))
    else:
        gj, geo_code_field, geo_name_field = lad_geojson_for_codes(tuple(codes))

    # ---- EV charger counts by local authority (from user sheet) ----
    ev_counts_df = load_ev_counts_df().copy()
    ev_counts_df.columns = [str(c).strip() for c in ev_counts_df.columns]
    ev_counts_code_col = pick_col(ev_counts_df.columns, ["local authority code", "local_authority_code", "lad code", "lad_code"])
    ev_counts_name_col = pick_col(ev_counts_df.columns, ["local authority", "local_authority", "lad name", "lad_name"])
    ev_counts_key_col = pick_col(ev_counts_df.columns, ["key", "metric", "measure"])
    ev_counts_value_col = pick_col(ev_counts_df.columns, ["value", "count"])
    ev_counts_date_col = pick_col(ev_counts_df.columns, ["date"])

    ev_counts_by_lad: Dict[str, float] = {}
    ev_count_date_label = None
    if all([ev_counts_code_col, ev_counts_key_col, ev_counts_value_col]):
        evc = ev_counts_df.copy()
        evc[ev_counts_code_col] = evc[ev_counts_code_col].astype(str).str.strip()
        evc[ev_counts_key_col] = evc[ev_counts_key_col].astype(str).str.strip()
        evc["__value_num"] = evc[ev_counts_value_col].apply(parse_num)
        if ev_counts_date_col:
            evc["__date"] = pd.to_datetime(evc[ev_counts_date_col], errors="coerce", dayfirst=True)
        else:
            evc["__date"] = pd.NaT

        evc = evc[evc[ev_counts_code_col].str.match(r"^W06")].copy()
        evc = evc[evc[ev_counts_key_col].str.lower().eq("ev chargers")].dropna(subset=["__value_num"]).copy()
        if not evc.empty:
            if evc["__date"].notna().any():
                latest_date = evc["__date"].max()
                evc = evc[evc["__date"].eq(latest_date)].copy()
                ev_count_date_label = latest_date.strftime("%d %B %Y")
            ev_counts_by_lad = dict(zip(evc[ev_counts_code_col].astype(str), evc["__value_num"].astype(float)))

    # ---- BEV heat points ----
    bev_heat_pts = []
    for feat in gj["features"]:
        lad_code = str((feat.get("properties") or {}).get(geo_code_field))
        v = bev_by_code.get(lad_code)
        if v is None or not np.isfinite(v):
            continue
        c = shape(feat["geometry"]).centroid
        bev_heat_pts.append([c.y, c.x, float(v)])

    lats = [p[0] for p in bev_heat_pts]
    lons = [p[1] for p in bev_heat_pts]


    # ---- base map + tiles ----
    m = folium.Map(location=[float(np.mean(lats)), float(np.mean(lons))], zoom_start=8, tiles=None)
    folium.TileLayer("cartodbpositron", name="CartoDB Positron", show=True).add_to(m)
    folium.TileLayer("OpenStreetMap", name="OSM (default)", show=False).add_to(m)
    folium.TileLayer("cartodbdark_matter", name="CartoDB Dark", show=False).add_to(m)
    folium.TileLayer(
           tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
           attr="© OpenTopoMap (CC-BY-SA)",
           name="OpenTopoMap",
           show=False,
           ).add_to(m)


    # ---- EV charger counts choropleth (LAD only) ----
    if ev_counts_by_lad and geo_level == "LAD":
        evcmini = pd.DataFrame({"Area code": list(ev_counts_by_lad.keys()), "val": list(ev_counts_by_lad.values())})
        evc_legend = "Public EV chargers by local authority"
        if ev_count_date_label:
            evc_legend += f" ({ev_count_date_label})"
        folium.Choropleth(
            geo_data=gj,
            data=evcmini,
            columns=["Area code", "val"],
            key_on=f"feature.properties.{geo_code_field}",
            name="EV chargers by local authority (choropleth)",
            fill_color="YlOrRd",
            fill_opacity=0.45,
            line_color="#bdbdbd",
            line_opacity=0.7,
            line_weight=1.0,
            legend_name=evc_legend,
            show=False,
        ).add_to(m)

    # ---- BEV layer ----
    HeatMap(
        bev_heat_pts,
        radius=20,
        blur=25,
        max_zoom=10,
        min_opacity=0.25,
        name=f"BEV Keepership heatmap ({quarter})",
        show=True,
    ).add_to(m)

    # ---- centroid labels (optional) ----
    if show_centroids:
        quarter_layer = folium.FeatureGroup(name=f"{quarter} values (centroids)", show=True)
        for feat in gj["features"]:
            lad_code = str((feat.get("properties") or {}).get(geo_code_field))
            v = bev_by_code.get(lad_code)
            if v is None or not np.isfinite(v):
                continue
            c = shape(feat["geometry"]).centroid
            lad_name = name_by_code.get(lad_code, lad_code)
            label = f"{lad_name}: {int(v):,} ({quarter})"
            folium.CircleMarker(
                location=[c.y, c.x],
                radius=4,
                color="black",
                weight=1,
                fill=True,
                fill_color="black",
                fill_opacity=0.9,
                tooltip=label,
                popup=label,
            ).add_to(quarter_layer)
        quarter_layer.add_to(m)

    # ---- WIMD domain values (rank) ----
    wimd = load_wimd_df().copy()
    req = {"Area code", "Domain", "Data values"}
    missing = req - set(wimd.columns)
    if missing:
        raise ValueError(f"WIMD missing columns: {missing}. Found: {wimd.columns.tolist()}")

    wimd["Area code"] = wimd["Area code"].astype(str).str.strip()
    wimd["Domain"] = wimd["Domain"].astype(str).str.strip()
    wimd["__value_num"] = wimd["Data values"].apply(parse_num)

    domain_value_by_code: Dict[str, Dict[str, float]] = {}

    if geo_level == "LSOA":
        wimd_lsoa = wimd[wimd["Area code"].str.match(r"^W01")].dropna(subset=["__value_num"]).copy()
        if wimd_lsoa.empty:
            raise ValueError("WIMD file has no usable W01 (Welsh LSOA) rows with numeric 'Data values'.")
        for dom in sorted(wimd_lsoa["Domain"].dropna().unique().tolist()):
            d = wimd_lsoa[wimd_lsoa["Domain"] == dom]
            domain_value_by_code[str(dom)] = dict(zip(d["Area code"], d["__value_num"].astype(float)))
    else:
        # LAD-direct first
        wimd_lad_num = wimd[wimd["Area code"].isin(codes_set)].dropna(subset=["__value_num"]).copy()
        if not wimd_lad_num.empty:
            for dom in sorted(wimd_lad_num["Domain"].dropna().unique().tolist()):
                d = wimd_lad_num[wimd_lad_num["Domain"] == dom]
                domain_value_by_code[str(dom)] = dict(zip(d["Area code"], d["__value_num"].astype(float)))
        else:
            # LSOA-level (W01...) -> LTLA22CD aggregation
            wimd_lsoa = wimd[wimd["Area code"].str.match(r"^W01")].dropna(subset=["__value_num"]).copy()
            if wimd_lsoa.empty:
                raise ValueError("WIMD file has no numeric 'Data values' for LAD codes and no usable W01 LSOA rows.")

            lsoa_meta = arcgis_pjson(f"{LSOA_TO_LAD_FS}/{LSOA_TO_LAD_LAYER}")
            lsoa_fields = [f["name"] for f in lsoa_meta.get("fields", [])]

            lsoa_code_field = pick_field(lsoa_fields, ["lsoa21cd", "lsoa11cd", "lsoacd"])
            lad_lookup_field = pick_field(lsoa_fields, ["ltla22cd", "lad22cd", "lad23cd", "lad24cd", "ladcd"])
            if lsoa_code_field is None or lad_lookup_field is None:
                raise ValueError(f"Couldn't infer LSOA->LTLA lookup fields. Fields include: {lsoa_fields[:40]} ...")

            where = f"{lsoa_code_field} LIKE 'W01%'"
            lookup = arcgis_query_table(
                LSOA_TO_LAD_FS,
                LSOA_TO_LAD_LAYER,
                where=where,
                out_fields=",".join([lsoa_code_field, lad_lookup_field]),
            )
            lookup[lsoa_code_field] = lookup[lsoa_code_field].astype(str).str.strip()
            lookup[lad_lookup_field] = lookup[lad_lookup_field].astype(str).str.strip()

            merged = wimd_lsoa.merge(lookup, left_on="Area code", right_on=lsoa_code_field, how="left")
            merged = merged.dropna(subset=[lad_lookup_field, "__value_num"])
            merged = merged[merged[lad_lookup_field].isin(codes_set)].copy()

            for dom, g in merged.groupby("Domain", dropna=True):
                lad_vals = g.groupby(lad_lookup_field)["__value_num"].median()
                domain_value_by_code[str(dom)] = lad_vals.to_dict()

    # ---- WIMD layers (Income deprivation decile only) ----
    # WIMD documentation is rank/decile oriented: lower ranks/scores indicate greater deprivation.
    # For comparability and publication, map Income as deciles:
    #   1 = most deprived 10%; 10 = least deprived 10%.
    domains_sorted = ["Income"] if "Income" in domain_value_by_code else []

    for dom in domains_sorted:
        value_by_code = domain_value_by_code[dom]
        decile_by_code = wimd_rank_to_decile(value_by_code)
        dmini = pd.DataFrame({
            "Area code": list(decile_by_code.keys()),
            "decile": list(decile_by_code.values())
        })

        show_this = (
            default_wimd_domain is not None
            and str(dom).strip().lower() == str(default_wimd_domain).strip().lower()
        )

        folium.Choropleth(
            geo_data=gj,
            data=dmini,
            columns=["Area code", "decile"],
            key_on=f"feature.properties.{geo_code_field}",
            name="Income deprivation decile (1=most deprived, 10=least deprived)",
            fill_color="RdYlGn",
            fill_opacity=0.62,
            line_color="#636363",
            line_opacity=0.55,
            line_weight=1.1,
            bins=list(range(1, 12)),
            nan_fill_color="#f0f0f0",
            legend_name="Income deprivation decile (1 = most deprived; 10 = least deprived)",
            show=show_this,
        ).add_to(m)

    # ---- Hover tooltip (values only; no codes) ----
    hover_gj = {"type": "FeatureCollection", "features": []}
    for feat in gj["features"]:
        props0 = feat.get("properties") or {}
        lad_code = str(props0.get(geo_code_field))
        lad_name = name_by_code.get(lad_code, props0.get(geo_name_field, lad_code))

        props = {("LSOA" if geo_level=="LSOA" else "LAD"): lad_name}
        bev = bev_by_code.get(lad_code)
        props[f"BEV keepership ({quarter})"] = f"{int(bev):,}" if (bev is not None and np.isfinite(bev)) else "Suppressed / no data"
        evc = ev_counts_by_lad.get(lad_code) if geo_level == "LAD" else None
        if geo_level == "LAD":
            props["EV chargers (count)"] = f"{int(evc):,}" if (evc is not None and np.isfinite(evc)) else "NA"

        for dom in domains_sorted:
            decile_v = wimd_rank_to_decile(domain_value_by_code[dom]).get(lad_code)
            props[f"WIMD {dom} decile"] = f"{decile_v}" if decile_v is not None else "NA"

        hover_gj["features"].append({"type": "Feature", "geometry": feat["geometry"], "properties": props})

        geo_label = "LSOA" if geo_level=="LSOA" else "LAD"
    tooltip_fields = [geo_label, f"BEV keepership ({quarter})"] + (["EV chargers (count)"] if geo_level == "LAD" else []) + [f"WIMD {dom} decile" for dom in domains_sorted]
    tooltip_aliases = [f"{geo_label}:", f"BEV keepership ({quarter}):"] + (["EV chargers:"] if geo_level == "LAD" else []) + [f"{dom} decile:" for dom in domains_sorted]

    folium.GeoJson(
        hover_gj,
        name="Hover (BEV keepership + Income WIMD decile)",
        style_function=lambda x: {"fillOpacity": 0.0, "weight": 0.0, "color": "transparent"},
        tooltip=folium.GeoJsonTooltip(
            fields=tooltip_fields,
            aliases=tooltip_aliases,
            sticky=True,
            labels=True,
        ),
    ).add_to(m)

    # ---- Charging points ----
    if show_charging:
        cps = load_charge_df().copy()
        lat_col = pick_col(cps.columns, ["latitude", "lat", "y", "y_wgs84", "y_coordinate", "northing"])
        lon_col = pick_col(cps.columns, ["longitude", "lon", "lng", "long", "x", "x_wgs84", "x_coordinate", "easting"])
        if lat_col is not None and lon_col is not None:
            cps["__lat"] = pd.to_numeric(cps[lat_col], errors="coerce")
            cps["__lon"] = pd.to_numeric(cps[lon_col], errors="coerce")
            cps = cps.dropna(subset=["__lat", "__lon"])

            charging_layer = folium.FeatureGroup(name="Charging points", show=True)
            cluster = MarkerCluster(name="Charging points (clustered)").add_to(charging_layer)

            def charging_icon():
                return folium.Icon(color="blue", icon="flash", prefix="glyphicon")

            for _, r in cps.iterrows():
                html_tbl = row_to_html_table(r)
                iframe = folium.IFrame(html=html_tbl, width=430, height=280)
                popup = folium.Popup(iframe, max_width=450)

                non_null = []
                for c in cps.columns:
                    if c in {"__lat", "__lon"}:
                        continue
                    v = r.get(c)
                    if pd.notna(v) and str(v).strip() and str(v).lower() != "nan":
                        non_null.append(f"{c}: {v}")
                    if len(non_null) >= 3:
                        break
                tooltip = " | ".join(non_null) if non_null else "Charging point"

                folium.Marker(
                    location=[float(r["__lat"]), float(r["__lon"])],
                    tooltip=tooltip,
                    popup=popup,
                    icon=charging_icon(),
                ).add_to(cluster)

            charging_layer.add_to(m)

    # ---- Layer control (right) ----
    css = """
    {% macro html(this, kwargs) %}
    <style>
    .leaflet-control-layers { right: 10px !important; left: auto !important; }
    </style>
    {% endmacro %}
    """
    macro = MacroElement()
    macro._template = Template(css)
    m.get_root().add_child(macro)

    
    # --- Enforce single-select for WIMD choropleth layers in the Layer Control ---
    js = """
    {% macro html(this, kwargs) %}
    <script>
    (function() {
      function isWimdChoroLabel(lblText) {
        return (lblText || '').trim().startsWith('WIMD ') && (lblText || '').includes('decile');
      }
      function wire() {
        var ctl = document.querySelector('.leaflet-control-layers');
        if (!ctl) { return; }
        ctl.addEventListener('change', function(ev) {
          var t = ev.target;
          if (!t || t.type !== 'checkbox' || !t.checked) { return; }
          var label = t.closest('label');
          if (!label) { return; }
          var txt = label.textContent || '';
          if (!isWimdChoroLabel(txt)) { return; }
          // turn off any other checked WIMD choropleths by clicking them
          var inputs = ctl.querySelectorAll('input[type="checkbox"]');
          inputs.forEach(function(inp) {
            if (inp === t || !inp.checked) { return; }
            var lab = inp.closest('label');
            if (!lab) { return; }
            var lt = lab.textContent || '';
            if (isWimdChoroLabel(lt)) {
              inp.click();
            }
          });
        }, true);
      }
      if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', wire);
      } else {
        wire();
      }
    })();
    </script>
    {% endmacro %}
    """
    js_macro = MacroElement()
    js_macro._template = Template(js)
    m.get_root().add_child(js_macro)# ---- Font size overrides ----
    css_fonts = """
{% macro html(this, kwargs) %}
<style>
/* Layer control (right panel) */
.leaflet-control-layers {
    font-size: 13px !important;
}
.leaflet-control-layers label {
    font-size: 13px !important;
    line-height: 1.2 !important;
}

/* Hover tooltip */
.leaflet-tooltip {
    font-size: 16px !important;
    line-height: 1.35 !important;
    padding: 7px 9px !important;
}

/* Popups */
.leaflet-popup-content {
    font-size: 15px !important;
    line-height: 1.35 !important;
}

/* Choropleth legend */
.legend, .leaflet-control .legend {
    font-size: 13px !important;
    line-height: 1.2 !important;
    padding: 7px 8px !important;
}
.legend .caption,
.leaflet-control .legend .caption {
    font-size: 14px !important;
    font-weight: 700 !important;
}
.legend-scale ul li,
.leaflet-control .legend-scale ul li {
    font-size: 12px !important;
}
</style>
{% endmacro %}
"""
    macro_fonts = MacroElement()
    macro_fonts._template = Template(css_fonts)
    m.get_root().add_child(macro_fonts)

    add_user_instruction_box(m, geo_level=geo_level, quarter=quarter)
    add_common_map_css(m)

    folium.LayerControl(collapsed=False).add_to(m)

    return m._repr_html_()


# ---------------------------
# Dash layout
# ---------------------------
try:
    # Use the LSOA BEV source because this dashboard now defaults to PCON/LSOA aggregation.
    _df = load_bev_lsoa_df()
    QUARTERS = available_quarters(_df)
except Exception:
    QUARTERS = []

if "2025 Q4" in QUARTERS:
    QUARTERS = ["2025 Q4"] + [q for q in QUARTERS if q != "2025 Q4"]
DEFAULT_QUARTER = QUARTERS[0] if QUARTERS else None

layout = html.Div(
    [
        back_button(),
        html.Div(
            dcc.Markdown("""<style>
            .thrust-field, .thrust-field * { font-size: 20px !important; }
            label { font-size: 21px !important; font-weight: 600; }
            button { font-size: 20px !important; font-weight: 600; }
            </style>""", dangerously_allow_html=True)
        ),
        html.H1("D) Clean and Equitable Transportation Solutions", style={"textAlign": "center", "marginBottom": "10px", "fontSize": "34px"}),
        html.P(
            "Interactive map combining BEV keepership, EV charger counts, and Income deprivation decile (1 = most deprived; 10 = least deprived).",
            style={"textAlign": "center"},
        ),
        html.Div(
            [
                html.Button("Download combined dataset", id="t1-download-button", n_clicks=0, style={"height": "38px"}),
                dcc.Download(id="t1-download-data"),
            ],
            style={"textAlign": "center", "margin": "15px"},
        ),
        html.Iframe(
            id="t1-map",
            srcDoc=(build_thrust_one_map(DEFAULT_QUARTER, geo_level="PCON", default_wimd_domain="Income", show_charging=True, show_centroids=False) if DEFAULT_QUARTER else folium.Map(location=[52.3, -3.8], zoom_start=7)._repr_html_()),
            style={"width": "100%", "height": "1260px", "border": "none"},
        ),
        html.Div(id="t1-info", style={"textAlign": "center", "marginTop": "10px"}),
    ]
)


# Map is rendered with fixed defaults: 2025 Q4, parliamentary constituency,
# charging points shown, centroid labels hidden. Visible controls were removed
# to simplify the dashboard for deployment.
@callback(
    Output("t1-download-data", "data"),
    Input("t1-download-button", "n_clicks"),
    prevent_initial_call=True,
)
def download_combined_dataset(n_clicks):
    quarter = "2025 Q4"
    geo_level = "PCON"
    try:
        df = load_data(COMBINED_DATASET_URL)
    except Exception:
        df = build_combined_dataset(quarter=quarter, geo_level=geo_level)
    filename = "ev_equity_combined_PCON_2025_Q4.csv"
    return dcc.send_data_frame(df.to_csv, filename, index=False)
