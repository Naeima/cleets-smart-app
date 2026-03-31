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
DATA_SOURCE = "https://drive.google.com/uc?id=1GMjkMXOI-wwHa4e4qHkiNNLdHrY-vbIu" #EV keepership Local Area District (LAD) level 
DATA_SOURCE_LSOA = "https://drive.google.com/uc?export=download&id=1A9gEvzfN9wbxmBdOx8VIo4kqCoM4OWY5" # EV keepership VEH0135 at Lower Layer Super Output Areas (LSOAs) are small, consistent geographic units used in England and Wales for reporting census and official statistics. Designed by the Office for National Statistics (ONS), they typically contain 400–1,200 households or 1,000–3,000 residents,
WIMD_URL = "https://drive.google.com/uc?export=download&id=1NC_Lds-IsMXNzy7x_PsRVzESPemoOLF0" # Wales Index of multiple (8 factors) deprevations.
CHARGE_URL = "https://drive.google.com/uc?export=download&id=1RFtC5hSEIrg5yG1rkmfD8JasAK6h212K" #charging points sourced from ONS as of October 2024. 
EV_COUNTS_URL = "https://drive.google.com/uc?id=1x5HKrqF4qyIBbUxAv3yeG5E22ich0go2&export=download" # Welsh local-authority EV charger counts as of January 26

# ONS lookup: LSOA21 -> LTLA22 (used if WIMD is LSOA-level)
LSOA_TO_LAD_FS = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
    "LSOA_2021_to_Ward_to_Lower_Tier_Local_Authority_May_2022_Lookup_for_England_2022/FeatureServer"
)
LSOA_TO_LAD_LAYER = "0"

# LAD boundaries
LAD_FS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Authority_Districts_May_2024_Boundaries_UK_BGC/FeatureServer"
LAD_LAYER = "0"

# LSOA boundaries (England & Wales LSOA 2021 BFE)
LSOA_FS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_2021_EW_BFE_V10_RUC/FeatureServer"
LSOA_LAYER = "3"

# Register as a page (like weather.py)
register_page(__name__, path="/thrust-one")


# ---------------------------
# Helpers
# ---------------------------
def _download(url: str) -> tuple[bytes, str]:
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
    if pd.isna(x):
        return np.nan
    s = str(x).replace(",", "").strip()
    return int(s) if s and s != "[z]" else np.nan


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
        "<table style='border-collapse:collapse; width:100%; font-size:12px;'>"
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
    lsoa_name_field = pick_field(lsoa_fields, ["lsoa21nm", "lsoa11nm", "lsoanm"])
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


def available_quarters(df: pd.DataFrame) -> list[str]:
    qs = [c for c in df.columns if re.match(r"^\d{4}\sQ[1-4]$", str(c).strip())]

    def _key(q):
        m = re.match(r"^(\d{4})\sQ([1-4])$", q)
        if not m:
            return (0, 0)
        return (int(m.group(1)), int(m.group(2)))

    return [q for q in sorted(qs, key=_key, reverse=True)]


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
    if geo_level not in {"LAD", "LSOA"}:
        geo_level = "LAD"

    df = (load_bev_lsoa_df() if geo_level == "LSOA" else load_bev_lad_df()).copy()
    df.columns = [str(c).strip() for c in df.columns]

    code_col = next((c for c in df.columns if c.strip().lower() in {"ons code", "ons_code", "onscode"}), None)
    name_col = next((c for c in df.columns if c.strip().lower() in {"ons geography", "ons_geography", "onsgeography"}), None)
    if code_col is None:
        raise ValueError("Couldn't find 'ONS Code' column in BEV dataset.")

    if quarter not in df.columns:
        raise ValueError(f"Quarter {quarter!r} not found in BEV dataset.")

    df["value"] = df[quarter].apply(to_int)

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

    # ---- WIMD layers ----
    WIMD_GRADIENT = {
        0.00: "#f7fbff",
        0.40: "#c6dbef",
        0.70: "#6baed6",
        0.90: "#2171b5",
        1.00: "#08306b",
    }

    preferred_domains = [
        "Income", "Employment", "Health", "Education",
        "Access to Services", "Community Safety", "Physical Environment", "Housing",
    ]
    domains_in_map = list(domain_value_by_code.keys())
    domains_sorted = [d for d in preferred_domains if d in domains_in_map] + [d for d in domains_in_map if d not in preferred_domains]

    from shapely.geometry import shape as _shape  # avoid any shadowing

    for dom in domains_sorted:
        val_by_lad = domain_value_by_code[dom]
        dmini = pd.DataFrame({"Area code": list(val_by_lad.keys()), "val": list(val_by_lad.values())})

        show_this = (default_wimd_domain is not None and str(dom).strip().lower() == str(default_wimd_domain).strip().lower())

        folium.Choropleth(
            geo_data=gj,
            data=dmini,
            columns=["Area code", "val"],
            key_on=f"feature.properties.{geo_code_field}",
            name=f"WIMD {dom} (choropleth)",
            fill_color="BuGn",
            fill_opacity=0.14,
            line_color="#6baed6",
            line_opacity=0.55,
            line_weight=1.2,
            legend_name=f"WIMD {dom} (rank; lower = more deprived)",
            show=show_this,
        ).add_to(m)

        pts = []
        for feat in gj["features"]:
            lad_code = str((feat.get("properties") or {}).get(geo_code_field))
            v = val_by_lad.get(lad_code)
            if v is None or not np.isfinite(v):
                continue
            c = _shape(feat["geometry"]).centroid
            pts.append([c.y, c.x, float(v)])

        if pts:
            HeatMap(
                pts,
                radius=35,
                blur=32,
                max_zoom=10,
                gradient=WIMD_GRADIENT,
                min_opacity=0.12,
                name=f"WIMD {dom} (heatmap)",
                show=False,
            ).add_to(m)

    # ---- Hover tooltip (values only; no codes) ----
    hover_gj = {"type": "FeatureCollection", "features": []}
    for feat in gj["features"]:
        props0 = feat.get("properties") or {}
        lad_code = str(props0.get(geo_code_field))
        lad_name = name_by_code.get(lad_code, props0.get(geo_name_field, lad_code))

        props = {("LSOA" if geo_level=="LSOA" else "LAD"): lad_name}
        bev = bev_by_code.get(lad_code)
        props[f"BEV ({quarter})"] = f"{int(bev):,}" if (bev is not None and np.isfinite(bev)) else "NA"
        evc = ev_counts_by_lad.get(lad_code) if geo_level == "LAD" else None
        if geo_level == "LAD":
            props["EV chargers (count)"] = f"{int(evc):,}" if (evc is not None and np.isfinite(evc)) else "NA"

        for dom in domains_sorted:
            v = domain_value_by_code[dom].get(lad_code)
            props[f"WIMD {dom} (rank)"] = f"{v:.0f}" if (v is not None and np.isfinite(v)) else "NA"

        hover_gj["features"].append({"type": "Feature", "geometry": feat["geometry"], "properties": props})

        geo_label = "LSOA" if geo_level=="LSOA" else "LAD"
    tooltip_fields = [geo_label, f"BEV ({quarter})"] + (["EV chargers (count)"] if geo_level == "LAD" else []) + [f"WIMD {dom} (rank)" for dom in domains_sorted]
    tooltip_aliases = [f"{geo_label}:", f"BEV ({quarter}):"] + (["EV chargers:"] if geo_level == "LAD" else []) + [f"{dom} rank:" for dom in domains_sorted]

    folium.GeoJson(
        hover_gj,
        name="Hover (BEV + WIMD values)",
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
        return (lblText || '').trim().startsWith('WIMD ') && (lblText || '').includes('(choropleth)');
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
    m.get_root().add_child(js_macro)

    folium.LayerControl(collapsed=False).add_to(m)

    return m._repr_html_()


# ---------------------------
# Dash layout
# ---------------------------
try:
    _df = load_bev_lad_df()
    QUARTERS = available_quarters(_df)
except Exception:
    QUARTERS = ["2025 Q3"]

layout = html.Div(
    [
        html.H1("D) Clean and Equitable Transportation Solutions", style={"textAlign": "center", "marginBottom": "10px"}),
        html.P(
            "Interactive map for Wales combining BEV keepership (selected quarter), WIMD 2025 deprivation ranks (by domain), and EV charging points.",
            style={"textAlign": "center"},
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("BEV quarter:"),
                        dcc.Dropdown(
                            id="t1-quarter",
                            options=[{"label": q, "value": q} for q in QUARTERS],
                            value=QUARTERS[0],
                            clearable=False,
                            style={"minWidth": "220px"},
                        ),
                    ],
                    style={"display": "inline-block", "verticalAlign": "top"},
                ),
                html.Div(
                    [
                        html.Label("Default visible WIMD domain layer:"),
                        dcc.Dropdown(
                            id="t1-wimd-default",
                            options=[{"label": d, "value": d} for d in [
                                "Income", "Employment", "Health", "Education",
                                "Access to Services", "Community Safety", "Physical Environment", "Housing",
                            ]],
                            value="Income",
                            clearable=True,
                            placeholder="None (hide by default)",
                            style={"minWidth": "280px"},
                        ),
                    ],
                    style={"display": "inline-block", "verticalAlign": "top", "marginLeft": "14px"},
                ),
                html.Div(
                    [
                        html.Label("Geography:"),
                        dcc.Dropdown(
                            id="t1-geo",
                            options=[
                                {"label": "Local authority district (LAD)", "value": "LAD"},
                                {"label": "Lower layer super output area (LSOA)", "value": "LSOA"},
                            ],
                            value="LAD",
                            clearable=False,
                            style={"minWidth": "280px"},
                        ),
                    ],
                    style={"display": "inline-block", "verticalAlign": "top", "marginLeft": "14px"},
                ),
                html.Div(
                    [
                        html.Label("Options:"),
                        dcc.Checklist(
                            id="t1-options",
                            options=[
                                {"label": "Show charging points", "value": "charging"},
                                {"label": "Enable centroid labels layer", "value": "centroids"},
                            ],
                            value=["charging", "centroids"],
                            style={"marginTop": "6px"},
                        ),
                    ],
                    style={"display": "inline-block", "verticalAlign": "top", "marginLeft": "14px"},
                ),
                html.Button("Update map", id="t1-refresh", n_clicks=0, style={"marginLeft": "14px", "height": "38px"}),
            ],
            style={"textAlign": "center", "margin": "15px"},
        ),
        html.Iframe(
            id="t1-map",
            srcDoc=build_thrust_one_map(QUARTERS[0], geo_level="LAD", default_wimd_domain="Income", show_charging=True, show_centroids=True),
            style={"width": "100%", "height": "1260px", "border": "none"},
        ),
        html.Div(id="t1-info", style={"textAlign": "center", "marginTop": "10px"}),
    ]
)


@callback(
    Output("t1-map", "srcDoc"),
    Output("t1-info", "children"),
    Input("t1-refresh", "n_clicks"),
    State("t1-quarter", "value"),
    State("t1-geo", "value"),
    State("t1-wimd-default", "value"),
    State("t1-options", "value"),
    prevent_initial_call=True,
)
def update_thrust_one_map(n_clicks, quarter, geo_level, wimd_default, options):
    options = options or []
    show_charging = "charging" in options
    show_centroids = "centroids" in options

    try:
        html_map = build_thrust_one_map(
            quarter,
            geo_level=geo_level or "LAD",
            default_wimd_domain=wimd_default if (wimd_default and str(wimd_default).strip()) else None,
            show_charging=show_charging,
            show_centroids=show_centroids,
        )
        msg = f"Updated at {time.strftime('%H:%M:%S')} (quarter: {quarter}; geography: {geo_level}). Use the Layer Control (top-right) to toggle domains and layers."
        return html_map, msg
    except Exception as e:
        fallback = folium.Map(location=[51.5, -3.2], zoom_start=7)._repr_html_()
        return fallback, f"Error building map: {e}"
