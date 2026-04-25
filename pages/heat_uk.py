# ============================================================
# CLEETS-SMART Dashboard B: Heat + Transport GHG (Local Authorities)
#   - Heat: decadal NetCDF layers (Google Drive via gdown)
#   - Heat time series:
#       (A) Daily UK-mean tas (CFTime/360-day safe, plotted on numeric axis with dd/mm/yyyy ticks)
#       (B) Annual anomaly per decade (baseline 1990–2000)
#       (C) Paris targets chart (mean anomaly with 1.5/2.0°C reference lines)
#   - Transport GHG: DESNZ LA GHG CSV (resolved via GOV.UK collection; same dataset surfaced in NAEI LA GHG app)
#   - GHG detail: Total + CO2 + CH4 + N2O (all attached to each LAD; choose one for choropleth)
#   - GHG visibility:
#       (A) Numeric values on map (centroid labels, ON by default)
#       (B) Discrete legend card with bin ranges + units
# ============================================================
from __future__ import annotations

import os
import re
import time
from functools import lru_cache
from typing import Dict, Any, Tuple, Optional

import requests
import numpy as np
import pandas as pd
import xarray as xr

import folium
from folium.plugins import MarkerCluster, HeatMap

import gdown
import branca.colormap as bcm

from dash import html, dcc, Input, Output, State, callback, register_page
import plotly.graph_objects as go

import matplotlib as mpl
import matplotlib.colors as mcolors

# Optional: geopandas for centroid labels (recommended)
try:
    import geopandas as gpd
    HAS_GPD = True
except Exception:
    HAS_GPD = False
    gpd = None  # type: ignore

# Dash page registration
register_page(__name__, path="/heat-uk")


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

# ============================================================
# 0. Heat datasets (Dafni NetCDF downloads)
# ============================================================

DATA_DIR = "heat_data"
os.makedirs(DATA_DIR, exist_ok=True)

HEAT_DATASETS = {
    # --- Historical ---
    "HEAT 1980–1990": {"fid": "1FenJCB_3nQIScCtg_c_7tBr0rkCcTEiZ", "path": os.path.join(DATA_DIR, "heat_1980_1990.nc")},  
    "HEAT 1990–2000": {"fid": "1Lm2wTq2NZCbBD8ZoKpKlgY_4P_AWgFN6", "path": os.path.join(DATA_DIR, "heat_1990_2000.nc")},
    "HEAT 2001–2010": {"fid": "1GZzUTycIngy1xgJUwwINdZFmQzkqj1rO", "path": os.path.join(DATA_DIR, "heat_2001_2010.nc")},
    "HEAT 2010–2020": {"fid": "18YV53RC6yxzkGe0tkBVBIa-p0PW6n-w9", "path": os.path.join(DATA_DIR, "heat_2010_2020.nc")},
    # --- Future ---
    "HEAT 2020–2030": {"fid": "1XfbVO0cnmNFC0oT-FyspAAypLlyN3Gcw", "path": os.path.join(DATA_DIR, "heat_2020_2030.nc")},
    "HEAT 2030–2040": {"fid": "1OOEUaevo0VVUE5MPZtFKrarfB0RK6Kec", "path": os.path.join(DATA_DIR, "heat_2030_2040.nc")},  #https://drive.google.com/file/d/1OOEUaevo0VVUE5MPZtFKrarfB0RK6Kec/view?usp=sharing
    "HEAT 2040–2050": {"fid": "128chSSQv_O9wiv_f3lBGppIQ3cGqGlPp", "path": os.path.join(DATA_DIR, "heat_2040_2050.nc")},  #https://drive.google.com/file/d/128chSSQv_O9wiv_f3lBGppIQ3cGqGlPp/view?usp=sharing
    "HEAT 2050–2060": {"fid": "10DrxTWyBM_D0wT5WIFJZN9tVpqZKpKhQ", "path": os.path.join(DATA_DIR, "heat_2050_2060.nc")},
    "HEAT 2060–2070": {"fid": "1ngGta9tgC6Pa5Or4LgYlViVEgnjanHyk", "path": os.path.join(DATA_DIR, "heat_2060_2070.nc")},
}

# Download once if missing
for label, meta in HEAT_DATASETS.items():
    if not os.path.exists(meta["path"]):
        target = meta["path"]
        os.makedirs(os.path.dirname(target), exist_ok=True)

        if not os.path.exists(target):
            gdown.download(
                f"https://drive.google.com/uc?id={meta['fid']}",
                target,
                quiet=False,
            )

        # gdown.download(f"https://drive.google.com/uc?id={meta['fid']}", meta["path"], quiet=False)

HEAT_FILES = {label: meta["path"] for label, meta in HEAT_DATASETS.items()}

HEAT_TS_COLORS = {
    "HEAT 1980–1990": "#7f7f7f",
    "HEAT 1990–2000": "#2ca02c",
    "HEAT 2001–2010": "#1f77b4",
    "HEAT 2010–2020": "#17becf",
    "HEAT 2020–2030": "#ff7f0e",
    "HEAT 2030–2040": "#d62728",
    "HEAT 2040–2050": "#9467bd",
    "HEAT 2050–2060": "#8c564b",
    "HEAT 2060–2070": "#e377c2",
}

TS_FONT_STYLE = {
    "base": 18,
    "title": 24,
    "axis_title": 20,
    "ticks": 16,
    "legend": 16,
    "hover": 16,
}


# ============================================================
# 1. Transport GHG (Local Authority)
# ============================================================

LAD_FEATURESERVER_URL = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
    "Local_Authority_Districts_December_2024_Boundaries_UK_BUC/FeatureServer/0"
)

# Resolve dataset from GOV.UK collection to avoid brittle "media hash" URLs
GOVUK_LAGHG_COLLECTION_URL = (
    "https://www.gov.uk/government/collections/"
    "uk-local-authority-and-regional-greenhouse-gas-emissions-statistics"
)

DEFAULT_GHG_YEAR = 2023

# Key: internal gas id -> (display name, property field)
GAS_FIELDS = {
    "total": ("Total GHG", "ghg_total"),
    "co2": ("CO₂", "ghg_co2"),
    "ch4": ("CH₄", "ghg_ch4"),
    "n2o": ("N₂O", "ghg_n2o"),
}

# ============================================================
# Map rendering / latency controls
# ============================================================
# Smaller iframe reduces the visible map footprint. Turning off dense value-label
# marker layers avoids Leaflet/Folium rendering failures and materially lowers
# callback latency. Re-enable only for focused diagnostics.
MAP_HEIGHT = "620px"
ENABLE_HEAT_VALUE_LABELS = 0
ENABLE_GHG_VALUE_LABELS = 0
ENABLE_POLICY_TARGETING = 1

# ============================================================
# 2. Map utilities
# ============================================================

HEAT_GRID_STEP = 8
HEAT_LABEL_STEP = 18
HEAT_VMIN, HEAT_VMAX = 5.0, 25.0

# Heat colormap for the heat surface (Matplotlib cmap name)
HEAT_CMAP = "turbo"

CITY_PRESETS = {
    "Cardiff": (51.4816, -3.1791),
    "Swansea": (51.6214, -3.9436),
    "Newport": (51.5842, -2.9977),
    "Wrexham": (53.0465, -2.9938),
    "Bangor": (53.2280, -4.1290),
    "Aberystwyth": (52.4140, -4.0829),
    "UK (centre)": (54.5, -2.5),
}

def add_base_tiles(m: folium.Map) -> None:
    folium.TileLayer(
        tiles="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        name="OpenStreetMap",
        attr="© OpenStreetMap contributors",
        overlay=False,
        control=True,
        max_zoom=19,
    ).add_to(m)

    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
        name="CartoDB Positron",
        attr="© OpenStreetMap contributors, © CARTO",
        overlay=False,
        control=True,
        max_zoom=19,
    ).add_to(m)

    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        name="CartoDB Dark Matter",
        attr="© OpenStreetMap contributors, © CARTO",
        overlay=False,
        control=True,
        max_zoom=19,
    ).add_to(m)

    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        name="Esri World Imagery",
        attr="Tiles © Esri & contributors",
        overlay=False,
        control=True,
        max_zoom=19,
    ).add_to(m)

    folium.TileLayer(
        tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
        name="OpenTopoMap",
        attr="© OpenStreetMap contributors, SRTM; style © OpenTopoMap (CC-BY-SA)",
        overlay=False,
        control=True,
        max_zoom=17,
    ).add_to(m)

    folium.TileLayer(
        tiles="https://{s}.tile-cyclosm.openstreetmap.fr/cyclosm/{z}/{x}/{y}.png",
        name="CyclOSM (transport)",
        attr="© OpenStreetMap contributors, tiles courtesy of CyclOSM",
        overlay=False,
        control=True,
        max_zoom=19,
    ).add_to(m)

def value_to_hex(val: float, vmin=HEAT_VMIN, vmax=HEAT_VMAX, cmap=HEAT_CMAP) -> str:
    norm = mcolors.Normalize(vmin=vmin, vmax=vmax, clip=True)
    rgba = mpl.colormaps[cmap](norm(float(val)))
    return mcolors.to_hex(rgba, keep_alpha=False)

def _requests_get(url: str, *, timeout: int = 120) -> requests.Response:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "CLEETS-SMART/1.0"})
    r.raise_for_status()
    return r

# ============================================================
# 3. Heat: load field + time series
# ============================================================

DAILY_PLOT_STEP = 3  # plot every Nth timestep to keep dashboard responsive

def _tas_to_celsius(da: xr.DataArray) -> xr.DataArray:
    units = str(da.attrs.get("units", "")).strip().lower()
    if units in {"k", "kelvin", "degk"} or ("kelvin" in units):
        return da - 273.15
    vmax = float(da.max())
    return da - 273.15 if vmax > 100 else da

def _time_to_ddmmyyyy_str(t) -> str:
    return f"{t.day:02d}/{t.month:02d}/{t.year:04d}"

@lru_cache(maxsize=64)
def load_heat_field(decade_label: str) -> Dict[str, Any]:
    """
    Returns sampled grid for the selected decade:
      {"lat": lat2d, "lon": lon2d, "z": z2d}
    """
    path = HEAT_FILES[decade_label]
    ds = xr.open_dataset(path, engine="netcdf4")

    tas = _tas_to_celsius(ds["tas"]).isel(time=0).mean(dim=["ensemble_member"])
    lat = np.array(tas["projection_y_coordinate"])
    lon = np.array(tas["projection_x_coordinate"])
    vals = np.array(tas)

    yi = np.arange(0, len(lat), HEAT_GRID_STEP)
    xi = np.arange(0, len(lon), HEAT_GRID_STEP)

    lat_s = lat[yi]
    lon_s = lon[xi]
    z_s = vals[np.ix_(yi, xi)]

    LAT, LON = np.meshgrid(lat_s, lon_s, indexing="ij")
    return {"lat": LAT, "lon": LON, "z": z_s}

@lru_cache(maxsize=64)
def daily_uk_mean_series_for_file(path: str) -> pd.DataFrame:
    """
    Daily UK-mean tas time series (°C), CFTime-safe.

    Plotting:
      - Uses numeric x-axis = 'timestep' (pre-decimation index)
      - dd/mm/yyyy labels are carried in 'date_label' for hover/ticks
    """
    ds = xr.open_dataset(path, engine="netcdf4")
    tas = _tas_to_celsius(ds["tas"])

    if "time" not in tas.dims:
        raise ValueError(f"'time' dimension not found in tas: dims={tas.dims}")

    spatial_dims = [d for d in tas.dims if d != "time"]
    ts = tas.mean(dim=spatial_dims, skipna=True)

    df = ts.to_dataframe(name="tas").reset_index()

    df["date_label"] = df["time"].apply(_time_to_ddmmyyyy_str)

    df["month_360"] = df["time"].apply(lambda t: int(getattr(t, "month", 0)) if hasattr(t, "month") else None)
    df["day_of_month"] = df["time"].apply(lambda t: int(getattr(t, "day", 0)) if hasattr(t, "day") else None)
    df["day_of_year_360"] = df["time"].apply(
        lambda t: (int(getattr(t, "month", 0)) - 1) * 30 + int(getattr(t, "day", 0))
        if hasattr(t, "month") and hasattr(t, "day") else None
    )

    df["timestep"] = np.arange(len(df), dtype=int)

    if DAILY_PLOT_STEP and DAILY_PLOT_STEP > 1:
        df = df.iloc[::DAILY_PLOT_STEP, :].reset_index(drop=True)

    return df[["timestep", "date_label", "tas", "month_360", "day_of_month", "day_of_year_360"]]

def build_daily_uk_mean_chart(selected_decades: Optional[list[str]] = None) -> go.Figure:
    """
    Daily UK-mean tas time series (°C).

    Plotted sequentially by decade-series to avoid overlapping x-values across decades.
    X-axis is a continuous 'sequential timestep' that concatenates the selected decades
    in the order they appear in HEAT_FILES.
    """
    selected_decades = selected_decades or list(HEAT_FILES.keys())

    fig = go.Figure()
    tickvals: list[float] = []
    ticktext: list[str] = []

    offset = 0
    for lab in HEAT_FILES.keys():
        if lab not in selected_decades:
            continue

        df = daily_uk_mean_series_for_file(HEAT_FILES[lab])
        if df.empty:
            continue

        n = len(df)
        df = df.copy()
        df["x_seq"] = np.arange(n, dtype=float) + float(offset)

        # mark the start of each decade block on the x-axis
        tickvals.append(float(offset))
        ticktext.append(lab)

        fig.add_trace(
            go.Scatter(
                x=df["x_seq"],
                y=df["tas"],
                mode="lines",
                name=lab,
                line=dict(color=HEAT_TS_COLORS.get(lab, "#333333"), width=2),
                customdata=df[["date_label", "month_360", "day_of_month", "day_of_year_360", "timestep"]].to_numpy(),
                hovertemplate=(
                    "<b>Period:</b> " + lab + "<br>"
                    "<b>Date:</b> %{customdata[0]}<br>"
                    "<b>Month (360d):</b> %{customdata[1]:.0f}<br>"
                    "<b>Day (of month):</b> %{customdata[2]:.0f}<br>"
                    "<b>Day-of-year (360d):</b> %{customdata[3]:.0f}<br>"
                    "<b>Timestep (pre-decimation):</b> %{customdata[4]}<br>"
                    "<b>Temperature:</b> %{y:.2f} °C"
                    "<extra></extra>"
                ),
            )
        )

        offset += n

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=55, r=35, t=70, b=55),
        title=dict(
            text=(
                "Daily mean near-surface air temperature (tas) — UK mean<br>"
                "<sup>"
                "Source: DAFNI NetCDF layers (downloaded via Google Drive). "
                "Method: for each timestep, tas is averaged over the full grid and ensemble_member; "
                "series are concatenated sequentially by decade to avoid overlapping x-values."
                "</sup>"
            ),
            font=dict(size=TS_FONT_STYLE["title"]),
        ),
        xaxis_title="Decade blocks (concatenated sequentially)",
        yaxis_title="Temperature (°C)",
        font=dict(size=TS_FONT_STYLE["base"]),
        xaxis=dict(title_font=dict(size=TS_FONT_STYLE["axis_title"]), tickfont=dict(size=TS_FONT_STYLE["ticks"])),
        yaxis=dict(title_font=dict(size=TS_FONT_STYLE["axis_title"]), tickfont=dict(size=TS_FONT_STYLE["ticks"])),
        hoverlabel=dict(font_size=TS_FONT_STYLE["hover"]),
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.25, font=dict(size=TS_FONT_STYLE["legend"])),
    )

    if tickvals and ticktext:
        fig.update_xaxes(tickmode="array", tickvals=tickvals, ticktext=ticktext)

    return fig




# ---------------- Improved annual temperature + anomaly pipeline (monthly stitching) ----------------
# Rationale:
#   Decadal NetCDF files may split a calendar year across two files (e.g., one file contains only December,
#   the next contains Jan–Nov). If you QC "12 months per year" *within each file*, you can incorrectly drop
#   legitimate years (e.g., 2020). The fix is:
#     1) build a continuous UK-mean *monthly* series across all files,
#     2) resolve overlapping year-months across files (median),
#     3) aggregate to annual means, keeping only years with 12 months.

def _ym_from_time_values(time_values) -> tuple[np.ndarray, np.ndarray]:
    """Return (year, month) int arrays from CFTime or datetime-like objects."""
    years: list[int] = []
    months: list[int] = []
    for t in time_values:
        years.append(int(getattr(t, "year")))
        months.append(int(getattr(t, "month")))
    return np.asarray(years, dtype=int), np.asarray(months, dtype=int)


def _parse_decade_bounds(decade_label: str) -> tuple[int, int]:
    """
    Parse labels like 'HEAT 2040–2050' or 'HEAT 2040-2050'.

    Convention used here:
      - map/file label end year is exclusive for slicing annual series
      - e.g. '2040–2050' returns years 2040..2049
    This avoids double-counting boundary years across adjacent decade files.
    """
    m = re.search(r"(\d{4}).*?(\d{4})", decade_label)
    if not m:
        raise ValueError(f"Could not parse years from label: {decade_label}")
    start_year = int(m.group(1))
    end_year = int(m.group(2))
    if end_year <= start_year:
        raise ValueError(f"Invalid decade bounds in label: {decade_label}")
    return start_year, end_year


def _monthly_series_for_file(path: str) -> pd.Series:
    """Monthly UK-mean tas (°C) as a Series indexed by (year, month)."""
    ds = xr.open_dataset(path, engine="netcdf4")
    tas = _tas_to_celsius(ds["tas"])

    if "time" not in tas.dims:
        raise ValueError(f"'time' dimension not found in tas: dims={tas.dims}")

    spatial_dims = [d for d in tas.dims if d != "time"]
    ts = tas.mean(dim=spatial_dims, skipna=True)  # UK mean per timestep

    # Build (year, month) for each timestep, then average within each month
    years, months = _ym_from_time_values(ts["time"].values)
    df = pd.DataFrame({
        "year": years,
        "month": months,
        "tas": np.asarray(ts.values, dtype=float),
    })
    df = df[np.isfinite(df["tas"].to_numpy())].copy()

    monthly = df.groupby(["year", "month"])["tas"].mean()
    monthly.index = pd.MultiIndex.from_tuples(
        [(int(y), int(m)) for (y, m) in monthly.index], names=["year", "month"]
    )
    monthly.name = "tas_monthly_uk"
    return monthly.sort_index()


@lru_cache(maxsize=2)
def _continuous_monthly_series() -> pd.Series:
    """Continuous monthly UK series across all HEAT_FILES; overlaps merged by median."""
    parts = []
    for p in HEAT_FILES.values():
        parts.append(_monthly_series_for_file(p))

    combined = pd.concat(parts)
    # median across duplicated (year,month) from overlapping files
    merged = combined.groupby(level=[0, 1]).median()
    merged.name = "tas_monthly_uk"
    return merged.sort_index()


@lru_cache(maxsize=2)
def _continuous_annual_series() -> pd.Series:
    """
    Annual means from the continuous monthly series.

    Keep years with at least 12 distinct months. If overlap resolution leaves more
    than 12 monthly entries impossible? no, because overlaps are merged upstream by
    (year, month). This function is intentionally strict so incomplete boundary years
    do not silently disappear without a defined rule.
    """
    m = _continuous_monthly_series()

    month_counts = m.groupby(level=0).size()
    annual = m.groupby(level=0).mean()

    keep_years = month_counts[month_counts >= 12].index.astype(int)
    annual = annual.loc[keep_years]
    annual.index = annual.index.astype(int)
    annual.name = "tas_annual_uk"
    return annual.sort_index()


@lru_cache(maxsize=2)
def _baseline_temp_1990_2000() -> float:
    s = _continuous_annual_series()
    base = s.loc[1990:2000]
    return float(base.mean())


@lru_cache(maxsize=2)
def _continuous_anomaly_series() -> pd.Series:
    s = _continuous_annual_series()
    baseline = _baseline_temp_1990_2000()
    out = s - baseline
    out.name = "anom_annual_uk"
    return out


@lru_cache(maxsize=64)
def decade_anomaly_series(decade_label: str) -> pd.Series:
    """
    Decade slice of the cleaned continuous anomaly series.

    The file labels are treated as half-open intervals [start, end), so
    'HEAT 2040–2050' maps to years 2040..2049. This prevents the shared
    boundary year from being visually assigned to two adjacent decades.
    """
    anom = _continuous_anomaly_series()
    y0, y1 = _parse_decade_bounds(decade_label)
    s = anom[(anom.index.astype(int) >= y0) & (anom.index.astype(int) < y1)].copy()
    s.name = decade_label
    return s

# --------------------------------------------------------------------------------




def build_decade_separated_anomaly_chart(selected_decades: Optional[list[str]] = None) -> go.Figure:
    selected_decades = selected_decades or list(HEAT_FILES.keys())
    fig = go.Figure()

    anom = _continuous_anomaly_series()

    fig.add_trace(
        go.Scatter(
            x=anom.index.astype(int),
            y=anom.values,
            mode="lines+markers",
            name="Annual anomaly (continuous)",
            line=dict(width=3, color="#111111"),
            hovertemplate="Year: %{x}<br>ΔT: %{y:.2f} °C<extra></extra>",
        )
    )

    for lab in HEAT_FILES.keys():
        if lab not in selected_decades:
            continue

        s = decade_anomaly_series(lab)
        if s.empty:
            continue

        fig.add_trace(
            go.Scatter(
                x=s.index.astype(int),
                y=s.values,
                mode="lines+markers",
                name=lab,
                visible=True,
                line=dict(color=HEAT_TS_COLORS.get(lab, "#333333"), width=2),
                hovertemplate="<b>%{fullData.name}</b><br>Year: %{x}<br>ΔT: %{y:.2f} °C<extra></extra>",
            )
        )

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=50, r=40, t=70, b=55),
        title=dict(
            text=(
                "Annual mean temperature anomaly — continuous series (baseline 1990–2000)<br>"
                "<sup>"
                "Method: UK-mean tas → monthly means per (year, month) → stitch across decadal files (median for overlaps) "
                "→ annual means (years with 12 months only) → anomaly relative to 1990–2000. "
                "Decade overlays use half-open ranges [start, end), so boundary years are not double-counted."
                "</sup>"
            ),
            font=dict(size=TS_FONT_STYLE["title"]),
        ),
        yaxis_title="Temperature anomaly (°C)",
        xaxis_title="Year",
        font=dict(size=TS_FONT_STYLE["base"]),
        xaxis=dict(title_font=dict(size=TS_FONT_STYLE["axis_title"]), tickfont=dict(size=TS_FONT_STYLE["ticks"])),
        yaxis=dict(title_font=dict(size=TS_FONT_STYLE["axis_title"]), tickfont=dict(size=TS_FONT_STYLE["ticks"])),
        hoverlabel=dict(font_size=TS_FONT_STYLE["hover"]),
        legend=dict(orientation="h", y=-0.25, font=dict(size=TS_FONT_STYLE["legend"])),
    )
    fig.update_xaxes(type="linear", tickmode="auto")
    return fig



def build_paris_targets_chart(selected_decades: Optional[list[str]] = None) -> go.Figure:
    """
    Annual anomaly series with Paris thresholds.
    Built from monthly stitching across files; annual values require 12 months.
    If selected_decades is provided, restrict to the union of those decade ranges.
    """
    selected_decades = selected_decades or list(HEAT_FILES.keys())

    anom = _continuous_anomaly_series()

    years_keep: set[int] = set()
    for lab in HEAT_FILES.keys():
        if lab not in selected_decades:
            continue
        y0, y1 = _parse_decade_bounds(lab)
        years_keep.update(range(y0, y1))

    if years_keep:
        idx = [int(y) for y in anom.index.astype(int) if int(y) in years_keep]
        anom = anom.loc[idx]

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=anom.index.astype(int),
            y=anom.values,
            mode="lines+markers",
            name="Annual anomaly (continuous)",
            line=dict(width=3),
            hovertemplate="Year: %{x}<br>ΔT: %{y:.2f} °C<extra></extra>",
        )
    )

    fig.add_hline(
        y=1.5,
        line_dash="dash",
        annotation_text="1.5 °C (Paris target)",
        annotation_position="top left",
    )
    fig.add_hline(
        y=2.0,
        line_dash="dash",
        annotation_text="2.0 °C (Paris upper limit)",
        annotation_position="top left",
    )

    fig.update_layout(
        title=dict(
            text=(
                "Rise in Average Temperature Relative to Paris Agreement Targets<br>"
                "<sup>"
                "Method: monthly stitching across decadal files; annual values require 12 months; "
                "anomalies relative to 1990–2000."
                "</sup>"
            ),
            font=dict(size=TS_FONT_STYLE["title"]),
        ),
        xaxis_title="Year",
        yaxis_title="Temperature Anomaly (°C relative to 1990–2000)",
        template="plotly_white",
        margin=dict(l=55, r=35, t=70, b=55),
        font=dict(size=TS_FONT_STYLE["base"]),
        xaxis=dict(title_font=dict(size=TS_FONT_STYLE["axis_title"]), tickfont=dict(size=TS_FONT_STYLE["ticks"])),
        yaxis=dict(title_font=dict(size=TS_FONT_STYLE["axis_title"]), tickfont=dict(size=TS_FONT_STYLE["ticks"])),
        hoverlabel=dict(font_size=TS_FONT_STYLE["hover"]),
        legend=dict(orientation="h", y=-0.25, font=dict(size=TS_FONT_STYLE["legend"])),
    )
    fig.update_xaxes(type="linear", tickmode="auto")
    return fig

# ============================================================
# 4. Transport GHG: resolve CSV (GOV.UK) + load + pivot gases
# ============================================================

@lru_cache(maxsize=1)
def resolve_latest_govuk_laghg_stats_page_url() -> str:
    html0 = _requests_get(GOVUK_LAGHG_COLLECTION_URL, timeout=60).text
    m = re.search(
        r'href="(/government/statistics/uk-local-authority-and-regional-greenhouse-gas-emissions[^"]+)"',
        html0,
        flags=re.IGNORECASE,
    )
    if not m:
        raise RuntimeError("Could not resolve latest GOV.UK LA GHG stats page from the GOV.UK collection.")
    return "https://www.gov.uk" + m.group(1)

@lru_cache(maxsize=1)
def resolve_latest_laghg_csv_url() -> str:
    stats_url = resolve_latest_govuk_laghg_stats_page_url()
    html0 = _requests_get(stats_url, timeout=60).text

    abs_assets = set(re.findall(r"https://assets\.publishing\.service\.gov\.uk/[^\s\"<>]+\.csv", html0))
    rel_uploads = set(re.findall(r"(/government/uploads/system/uploads/attachment_data/file/[^\s\"<>]+\.csv)", html0))

    candidates = set(abs_assets)
    candidates.update(("https://www.gov.uk" + u) for u in rel_uploads)

    if not candidates:
        raise RuntimeError(f"Could not find CSV asset links on: {stats_url}")

    cand_list = sorted(candidates)
    preferred = [u for u in cand_list if ("local-authority" in u.lower() and "ghg" in u.lower())]
    return preferred[0] if preferred else cand_list[0]

@lru_cache(maxsize=2)
def load_ghg_la_raw() -> pd.DataFrame:
    cache_path = os.path.join(DATA_DIR, "la_ghg_latest.csv")
    if os.path.exists(cache_path) and (time.time() - os.path.getmtime(cache_path)) < 14 * 86400:
        return pd.read_csv(cache_path)

    csv_url = resolve_latest_laghg_csv_url()
    r = _requests_get(csv_url, timeout=180)
    with open(cache_path, "wb") as f:
        f.write(r.content)
    return pd.read_csv(cache_path)

@lru_cache(maxsize=2)
def load_lad_geojson() -> Dict[str, Any]:
    query_url = f"{LAD_FEATURESERVER_URL}/query"
    params = {"where": "1=1", "outFields": "*", "f": "geojson", "resultRecordCount": 5000}
    r = requests.get(query_url, params=params, timeout=180, headers={"User-Agent": "CLEETS-SMART/1.0"})
    r.raise_for_status()
    return r.json()

def _find_col(df: pd.DataFrame, predicates: list) -> Optional[str]:
    cols = [str(c).strip() for c in df.columns]
    for c in cols:
        cl = c.lower()
        if all(p(cl) for p in predicates):
            return c
    return None

def _gas_key_from_text(g: str) -> Optional[str]:
    gl = str(g).strip().lower()

    if "total" in gl and ("greenhouse" in gl or "ghg" in gl):
        return "total"
    if "carbon dioxide" in gl or re.search(r"\bco2\b", gl):
        return "co2"
    if "methane" in gl or re.search(r"\bch4\b", gl):
        return "ch4"
    if "nitrous oxide" in gl or re.search(r"\bn2o\b", gl):
        return "n2o"
    return None

def load_transport_ghg_gases(year: int, metric: str) -> Tuple[pd.DataFrame, str]:
    df = load_ghg_la_raw().copy()
    df.columns = [str(c).strip() for c in df.columns]

    col_year = next((c for c in df.columns if c.lower() in ("calendar year", "year")), None)
    col_code = _find_col(df, [lambda s: "local authority" in s, lambda s: "code" in s])
    col_name = _find_col(df, [lambda s: "local authority" in s, lambda s: "name" in s])
    col_sector = _find_col(df, [lambda s: "sector" in s])
    col_gas = _find_col(df, [lambda s: "greenhouse gas" in s]) or _find_col(df, [lambda s: "gas" in s])
    col_kt = _find_col(df, [lambda s: "emissions" in s, lambda s: "(kt" in s])
    col_pc = _find_col(df, [lambda s: "per capita" in s, lambda s: "co2e" in s]) or _find_col(df, [lambda s: "per capita" in s])

    if not (col_year and col_code and col_sector and col_kt and col_gas):
        raise RuntimeError(
            "Could not locate required columns in the LA GHG CSV. "
            f"Found year={col_year}, code={col_code}, sector={col_sector}, kt={col_kt}, gas={col_gas}."
        )

    df[col_year] = pd.to_numeric(df[col_year], errors="coerce")
    d = df[df[col_year] == int(year)].copy()
    d = d[d[col_sector].astype(str).str.strip().str.lower().eq("transport")]

    if metric == "per_capita" and col_pc:
        d[col_pc] = pd.to_numeric(d[col_pc], errors="coerce")
        num_col = col_pc
        units = "t CO₂e per capita"
    else:
        d[col_kt] = pd.to_numeric(d[col_kt], errors="coerce")
        num_col = col_kt
        units = "kt CO₂e"

    d["_gas_key"] = d[col_gas].apply(_gas_key_from_text)
    d = d.dropna(subset=["_gas_key", num_col])
    d = d[d["_gas_key"].isin(["total", "co2", "ch4", "n2o"])].copy()

    d["LADCD"] = d[col_code].astype(str).str.strip()
    d["LADNM"] = d[col_name].astype(str).str.strip() if col_name else d["LADCD"]

    wide = (
        d.pivot_table(
            index=["LADCD", "LADNM"],
            columns="_gas_key",
            values=num_col,
            aggfunc="max",
        )
        .reset_index()
    )

    for k in ["total", "co2", "ch4", "n2o"]:
        if k not in wide.columns:
            wide[k] = np.nan

    return wide[["LADCD", "LADNM", "total", "co2", "ch4", "n2o"]], units

def inject_ghg_gases_into_geojson(gj: Dict[str, Any], ghg_df: pd.DataFrame) -> Dict[str, Any]:
    lut_total = dict(zip(ghg_df["LADCD"], ghg_df["total"]))
    lut_co2 = dict(zip(ghg_df["LADCD"], ghg_df["co2"]))
    lut_ch4 = dict(zip(ghg_df["LADCD"], ghg_df["ch4"]))
    lut_n2o = dict(zip(ghg_df["LADCD"], ghg_df["n2o"]))

    out = gj.copy()
    for feat in out.get("features", []):
        props = feat.get("properties", {})
        code = str(props.get("LAD24CD") or props.get("LADCD") or "").strip()

        def _f(v):
            return float(v) if v is not None and pd.notna(v) else None

        props["ghg_total"] = _f(lut_total.get(code))
        props["ghg_co2"] = _f(lut_co2.get(code))
        props["ghg_ch4"] = _f(lut_ch4.get(code))
        props["ghg_n2o"] = _f(lut_n2o.get(code))

        feat["properties"] = props
    return out

# ============================================================
# 5. Folium renderers (Heat + GHG)
# ============================================================

def _mpl_gradient(cmap_name: str = HEAT_CMAP, n: int = 9) -> dict:
    cm = mpl.colormaps[cmap_name]
    stops = np.linspace(0, 1, n)
    return {float(s): mcolors.to_hex(cm(s), keep_alpha=False) for s in stops}

def add_map_layers_panel(m: folium.Map) -> None:
    # Updated: removed the yellow "Targeting adaptation" overlay from the panel list
    html0 = """
    <div style="position: fixed; top: 12px; left: 12px; z-index: 9999;
                background: rgba(255,255,255,0.95); padding: 10px 12px;
                border: 1px solid #cfcfcf; border-radius: 10px; width: 360px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.10);
                font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
                font-size: 13px; line-height: 1.35;">
      <div style="font-weight: 900; font-size: 14px; margin-bottom: 6px;">Map layers</div>

      <div style="font-weight: 800; margin-top: 6px;">Basemaps (choose one)</div>
      <ul style="margin: 6px 0 0 18px; padding: 0;">
        <li>OpenStreetMap</li>
        <li>CartoDB Positron</li>
        <li>CartoDB Dark Matter</li>
        <li>Esri World Imagery</li>
        <li>OpenTopoMap</li>
        <li>CyclOSM (transport)</li>
      </ul>

      <div style="font-weight: 800; margin-top: 10px;">Overlays (toggle on/off)</div>
      <ul style="margin: 6px 0 0 18px; padding: 0;">
        <li><b>Heat: HeatMap (tas)</b> — kernel-smoothed surface from gridded temperature (weights normalised)</li>
        <li><b>Heat: value labels</b> — sampled gridpoint °C labels (MarkerCluster; off by default)</li>
        <li><b>GHG: transport choropleth</b> — LAD polygons coloured by selected gas (Total/CO₂/CH₄/N₂O)</li>
        <li><b>GHG: values on map</b> — centroid numeric labels for selected gas (MarkerCluster; on by default)</li>
        <li><b>GHG: LA boundaries</b> — outline-only LAD boundaries (off by default)</li>
        <li><b>Co-benefits</b> — LADs in the top quartile for both transport emissions and temperature exposure proxy</li>
      </ul>

      <div style="margin-top: 10px; color:#555;">
        <b>Controls:</b> Use the layer control (top-right) to switch basemaps and overlays.
      </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(html0))

def add_left_context_panel(
    m: folium.Map,
    *,
    decade_label: str,
    ghg_year: int,
    ghg_metric: str,
    ghg_gas: str,
    ghg_units: str,
    targeting_summary: Optional[Dict[str, Any]] = None,
) -> None:
    """Left-side panel: data sources, modelling steps, and intended decision summary."""
    gas_name, _ = GAS_FIELDS.get(ghg_gas, GAS_FIELDS["total"])
    metric_label = "kt CO₂e" if ghg_metric == "kt" else "t CO₂e per capita"

    decision_html = """
    <div style="margin-top:10px; font-size:18px; line-height:1.65; color:#666;">
      <div style="font-weight:900; margin-bottom:4px;">Intended decision summary</div>

        <li>Identify local areas (LADs) that are high on transport emissions, high on temperature exposure, or high on both, so policymakers can target action.

        Input:

        <li>Transport emissions for each LAD (for the chosen year/metric/gas).

        <li>Temperature exposure proxy for each LAD: the temperature value at the LAD’s centroid (nearest climate gridpoint), for the chosen decade.

        Calculation:

        <li>Pick a cutoff like the 75th percentile (top 25%).

        <li>Emissions cutoff = value that separates the highest-emitting 25% of LADs from the rest.

        <li>Temperature cutoff = value that separates the hottest 25% of LADs from the rest.

        <li>Label each LAD (flags)

        <li>High emissions if its emissions are in the top 25% (≥ emissions cutoff).

        <li>High temperature if its temperature is in the top 25% (≥ temperature cutoff).

        <li>Co-benefits if it is both high emissions and high temperature (so interventions could reduce emissions and help in hotter-risk areas).
    </div>
    """

    if targeting_summary is not None:
        q = float(targeting_summary.get("q_percentile", 75.0))
        g_thr = float(targeting_summary.get("ghg_threshold", float("nan")))
        t_thr = float(targeting_summary.get("temp_threshold", float("nan")))
        n_total = int(targeting_summary.get("n_total", 0))
        n_hi_ghg = int(targeting_summary.get("n_high_ghg", 0))
        n_hi_tmp = int(targeting_summary.get("n_high_temp", 0))
        n_both = int(targeting_summary.get("n_both", 0))

        decision_html = f"""
        <div style="margin-top:8px; font-size:18px; line-height:1.35;">
          <div style="font-weight:900; margin-bottom:4px;">Intended decision summary</div>

          <div style="margin-top:6px;">
            <b>Inputs</b>
            <div>• Transport emissions for each LAD (for the selected year/metric/gas).</div>
            <div>• Temperature exposure proxy for each LAD (<b>tas_centroid_c</b>: nearest gridpoint temperature at the LAD centroid, for the selected decade).</div>
          </div>

          <div style="margin-top:8px;">
            <b>Calculations (intended)</b>
            <div>• Compute thresholds at a chosen percentile (q={q:.0f}; i.e., the {q:.0f}th percentile / top quartile):</div>
            <div style="margin-left:14px;">– ghg_threshold = percentile(transport_emissions, {q:.0f})</div>
            <div style="margin-left:14px;">– temp_threshold = percentile(tas_centroid_c, {q:.0f})</div>
          </div>

          <div style="margin-top:8px;">
            <b>Flags per LAD</b>
            <div style="margin-left:14px;">– High emissions if emissions ≥ ghg_threshold</div>
            <div style="margin-left:14px;">– High temperature if temperature ≥ temp_threshold</div>
            <div style="margin-left:14px;">– Co-benefits if both are true</div>
          </div>

          <div style="margin-top:8px;">
            <b>Thresholds (computed)</b><br/>
            – Transport emissions ≥ <b>{g_thr:,.2f} {ghg_units}</b><br/>
            – Temperature ≥ <b>{t_thr:,.2f} °C</b> ({decade_label})
          </div>

          <div style="margin-top:6px;">
            <b>Counts (LADs with valid emissions + temperature)</b>: <b>{n_total}</b><br/>
            – High emissions: <b>{n_hi_ghg}</b><br/>
            – High temperature: <b>{n_hi_tmp}</b><br/>
            – Co-benefits (both): <b>{n_both}</b>
          </div>
        </div>

        """

    html0 = f"""
    <div style="position: fixed; top: 12px; left: 12px; z-index: 9999;
                background: rgba(255,255,255,0.95); padding: 10px 12px;
                border: 1px solid #cfcfcf; border-radius: 10px; width: 430px;
                max-height: 360px; overflow-y: auto;
                box-shadow: 0 2px 8px rgba(0,0,0,0.10);
                font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
                font-size: 18px; line-height: 1.35;">
      <div style="font-weight: 900; font-size: 18px; margin-bottom: 6px;">
        Data sources, modelling, and decision summary
      </div>

      <div style="color:#444; margin-bottom:6px;">
        This panel summarises <b>where the data come from</b>, <b>how layers are derived</b>, and the <b>intended decision summary</b>
        used for targeting (thresholds + counts).
      </div>

      <div style="font-weight: 800; margin-top: 6px;">Transport emissions (Local Authority Districts)</div>
      <div>• Source: DESNZ “UK local authority and regional greenhouse gas emissions statistics” (transport sector).</div>
      <div>• Filters: <b>{ghg_year}</b>, <b>{metric_label}</b>, gas: <b>{gas_name}</b> ({ghg_units}).</div>
      <div>• Processing: sector=Transport → pivot gases → join to Dec-2024 LAD boundaries by code.</div>

      <div style="font-weight: 800; margin-top: 10px;">Temperature (tas)</div>
      <div>• Source: DAFNI decadal NetCDF layers (near-surface air temperature, tas).</div>
      <div>• Processing: Kelvin→°C if needed; map shows a smoothed surface from a subsampled grid (selected: <b>{decade_label}</b>).</div>
      <div>• For targeting overlays: temperature is sampled at each LAD centroid using nearest gridpoint.</div>

      {decision_html}

      <div style="margin-top:10px; color:#555;">
        Interpretation: spatial overlay for prioritisation; it does not imply local causality between LAD emissions and local temperature.
      </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(html0))

# ============================================================
# 5b. Policy targeting layers (Mitigation / Co-benefits only)
#     (Removed the yellow adaptation overlay, per request)
# ============================================================

@lru_cache(maxsize=64)
def _heat_points_for_decade(decade_label: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    heat = load_heat_field(decade_label)
    lat = np.asarray(heat["lat"]).astype(float).ravel()
    lon = np.asarray(heat["lon"]).astype(float).ravel()
    z = np.asarray(heat["z"]).astype(float).ravel()
    ok = np.isfinite(lat) & np.isfinite(lon) & np.isfinite(z)
    return lat[ok], lon[ok], z[ok]

def _nn_temp(lat0: float, lon0: float, lat: np.ndarray, lon: np.ndarray, z: np.ndarray) -> float | None:
    if lat.size == 0:
        return None
    d2 = (lat - lat0) ** 2 + (lon - lon0) ** 2
    j = int(np.argmin(d2))
    v = float(z[j])
    return v if np.isfinite(v) else None

def attach_centroid_temperature_to_geojson(gj: Dict[str, Any], decade_label: str) -> Dict[str, Any]:
    if not HAS_GPD:
        return gj

    lat, lon, z = _heat_points_for_decade(decade_label)
    out = gj.copy()

    try:
        gdf = gpd.GeoDataFrame.from_features(out["features"], crs="EPSG:4326")
        gdf["centroid"] = gdf.geometry.centroid

        lut: dict[str, float | None] = {}
        for _, r in gdf.iterrows():
            code = str(r.get("LAD24CD") or r.get("LADCD") or "").strip()
            pt = r["centroid"]
            if not code or pt is None:
                continue
            lut[code] = _nn_temp(float(pt.y), float(pt.x), lat, lon, z)

        for feat in out.get("features", []):
            props = feat.get("properties", {})
            code = str(props.get("LAD24CD") or props.get("LADCD") or "").strip()
            v = lut.get(code, None)
            props["tas_centroid_c"] = float(v) if v is not None and np.isfinite(v) else None
            feat["properties"] = props
    except Exception:
        return gj

    return out

def add_policy_targeting_layers(
    m: folium.Map,
    *,
    gj: Dict[str, Any],
    ghg_prop_field: str,
    decade_label: str,
    ghg_units: str,
    q: float = 75.0,
) -> Dict[str, Any] | None:
    """
    Adds overlays:
      - Targeting mitigation: high transport emissions (top-q percentile)
      - Co-benefits: high emissions ∩ high temperature (top-q percentile for both)
    Also returns a summary dict used by the left panel.
    """
    gj3 = attach_centroid_temperature_to_geojson(gj, decade_label)

    ghg_vals = np.array([f["properties"].get(ghg_prop_field) for f in gj3.get("features", [])], dtype=float)
    tmp_vals = np.array([f["properties"].get("tas_centroid_c") for f in gj3.get("features", [])], dtype=float)

    g_ok = ghg_vals[np.isfinite(ghg_vals)]
    t_ok = tmp_vals[np.isfinite(tmp_vals)]

    if g_ok.size == 0 or t_ok.size == 0:
        note = (
            "<div style='position:fixed; top: 120px; left: 12px; z-index: 9999;"
            "background: rgba(255,255,255,0.92); padding: 8px 10px;"
            "border: 1px solid #cfcfcf; border-radius: 10px; width: 360px;"
            "box-shadow: 0 2px 8px rgba(0,0,0,0.10);"
            "font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;"
            "font-size: 18px; line-height: 1.65;'>"
            "<b>Targeting layers unavailable.</b><br>"
            "Reason: could not compute centroid temperatures (requires geopandas) or no valid values."
            "</div>"
        )
        m.get_root().html.add_child(folium.Element(note))
        return None

    g_thr = float(np.nanpercentile(g_ok, q))
    t_thr = float(np.nanpercentile(t_ok, q))

    def _flag(feat):
        p = feat["properties"]
        g = p.get(ghg_prop_field)
        t = p.get("tas_centroid_c")
        g_hi = (g is not None and float(g) >= g_thr)
        t_hi = (t is not None and float(t) >= t_thr)
        return g_hi, t_hi

    def style_mit(feat):
        g_hi, _ = _flag(feat)
        return {
            "fillColor": "#6f42c1" if g_hi else "#000000",
            "fillOpacity": 0.42 if g_hi else 0.0,
            "color": "#2b2b2b",
            "weight": 0.8,
        }

    def style_coben(feat):
        g_hi, t_hi = _flag(feat)
        both = g_hi and t_hi
        return {
            "fillColor": "#d63384" if both else "#000000",
            "fillOpacity": 0.55 if both else 0.0,
            "color": "#111111",
            "weight": 1.0 if both else 0.6,
        }

    tooltip = folium.GeoJsonTooltip(
        fields=["LAD24NM", "LAD24CD", ghg_prop_field, "tas_centroid_c"],
        aliases=["Local authority", "Code", f"Transport emissions ({ghg_units})",
                 f"Temperature at centroid (°C) — {decade_label}"],
        localize=True,
        sticky=True,
    )

    fg1 = folium.FeatureGroup(name="Targeting mitigation: high transport emissions", show=False)
    folium.GeoJson(gj3, style_function=style_mit, tooltip=tooltip).add_to(fg1)
    fg1.add_to(m)

    fg3 = folium.FeatureGroup(name="Co-benefits: high emissions ∩ high temperature", show=True)
    folium.GeoJson(gj3, style_function=style_coben, tooltip=tooltip).add_to(fg3)
    fg3.add_to(m)

    # Summary counts
    n_total = 0
    n_high_ghg = 0
    n_high_temp = 0
    n_both = 0

    for feat in gj3.get("features", []):
        p = feat.get("properties", {})
        g = p.get(ghg_prop_field)
        t = p.get("tas_centroid_c")
        if g is None or t is None:
            continue
        try:
            g = float(g)
            t = float(t)
        except Exception:
            continue
        if not (np.isfinite(g) and np.isfinite(t)):
            continue

        n_total += 1
        g_hi = g >= g_thr
        t_hi = t >= t_thr
        n_high_ghg += int(g_hi)
        n_high_temp += int(t_hi)
        n_both += int(g_hi and t_hi)

    return {
        "q_percentile": float(q),
        "ghg_threshold": float(g_thr),
        "temp_threshold": float(t_thr),
        "n_total": int(n_total),
        "n_high_ghg": int(n_high_ghg),
        "n_high_temp": int(n_high_temp),
        "n_both": int(n_both),
    }

# ============================================================
# 5c. Heat layer
# ============================================================

def add_heat_overlay(m: folium.Map, heat: Dict[str, Any], opacity=0.55) -> Dict[str, float | None]:
    if not heat:
        return {"min": None, "mean": None, "max": None}

    lat = np.asarray(heat["lat"])
    lon = np.asarray(heat["lon"])
    z = np.asarray(heat["z"])

    vals = z[np.isfinite(z)]
    stats = {
        "min": float(np.nanmin(vals)) if vals.size else None,
        "mean": float(np.nanmean(vals)) if vals.size else None,
        "max": float(np.nanmax(vals)) if vals.size else None,
    }

    pts = []
    denom = max(1e-12, (HEAT_VMAX - HEAT_VMIN))
    for i in range(z.shape[0]):
        for j in range(z.shape[1]):
            v = z[i, j]
            if not np.isfinite(v):
                continue
            w = float((v - HEAT_VMIN) / denom)
            w = 0.0 if w < 0.0 else (1.0 if w > 1.0 else w)
            pts.append([float(lat[i, j]), float(lon[i, j]), w])

    fg = folium.FeatureGroup(name="Heat: HeatMap (tas)", show=True)

    HeatMap(
        pts,
        name="HeatMap",
        min_opacity=0.05,
        max_opacity=float(opacity),
        radius=12,
        blur=10,
        gradient=_mpl_gradient(HEAT_CMAP, n=9),
    ).add_to(fg)

    fg.add_to(m)
    return stats

def add_temperature_value_labels(m: folium.Map, decade_label: str) -> None:
    try:
        heat = load_heat_field(decade_label)
        lat = np.asarray(heat["lat"])
        lon = np.asarray(heat["lon"])
        z = np.asarray(heat["z"])

        fg = folium.FeatureGroup(name="Heat: value labels (toggle)", show=False)
        mc = MarkerCluster(name="Temp labels").add_to(fg)

        step = max(1, HEAT_LABEL_STEP // max(1, HEAT_GRID_STEP))
        for i in range(0, z.shape[0], step):
            for j in range(0, z.shape[1], step):
                val = z[i, j]
                if not np.isfinite(val):
                    continue
                label = f"{val:.1f}°C"
                color = value_to_hex(float(val), vmin=HEAT_VMIN, vmax=HEAT_VMAX, cmap="coolwarm")
                html0 = (
                    f"<div style='background:{color}; color:white;"
                    "border:1px solid #222; border-radius:14px; padding:2px 7px;"
                    "font-size:18px; font-weight:700; white-space:nowrap;"
                    "box-shadow:0 1px 2px rgba(0,0,0,0.2)'>"
                    f"{label}</div>"
                )
                folium.Marker(
                    location=[float(lat[i, j]), float(lon[i, j])],
                    icon=folium.DivIcon(html=html0),
                    tooltip=f"Temperature: {label}",
                ).add_to(mc)

        fg.add_to(m)
    except Exception:
        return

# ============================================================
# 5d. Transport GHG layers (choropleth + labels)
# ============================================================

def _stats_of(series: pd.Series) -> Dict[str, float | None]:
    s = pd.to_numeric(series, errors="coerce")
    s = s[np.isfinite(s)]
    if s.empty:
        return {"min": None, "mean": None, "max": None}
    return {"min": float(s.min()), "mean": float(s.mean()), "max": float(s.max())}

def add_transport_ghg_layers(
    m: folium.Map,
    *,
    year: int,
    metric: str,
    choropleth_gas: str,
) -> Tuple[str, str, Dict[str, Dict[str, float | None]], Dict[str, Any]]:
    if choropleth_gas not in GAS_FIELDS:
        choropleth_gas = "total"

    gj = load_lad_geojson()
    ghg_df, units = load_transport_ghg_gases(year=year, metric=metric)
    gj2 = inject_ghg_gases_into_geojson(gj, ghg_df)

    stats_by_gas = {
        "total": _stats_of(ghg_df["total"]),
        "co2": _stats_of(ghg_df["co2"]),
        "ch4": _stats_of(ghg_df["ch4"]),
        "n2o": _stats_of(ghg_df["n2o"]),
    }

    _, prop_field = GAS_FIELDS[choropleth_gas]
    display_gas, _ = GAS_FIELDS[choropleth_gas]

    vals = pd.to_numeric(ghg_df[choropleth_gas], errors="coerce").to_numpy() if not ghg_df.empty else np.array([])
    finite = vals[np.isfinite(vals)]
    if finite.size == 0:
        vmin, vmax = 0.0, 1.0
    else:
        vmin = float(finite.min())
        vmax = float(finite.max())
        if vmax <= vmin:
            vmax = vmin + 1.0

    cmap = bcm.linear.Viridis_09.scale(vmin, vmax)
    caption = f"Transport emissions — {display_gas} ({units}) — {year}"
    cmap.caption = caption

    def style_fill(feat):
        v = feat["properties"].get(prop_field, None)
        return {
            "fillColor": cmap(v) if v is not None else "#dddddd",
            "color": "#555555",
            "weight": 0.8,
            "fillOpacity": 0.75 if v is not None else 0.15,
        }

    def style_outline(_feat):
        return {"fillOpacity": 0.0, "color": "#2b2b2b", "weight": 1.0}

    def highlight_fn(_feat):
        return {"weight": 2.2, "color": "#000000", "fillOpacity": 0.88}

    tooltip = folium.GeoJsonTooltip(
        fields=["LAD24NM", "LAD24CD", "ghg_total", "ghg_co2", "ghg_ch4", "ghg_n2o"],
        aliases=[
            "Local authority",
            "Code",
            f"Total GHG ({units})",
            f"CO₂ ({units})",
            f"CH₄ ({units})",
            f"N₂O ({units})",
        ],
        localize=True,
        sticky=True,
    )

    fg_fill = folium.FeatureGroup(name=f"GHG: transport choropleth — {display_gas} ({year})", show=True)
    folium.GeoJson(
        gj2,
        name="Transport GHG (filled)",
        style_function=style_fill,
        highlight_function=highlight_fn,
        tooltip=tooltip,
    ).add_to(fg_fill)
    fg_fill.add_to(m)

    fg_outline = folium.FeatureGroup(name="GHG: LA boundaries (outline)", show=False)
    folium.GeoJson(gj2, name="LA boundaries", style_function=style_outline).add_to(fg_outline)
    fg_outline.add_to(m)

    if HAS_GPD and ENABLE_GHG_VALUE_LABELS:
        try:
            gdf0 = gpd.GeoDataFrame.from_features(gj2["features"], crs="EPSG:4326")
            gdf0[prop_field] = pd.to_numeric(gdf0[prop_field], errors="coerce")
            gdf0 = gdf0.dropna(subset=[prop_field]).copy()
            gdf0["centroid"] = gdf0.geometry.centroid

            fg_lab = folium.FeatureGroup(name=f"GHG: values on map — {display_gas}", show=True)
            mc = MarkerCluster(name="GHG values").add_to(fg_lab)

            for _, r in gdf0.iterrows():
                v = float(r[prop_field])
                nm = str(r.get("LAD24NM", "") or "")
                cd = str(r.get("LAD24CD", "") or "")
                pt = r["centroid"]

                txt = f"{v:,.0f}" if units.startswith("kt") else f"{v:,.2f}"

                html0 = (
                    "<div style='background:rgba(255,255,255,0.96);"
                    "border:1.5px solid #111; border-radius:14px; padding:3px 8px;"
                    "font-size:18px; font-weight:900; white-space:nowrap;"
                    "box-shadow:0 1px 3px rgba(0,0,0,0.25)'>"
                    f"{txt}</div>"
                )
                folium.Marker(
                    location=[float(pt.y), float(pt.x)],
                    icon=folium.DivIcon(html=html0),
                    tooltip=f"{nm} ({cd}) • {display_gas}: {v:,.2f} {units}",
                ).add_to(mc)

            fg_lab.add_to(m)
        except Exception:
            pass

    return units, caption, stats_by_gas, gj2

# ============================================================
# 6. Map builder
# ============================================================

def build_map(
    decade_label: str,
    ghg_year: int,
    ghg_metric: str,
    ghg_gas: str,
    focus_city: str,
) -> str:
    lat0, lon0 = CITY_PRESETS.get(focus_city, CITY_PRESETS["UK (centre)"])
    m = folium.Map(location=[lat0, lon0], zoom_start=6, tiles=None, control_scale=True)

    gj2_for_targeting = None
    targeting_summary = None
    add_base_tiles(m)

    # Heat (background)
    try:
        heat = load_heat_field(decade_label)
        add_heat_overlay(m, heat, opacity=0.30)
        if ENABLE_HEAT_VALUE_LABELS:
            add_temperature_value_labels(m, decade_label)
    except Exception as e:
        folium.Marker(
            [lat0, lon0],
            icon=folium.DivIcon(
                html="<div style='background:#f8d7da;border:1px solid #dc3545;"
                     "padding:10px;border-radius:10px;font-weight:800;'>"
                     f"Heat layer error: {e}</div>"
            ),
        ).add_to(m)

    # Transport GHG (foreground)
    ghg_units = "kt CO₂e"
    try:
        ghg_units, _, _, gj2_for_targeting = add_transport_ghg_layers(
            m,
            year=int(ghg_year),
            metric=str(ghg_metric),
            choropleth_gas=str(ghg_gas),
        )
    except Exception as e:
        folium.Marker(
            [lat0, lon0],
            icon=folium.DivIcon(
                html="<div style='background:#fff3cd;border:1px solid #ffc107;"
                     "padding:10px;border-radius:10px;font-weight:800;'>"
                     f"Transport GHG layer error: {e}</div>"
            ),
        ).add_to(m)

    # Policy overlays + summary (mitigation + co-benefits)
    if ENABLE_POLICY_TARGETING and gj2_for_targeting is not None:
        _, ghg_prop_field = GAS_FIELDS.get(ghg_gas, GAS_FIELDS["total"])
        targeting_summary = add_policy_targeting_layers(
            m,
            gj=gj2_for_targeting,
            ghg_prop_field=ghg_prop_field,
            decade_label=decade_label,
            ghg_units=ghg_units,
            q=75.0,
        )

    # Left panel (sources + intended decision summary)
    add_left_context_panel(
        m,
        decade_label=decade_label,
        ghg_year=int(ghg_year),
        ghg_metric=str(ghg_metric),
        ghg_gas=str(ghg_gas),
        ghg_units=str(ghg_units),
        targeting_summary=targeting_summary,
    )

    # Optional map-layer explainer panel (uncomment if you want it visible)
    # add_map_layers_panel(m)

    folium.CircleMarker(
        location=[lat0, lon0],
        radius=9,
        color="#0d6efd",
        weight=3,
        fill=True,
        fill_color="#0d6efd",
        fill_opacity=0.25,
        tooltip=f"Focus: {focus_city}",
    ).add_to(m)
    return m._repr_html_()

# ============================================================
# 7. Dash Layout
# ============================================================

layout = html.Div(
    [
        back_button(),
        html.H1("B) Heat + Transport GHG (Local Authorities)", style={"textAlign": "center", "marginBottom": "6px"}),

        dcc.Markdown(
            """
            This page combines **temperature projections** (decadal NetCDF layers) with **transport-sector GHG emissions** by local authority.  
            The GHG tooltip shows **Total + CO₂ + CH₄ + N₂O**; choose which gas colours the choropleth.  
            """,
            style={"maxWidth": "1100px", "margin": "0 auto 10px"},
        ),

        html.Div(
            [
                html.Div(
                    [
                        html.Label("Heat decade (map layer)"),
                        dcc.Dropdown(
                            id="heat-decade",
                            options=[{"label": k, "value": k} for k in HEAT_FILES.keys()],
                            value="HEAT 2020–2030",
                            clearable=False,
                        ),
                    ],
                    style={"minWidth": "320px"},
                ),
                html.Div(
                    [
                        html.Label("Transport GHG year"),
                        dcc.Dropdown(
                            id="ghg-year",
                            options=[{"label": str(y), "value": int(y)} for y in range(2005, DEFAULT_GHG_YEAR + 1)],
                            value=DEFAULT_GHG_YEAR,
                            clearable=False,
                        ),
                    ],
                    style={"minWidth": "220px"},
                ),
                html.Div(
                    [
                        html.Label("Transport GHG metric"),
                        dcc.RadioItems(
                            id="ghg-metric",
                            options=[
                                {"label": "kt CO₂e", "value": "kt"},
                                {"label": "t CO₂e per capita (if available)", "value": "per_capita"},
                            ],
                            value="kt",
                            inline=True,
                        ),
                    ],
                    style={"minWidth": "360px"},
                ),
                html.Div(
                    [
                        html.Label("GHG gas (choropleth colour)"),
                        dcc.RadioItems(
                            id="ghg-gas",
                            options=[
                                {"label": "Total", "value": "total"},
                                {"label": "CO₂", "value": "co2"},
                                {"label": "CH₄", "value": "ch4"},
                                {"label": "N₂O", "value": "n2o"},
                            ],
                            value="total",
                            inline=True,
                        ),
                    ],
                    style={"minWidth": "420px"},
                ),
                html.Div(
                    [
                        html.Label("Map focus"),
                        dcc.Dropdown(
                            id="focus-city",
                            options=[{"label": k, "value": k} for k in CITY_PRESETS.keys()],
                            value="UK (centre)",
                            clearable=False,
                        ),
                    ],
                    style={"minWidth": "240px"},
                ),
                html.Button("Update map", id="btn-update", n_clicks=0, style={"height": "38px", "marginTop": "22px"}),
            ],
            style={
                "display": "flex",
                "gap": "12px",
                "alignItems": "flex-start",
                "flexWrap": "wrap",
                "margin": "10px 0 12px",
                "justifyContent": "center",
            },
        ),

        html.Iframe(
            id="heat-map",
            srcDoc=build_map("HEAT 2020–2030", DEFAULT_GHG_YEAR, "kt", "total", "UK (centre)"),
            style={"width": "100%", "height": MAP_HEIGHT, "border": "1px solid #ddd", "borderRadius": "8px"},
        ),

        html.Div(
            [
                html.Label("Decades shown in time series"),
                dcc.Dropdown(
                    id="ts-decades",
                    options=[{"label": k, "value": k} for k in HEAT_FILES.keys()],
                    value=list(HEAT_FILES.keys()),
                    multi=True,
                    style={"maxWidth": "820px", "margin": "0 auto"},
                ),
                html.Div(
                    "Tip: click legend items to hide/show decades, or remove decades using the dropdown above.",
                    style={"textAlign": "center", "marginTop": "6px", "color": "#555"},
                ),
            ],
            style={"marginTop": "10px", "marginBottom": "6px"},
        ),

        html.Hr(style={"maxWidth": "1100px", "margin": "16px auto"}),

        dcc.Markdown(
        """
        **Daily tas time series (UK mean).**  
        **Source:** DAFNI NetCDF layers (downloaded via Google Drive in this app).  
        **Method:** at each timestep, compute the UK mean by averaging **tas** over the full grid and **ensemble_member**.  
        """,
            style={"maxWidth": "1100px", "margin": "0 auto 8px", "fontSize": "32px", "lineHeight": "1.5"},
        ),
        dcc.Graph(id="heat-daily-chart", figure=build_daily_uk_mean_chart(list(HEAT_FILES.keys()))),

        dcc.Markdown(
        """
        **Annual anomaly time series (baseline 1990–2000).**  \\
        
        **Source:** same DAFNI NetCDF layers.  \\
        
        **Method:** UK-mean **tas** → annual mean per year → anomaly relative to the 1990–2000 average. \\
        
        **Summary:** For each year, it shows how much the UK’s average temperature (from the tas data) is above or below the 1990–2000 average. \\
        
        Each coloured line corresponds to a different decadal dataset (e.g., 2010–2020, 2040–2050), plotted as yearly deviations from that baseline.
        """,
            style={"maxWidth": "1100px", "margin": "18px auto 18px", "fontSize": "32px", "lineHeight": "1.5"},
        ),
        dcc.Graph(id="heat-chart", figure=build_decade_separated_anomaly_chart(list(HEAT_FILES.keys()))),

        html.Div(id="heat-info", style={"textAlign": "center", "marginTop": "18px", "color": "#555"}),

        dcc.Markdown(
        """
        **Paris thresholds (mean anomaly).**  \\
        
        **Source:** DAFNI NetCDF layers.  \\
        
        **Method:** compute annual anomalies (baseline 1990–2000) for each selected decade-series, then take the mean anomaly per year; overlay 1.5°C and 2.0°C reference lines. \\
        
        **Summary:** It takes those yearly “above/below baseline” values across the selected decadal datasets, averages them into one mean line per year, and then shows how that mean compares to 1.5°C and 2.0°C reference lines (the Paris Agreement thresholds).
        """,
            style={"maxWidth": "1100px", "margin": "18px auto 28px", "fontSize": "32px", "lineHeight": "1.5"},
                ),
        dcc.Graph(id="paris-chart", figure=build_paris_targets_chart(list(HEAT_FILES.keys()))),
    ]
)
# ============================================================
# 8. Callbacks
# ============================================================

@callback(
    Output("heat-map", "srcDoc"),
    Output("heat-info", "children"),
    Input("btn-update", "n_clicks"),
    State("heat-decade", "value"),
    State("ghg-year", "value"),
    State("ghg-metric", "value"),
    State("ghg-gas", "value"),
    State("focus-city", "value"),
)
def update_map(_n, decade_label, ghg_year, ghg_metric, ghg_gas, focus_city):
    decade_label = decade_label or "HEAT 2020–2030"
    ghg_year = int(ghg_year or DEFAULT_GHG_YEAR)
    ghg_metric = ghg_metric or "kt"
    ghg_gas = ghg_gas or "total"
    focus_city = focus_city or "UK (centre)"

    html_map = build_map(decade_label, ghg_year, ghg_metric, ghg_gas, focus_city)
    info = (
        f"Updated {time.strftime('%H:%M:%S')} • Heat: {decade_label} • "
        f"Transport emissions: {ghg_year} ({ghg_metric}) • Choropleth: {ghg_gas.upper()} • Focus: {focus_city}"
    )
    return html_map, info

@callback(
    Output("heat-daily-chart", "figure"),
    Output("heat-chart", "figure"),
    Output("paris-chart", "figure"),
    Input("ts-decades", "value"),
)
def update_timeseries(decades):
    decades = decades or list(HEAT_FILES.keys())
    daily_fig = build_daily_uk_mean_chart(decades)
    annual_fig = build_decade_separated_anomaly_chart(decades)
    paris_fig = build_paris_targets_chart(decades)
    return daily_fig, annual_fig, paris_fig

# ============================================================
# 9. Standalone run (optional)
# ============================================================

if __name__ == "__main__":
    from dash import Dash
    app = Dash(__name__, use_pages=False)
    app.layout = layout
    app.run_server(debug=True, port=8052)
