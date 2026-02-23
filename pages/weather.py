# ============================================================
# CLEETS-SMART Dashboard A: Weather Forecaster (CITY SELECT + ENHANCED MAP)
# ============================================================
# Live weather forecasts for Wales using Open-Meteo (default)
# ============================================================

from __future__ import annotations

import time
from functools import lru_cache
from typing import Dict, List, Tuple, Optional

import requests
import folium
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import html, dcc, Input, Output, State, callback, register_page
from folium.plugins import MarkerCluster

# Optional folium plugins (safe import)
try:
    from folium.plugins import Fullscreen, MiniMap, MeasureControl, MousePosition
except Exception:  # pragma: no cover
    Fullscreen = MiniMap = MeasureControl = MousePosition = None  # type: ignore

# Dash page registration
register_page(__name__, path="/weather-forecaster")



# ============================================================
# 0) City gazetteer (South Wales) - AUTO from OS Open Names
# ============================================================
# Source: OS Open Names (CSV tiles; headerless; provides GEOMETRY_X/Y + admin areas). :contentReference[oaicite:1]{index=1}

from pathlib import Path
import glob

# pip install pyproj
from pyproj import Transformer

# --- Define what you consider "South Wales" (unitary authorities) ---
SOUTH_WALES_AUTHORITIES = {
    "Cardiff",
    "Vale of Glamorgan",
    "Bridgend",
    "Rhondda Cynon Taf",
    "Merthyr Tydfil",
    "Caerphilly",
    "Newport",
    "Torfaen",
    "Blaenau Gwent",
    "Monmouthshire",
    "Neath Port Talbot",
    "Swansea",
    # Optional (often treated as West Wales, but many projects include them):
    "Carmarthenshire",
    "Pembrokeshire",
}

# OS Open Names CSV columns (the product CSVs are typically headerless; this list matches the user guide). :contentReference[oaicite:2]{index=2}
OS_OPENNAMES_COLUMNS = [
    "ID",
    "NAMES_URI",
    "NAME1",
    "NAME1_LANG",
    "NAME2",
    "NAME2_LANG",
    "TYPE",
    "LOCAL_TYPE",
    "GEOMETRY_X",
    "GEOMETRY_Y",
    "MOST_DETAIL_VIEW_RES",
    "LEAST_DETAIL_VIEW_RES",
    "MBR_XMIN",
    "MBR_YMIN",
    "MBR_XMAX",
    "MBR_YMAX",
    "POSTCODE_DISTRICT",
    "POSTCODE_DISTRICT_URI",
    "POPULATED_PLACE",
    "POPULATED_PLACE_URI",
    "POPULATED_PLACE_TYPE",
    "DISTRICT_BOROUGH",
    "DISTRICT_BOROUGH_URI",
    "DISTRICT_BOROUGH_TYPE",
    "COUNTY_UNITARY",
    "COUNTY_UNITARY_URI",
    "COUNTY_UNITARY_TYPE",
    "REGION",
    "REGION_URI",
    "COUNTRY",
    "COUNTRY_URI",
    "RELATED_SPATIAL_OBJECT",
    "SAME_AS_DBPEDIA",
    "SAME_AS_GEONAMES",
]

def load_south_wales_places_from_opennames(
    opennames_dir: str | Path,
    *,
    include_local_types: set[str] | None = None,  # e.g., {"City","Town"}
    only_cities: bool = False,                     # True => LOCAL_TYPE == "City"
) -> dict[str, tuple[float, float]]:
    """
    Build a {place_name: (lat, lon)} gazetteer from OS Open Names CSV tiles.
    - Filters to Wales + South Wales unitary authorities.
    - Keeps Populated Place theme only (cities/towns/villages/suburbs etc.). :contentReference[oaicite:3]{index=3}
    """
    opennames_dir = Path(opennames_dir)

    # Common layouts after unzip vary; search recursively for CSV tiles.
    csv_files = [Path(p) for p in glob.glob(str(opennames_dir / "**" / "*.csv"), recursive=True)]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under: {opennames_dir}")

    # OSGB36 / British National Grid -> WGS84
    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)

    frames = []
    for fp in csv_files:
        # Many OS Open Names tiles are headerless; we supply headers explicitly.
        df = pd.read_csv(fp, header=None, names=OS_OPENNAMES_COLUMNS, dtype=str, low_memory=False)
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)

    # Keep only populated places in Wales
    df = df[(df["COUNTRY"] == "Wales") & (df["TYPE"] == "Populated Place")]

    # Filter to "South Wales" via unitary authority
    df = df[df["COUNTY_UNITARY"].isin(SOUTH_WALES_AUTHORITIES)]

    # Optionally restrict local types
    if only_cities:
        df = df[df["LOCAL_TYPE"] == "City"]
    elif include_local_types is not None:
        df = df[df["LOCAL_TYPE"].isin(include_local_types)]

    # Convert coordinates
    df = df.dropna(subset=["GEOMETRY_X", "GEOMETRY_Y", "NAME1"])
    df["GEOMETRY_X"] = pd.to_numeric(df["GEOMETRY_X"], errors="coerce")
    df["GEOMETRY_Y"] = pd.to_numeric(df["GEOMETRY_Y"], errors="coerce")
    df = df.dropna(subset=["GEOMETRY_X", "GEOMETRY_Y"])

    # Transform (x=easting, y=northing) -> (lat, lon)
    lons, lats = transformer.transform(df["GEOMETRY_X"].to_numpy(), df["GEOMETRY_Y"].to_numpy())
    df["lat"] = lats
    df["lon"] = lons

    # De-duplicate names (OS Open Names can contain multiple features with same name). :contentReference[oaicite:4]{index=4}
    # Keep the first occurrence; for better disambiguation, you could key by (NAME1, COUNTY_UNITARY).
    df = df.sort_values(["NAME1", "COUNTY_UNITARY"]).drop_duplicates(subset=["NAME1"], keep="first")

    return {row["NAME1"]: (float(row["lat"]), float(row["lon"])) for _, row in df.iterrows()}

# ---- Use it here ----
# Point this at your unzipped OS Open Names folder (set via env var or config in real deployments).
OS_OPENNAMES_DIR = Path("data/os_open_names")  # <-- change to your path

try:
    # If you literally want only "cities" (Cardiff/Swansea/Newport), set only_cities=True.
    WALES_CITIES = load_south_wales_places_from_opennames(
        OS_OPENNAMES_DIR,
        include_local_types={"City", "Town", "Village", "Suburban Area"},  # tweak as desired
        only_cities=False,
    )
except Exception:
    # Fallback to a minimal set so the app still runs if data isn't present
    WALES_CITIES = {
        "Cardiff": (51.4816, -3.1791),
        "Swansea": (51.6214, -3.9436),
        "Newport": (51.5842, -2.9977),
        "Bridgend": (51.5070, -3.5770),
        "Merthyr Tydfil": (51.7440, -3.3770),
    }

CITY_OPTIONS = [{"label": k, "value": k} for k in sorted(WALES_CITIES.keys())]

# ============================================================
# 1) Weather API Utilities (Open-Meteo)
# ============================================================

def _ts_bucket_5min() -> str:
    """5-minute cache bucket timestamp."""
    return str(int(time.time() // 300))


@lru_cache(maxsize=256)
def cached_get(url: str, params_tuple: Tuple[Tuple[str, str], ...]) -> dict:
    r = requests.get(url, params=dict(params_tuple), timeout=25)
    r.raise_for_status()
    return r.json()


def get_weather_current_multi(cities: List[str]) -> Dict[str, dict]:
    """
    Fetch current weather for multiple cities in ONE Open-Meteo call
    (Open-Meteo supports comma-separated latitude/longitude and returns a list-like structure). :contentReference[oaicite:0]{index=0}
    Returns: {city: {"temp": float|None, "wind": float|None, "precip": float|None}}
    """
    if not cities:
        return {}

    lats = ",".join(str(WALES_CITIES[c][0]) for c in cities)
    lons = ",".join(str(WALES_CITIES[c][1]) for c in cities)

    url = "https://api.open-meteo.com/v1/forecast"
    params = (
        ("latitude", lats),
        ("longitude", lons),
        ("current", "temperature_2m,precipitation,wind_speed_10m"),
        ("timezone", "Europe/London"),
        ("_ts", _ts_bucket_5min()),
    )
    raw = cached_get(url, params)

    # Open-Meteo multi-location responses can vary (some endpoints return list-of-structures). :contentReference[oaicite:1]{index=1}
    out: Dict[str, dict] = {c: {"temp": None, "wind": None, "precip": None} for c in cities}

    def _pull_one(obj: dict) -> dict:
        cur = obj.get("current", {}) or {}
        return {
            "temp": cur.get("temperature_2m"),
            "precip": cur.get("precipitation"),
            "wind": cur.get("wind_speed_10m"),
        }

    if isinstance(raw, list):
        # Assume order aligns with requested coordinates
        for c, obj in zip(cities, raw):
            out[c] = _pull_one(obj if isinstance(obj, dict) else {})
        return out

    # Single dict case: sometimes current values are returned as arrays for multi-loc
    cur = raw.get("current", {}) or {}
    if isinstance(cur.get("temperature_2m"), list):
        temps = cur.get("temperature_2m", [])
        precs = cur.get("precipitation", [])
        winds = cur.get("wind_speed_10m", [])
        for i, c in enumerate(cities):
            out[c] = {
                "temp": temps[i] if i < len(temps) else None,
                "precip": precs[i] if i < len(precs) else None,
                "wind": winds[i] if i < len(winds) else None,
            }
        return out

    # Fallback: single-location-style dict; map to the first city
    out[cities[0]] = _pull_one(raw)
    return out


def get_weather_hourly(lat: float, lon: float) -> dict:
    url = "https://api.open-meteo.com/v1/forecast"
    params = (
        ("latitude", str(lat)),
        ("longitude", str(lon)),
        ("current", "temperature_2m,precipitation,wind_speed_10m"),
        ("hourly", "temperature_2m,precipitation_probability,wind_speed_10m"),
        ("timezone", "Europe/London"),
        ("_ts", _ts_bucket_5min()),
    )
    return cached_get(url, params)


def parse_timeseries_openmeteo(raw: dict) -> pd.DataFrame:
    try:
        hourly = raw.get("hourly", {}) or {}
        return pd.DataFrame(
            {
                "time": hourly.get("time", []),
                "temp": hourly.get("temperature_2m", []),
                "pop": hourly.get("precipitation_probability", []),
                "wind": hourly.get("wind_speed_10m", []),
            }
        )
    except Exception:
        return pd.DataFrame()


# ============================================================
# 2) Map styling helpers
# ============================================================

def temp_to_color(temp_c: Optional[float]) -> str:
    """Simple blue→green→yellow→orange→red ramp."""
    if temp_c is None or not np.isfinite(temp_c):
        return "#808080"
    t = float(temp_c)
    if t <= 0:
        return "#1f78b4"
    if t <= 8:
        return "#33a02c"
    if t <= 15:
        return "#ffd92f"
    if t <= 22:
        return "#fb9a99"
    return "#e31a1c"


def temp_div_icon(temp_c: Optional[float], selected: bool = False) -> folium.DivIcon:
    val = "—" if temp_c is None or not np.isfinite(temp_c) else f"{temp_c:.0f}°C"
    bg = temp_to_color(temp_c)
    ring = "3px solid #000" if selected else "2px solid rgba(0,0,0,0.35)"
    scale = "1.15" if selected else "1.0"
    html = f"""
    <div style="
        transform: scale({scale});
        display:inline-flex;
        align-items:center;
        justify-content:center;
        width:44px; height:44px;
        border-radius:22px;
        background:{bg};
        border:{ring};
        box-shadow: 0 2px 6px rgba(0,0,0,0.25);
        font-weight:700;
        font-size:13px;
        color:#111;
        font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial;
    ">{val}</div>
    """
    return folium.DivIcon(html=html)

def add_base_tiles(m: folium.Map) -> None:
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap", control=True).add_to(m)

    folium.TileLayer(
        tiles="CartoDB positron",
        name="CartoDB Positron",
        control=True,
    ).add_to(m)

    folium.TileLayer(
        tiles="CartoDB dark_matter",
        name="CartoDB Dark Matter",
        control=True,
    ).add_to(m)

    # --- Stamen Terrain (needs attribution in newer folium) ---
    # Use an explicit URL + required attribution
    folium.TileLayer(
        tiles="https://stamen-tiles.a.ssl.fastly.net/terrain/{z}/{x}/{y}.png",
        name="Stamen Terrain",
        attr='Map tiles by Stamen Design, under CC BY 3.0. Data © OpenStreetMap contributors.',
        control=True,
        max_zoom=18,
    ).add_to(m)

    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Tiles © Esri",
        name="Esri World Imagery",
        control=True,
    ).add_to(m)

    folium.TileLayer(
        tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
        attr="© OpenStreetMap contributors, SRTM; style © OpenTopoMap (CC-BY-SA)",
        name="OpenTopoMap",
        control=True,
        max_zoom=17,
    ).add_to(m)

def build_weather_map(
    cities: List[str],
    current_by_city: Dict[str, dict],
    focus_city: Optional[str] = None,
) -> str:
    # center on focus, else Cardiff-ish
    if focus_city and focus_city in WALES_CITIES:
        clat, clon = WALES_CITIES[focus_city]
    else:
        clat, clon = 51.48, -3.18

    m = folium.Map(location=[clat, clon], zoom_start=7, tiles=None, control_scale=True)
    add_base_tiles(m)

    # nice map controls
    if Fullscreen is not None:
        try:
            Fullscreen(position="topright").add_to(m)
        except Exception:
            pass
    if MiniMap is not None:
        try:
            MiniMap(toggle_display=True).add_to(m)
        except Exception:
            pass
    if MeasureControl is not None:
        try:
            MeasureControl(position="topleft").add_to(m)
        except Exception:
            pass
    if MousePosition is not None:
        try:
            MousePosition(
                position="bottomright",
                separator=" | ",
                num_digits=5,
                prefix="Lat/Lon",
            ).add_to(m)
        except Exception:
            pass

    # marker groups (legend-like)
    fg = folium.FeatureGroup(name="City temperature markers", show=True).add_to(m)
    cluster = MarkerCluster(name="Cluster (cities)").add_to(fg)

    for c in cities:
        lat, lon = WALES_CITIES[c]
        cur = current_by_city.get(c, {}) or {}
        temp = cur.get("temp")
        wind = cur.get("wind")
        precip = cur.get("precip")

        tooltip = (
            f"<b>{c}</b><br>"
            f"Temp: {('—' if temp is None else f'{float(temp):.1f}')} °C<br>"
            f"Wind: {('—' if wind is None else f'{float(wind):.1f}')} km/h<br>"
            f"Precip: {('—' if precip is None else f'{float(precip):.1f}')} mm"
        )

        selected = (c == focus_city)
        folium.Marker(
            location=[lat, lon],
            tooltip=folium.Tooltip(tooltip, sticky=True),
            icon=temp_div_icon(temp, selected=selected),
        ).add_to(cluster)

        # subtle highlight ring for focused city
        if selected:
            folium.CircleMarker(
                location=[lat, lon],
                radius=18,
                color="#000000",
                weight=2,
                fill=False,
                opacity=0.8,
            ).add_to(fg)

    # simple legend
    legend = """
    <div style="position: fixed; bottom: 18px; left: 18px; z-index:9999;
                background: white; padding: 10px 12px; border: 1px solid #ccc;
                border-radius: 8px; font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial;
                font-size: 12px; box-shadow: 0 2px 10px rgba(0,0,0,0.15);">
      <b>Temperature marker colours</b><br>
      <div style="margin-top:6px;">
        <span style="display:inline-block;width:10px;height:10px;background:#1f78b4;border-radius:2px;"></span> ≤ 0°C
        &nbsp; <span style="display:inline-block;width:10px;height:10px;background:#33a02c;border-radius:2px;"></span> 1–8°C
        &nbsp; <span style="display:inline-block;width:10px;height:10px;background:#ffd92f;border-radius:2px;"></span> 9–15°C
        &nbsp; <span style="display:inline-block;width:10px;height:10px;background:#fb9a99;border-radius:2px;"></span> 16–22°C
        &nbsp; <span style="display:inline-block;width:10px;height:10px;background:#e31a1c;border-radius:2px;"></span> ≥ 23°C
      </div>
      <div style="margin-top:6px;">Black ring = selected city</div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend))

    folium.LayerControl(collapsed=True).add_to(m)
    return m._repr_html_()


# ============================================================
# 3) Plotly Graph Builder
# ============================================================

def build_weather_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["time"],
            y=df["temp"],
            mode="lines+markers",
            name="Temperature (°C)",
        )
    )
    fig.add_trace(
        go.Bar(
            x=df["time"],
            y=df["pop"],
            name="Precipitation Probability (%)",
            yaxis="y2",
            opacity=0.5,
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=40, r=40, t=40, b=40),
        legend=dict(orientation="h", y=-0.2),
        yaxis=dict(title="Temperature (°C)"),
        yaxis2=dict(
            title="Precip. Prob. (%)",
            overlaying="y",
            side="right",
            range=[0, 100],
        ),
    )
    return fig


# ============================================================
# 4) Dash Layout (CITY SELECTION)
# ============================================================

layout = html.Div(
    [
        html.H1("A) Weather Forecaster for South Wales", style={"textAlign": "center", "marginBottom": "10px"}),
        html.P(
            "Live hourly weather forecasts for Wales using Open-Meteo APIs. Data refreshes every 5 minutes.",
            style={"textAlign": "center"},
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Cities (map markers):"),
                        dcc.Dropdown(
                            id="wf-cities",
                            options=CITY_OPTIONS,
                            value=["Cardiff", "Swansea"],
                            multi=True,
                            placeholder="Select one or more cities…",
                            style={"minWidth": "360px"},
                        ),
                    ],
                    style={"display": "inline-block", "verticalAlign": "top"},
                ),
                html.Div(
                    [
                        html.Label("Selected city (chart + highlight):"),
                        dcc.Dropdown(
                            id="wf-focus",
                            options=CITY_OPTIONS,
                            value="Cardiff",
                            multi=False,
                            clearable=False,
                            style={"minWidth": "260px"},
                        ),
                    ],
                    style={"display": "inline-block", "verticalAlign": "top", "marginLeft": "14px"},
                ),
                html.Button("Update Forecast", id="wf-refresh", n_clicks=0, style={"marginLeft": "14px", "height": "38px"}),
            ],
            style={"textAlign": "center", "margin": "15px"},
        ),
        html.Iframe(
            id="wf-map",
            srcDoc=build_weather_map(["Cardiff", "Swansea"], get_weather_current_multi(["Cardiff", "Swansea"]), focus_city="Cardiff"),
            style={"width": "100%", "height": "820px", "border": "none"},
        ),
        dcc.Graph(id="wf-graph"),
        html.Div(id="wf-info", style={"textAlign": "center", "marginTop": "10px"}),
    ]
)


# ============================================================
# 5) Callbacks
# ============================================================

@callback(
    Output("wf-focus", "options"),
    Output("wf-focus", "value"),
    Input("wf-cities", "value"),
    State("wf-focus", "value"),
)
def _sync_focus_options(cities, focus):
    cities = cities or []
    opts = [{"label": c, "value": c} for c in cities] if cities else CITY_OPTIONS
    # keep focus if still present, else choose first selected, else Cardiff
    if cities:
        if focus in cities:
            return opts, focus
        return opts, cities[0]
    return CITY_OPTIONS, "Cardiff"


@callback(
    Output("wf-map", "srcDoc"),
    Output("wf-graph", "figure"),
    Output("wf-info", "children"),
    Input("wf-refresh", "n_clicks"),
    Input("wf-cities", "value"),
    Input("wf-focus", "value"),
)
def update_weather(n_clicks, cities, focus_city):
    cities = cities or []
    if not cities:
        cities = ["Cardiff"]
    if not focus_city or focus_city not in WALES_CITIES:
        focus_city = cities[0]

    # map: current for all selected cities (single call)
    try:
        current_by_city = get_weather_current_multi(cities)
    except Exception as e:
        current_by_city = {c: {"temp": None, "wind": None, "precip": None} for c in cities}
        map_html = build_weather_map(cities, current_by_city, focus_city=focus_city)
        return map_html, go.Figure(), f"Error retrieving current weather: {e}"

    map_html = build_weather_map(cities, current_by_city, focus_city=focus_city)

    # chart: hourly for focus city
    lat, lon = WALES_CITIES[focus_city]
    try:
        raw = get_weather_hourly(lat, lon)
        df = parse_timeseries_openmeteo(raw)
        fig = build_weather_chart(df)
        provider = "Open-Meteo"
    except Exception as e:
        fig = go.Figure()
        provider = "Open-Meteo (error)"
        return map_html, fig, f"Error retrieving hourly series for {focus_city}: {e}"

    cur = current_by_city.get(focus_city, {}) or {}
    temp = cur.get("temp")
    wind = cur.get("wind")
    precip = cur.get("precip")

    info = (
        f"Provider: {provider}. Selected: {focus_city} ({lat:.3f}, {lon:.3f}). "
        f"Current: {('—' if temp is None else f'{float(temp):.1f}')}°C, "
        f"wind {('—' if wind is None else f'{float(wind):.1f}')} km/h, "
        f"precip {('—' if precip is None else f'{float(precip):.1f}')} mm. "
        f"Updated at {time.strftime('%H:%M:%S')}."
    )
    return map_html, fig, info


# ============================================================
# 6) Standalone Run (optional)
# ============================================================

if __name__ == "__main__":
    from dash import Dash

    app = Dash(__name__)
    app.layout = layout
    app.run_server(debug=True, port=8051)
