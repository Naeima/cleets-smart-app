from dash import html, dcc, Input, Output, State, ctx, callback, no_update, ALL
from dash import dash_table, register_page
import dash_bootstrap_components as dbc
from datetime import datetime

import folium
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import requests
import re
import io
import time
from functools import lru_cache
from pathlib import Path
from typing import Optional
from shapely.geometry import shape


register_page(__name__, path="/equity")


# ============================================================
# Data sources
# ============================================================
# Every URL below is loaded and visualised by at least one sidebar item.
# Notes from inspecting the actual files behind the links (Sept 2026):
#   - VEH0105 (LAD) carries BodyType / Fuel / Keepership breakdowns
#     INCLUDING "Total" rows, so rows must be filtered to Total x Total x
#     Total before aggregating (summing everything multiplies the stock
#     roughly eightfold). Units are thousands.
#   - VEH0141 is national (UK countries) only - no LAD codes - so it powers
#     the Trends view, not a choropleth.
#   - VEH0125 (LSOA, all vehicles) is large (~240 MB) and served through
#     Google Drive's newer confirm form; _download handles both flows and
#     caches bytes on disk, and only the needed columns are parsed.

VEH0105_URL = "https://drive.google.com/uc?export=download&id=1MqF57lLua8HSEFOYV0V2lnZmy5fiKGMP"   # LAD, all vehicles (thousands; has Total rows)
VEH0132_URL = "https://drive.google.com/uc?export=download&id=1mGM0qG6MmH4bxvz8KL9NZuiJzPDqPFHi"   # LAD, ULEVs by fuel type (BEV / PHEV / other / Total)
VEH0141_URL = "https://drive.google.com/uc?export=download&id=1ubuFUSkL4Yqz1Dv8s5mkJttseXIxPfrZ"   # National plug-in stock by country and quarter (Trends view)
VEH0142_URL = "https://drive.google.com/file/d/1iFpTjn4anvJY_3vvavl44kLe5T2GeXP6/view?usp=sharing" # LAD, plug-in vehicles by body type, fuel and keepership

VEH0125_URL = "https://drive.google.com/uc?export=download&id=1w-626GyUeVdULmB0aYhxImMnx6UuTbtq"   # LSOA, all licensed vehicles (BodyType/Keepership/LicenceStatus)
VEH0145_URL = "https://drive.google.com/file/d/1jSc3swFMecXG7fOutp3CO-VzvaYpPDC5/view?usp=sharing" # LSOA, plug-in vehicles by fuel (BEV / PHEV / REX / Total)
VEH0135_URL = "https://drive.google.com/uc?export=download&id=1i40mJbxIe65CTjzlouUZi48Ge3jDOlfa"   # LSOA, ULEVs by fuel type

WIMD_WALES_URL = "https://drive.google.com/file/d/1K-PbySgovyzpFnnoY9exfDHpGXB1dLjT"   # WIMD income domain, Welsh LSOAs (rank 1 = most deprived)
IMD_ENGLAND_URL = "https://drive.google.com/file/d/1EXlkYrw--ueX1dzRSTUfzYxF5Gfk350W"  # IMD overall rank/decile, English LSOAs (rank 1 = most deprived)

POPULATION_URL = "https://drive.google.com/file/d/15kwuuNg6ZgdECrJDipg8b3H49cvi7vnS"       # ONS MYE2 mid-2024 population, LAD level
POP_DENSITY_URL = "https://drive.google.com/file/d/1nSbbxY8_hvV47OnZwBWo3-N-HuMdmL_-"      # ONS MYE5 population density, LAD level

LAD_FS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Authority_Districts_May_2024_Boundaries_UK_BGC/FeatureServer"
LAD_LAYER = "0"

LSOA_FS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_2021_EW_BFE_V10_RUC/FeatureServer"
LSOA_LAYER = "3"

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

# Cap on LSOA polygons rendered at once (folium slows badly beyond this).
# All Welsh LSOAs are kept first, then English LSOAs up to the cap; the
# area filter works on the full dataset, so no area is unreachable.
LSOA_RENDER_CAP = 2500

groups = ["Output", "Vehicles", "Equity", "Comparisons"]

layer_options = {
    "Output": [
        {"label": "Trends", "value": "trends"},
    ],
    "Vehicles": [
        {"label": "All", "value": "All"},
        {"label": "Plug-in", "value": "Plug-in"},
        {"label": "Battery Electric", "value": "Battery Electric"},
        {"label": "Ultra Low Emissions", "value": "Ultra Low Emissions"},
    ],
    "Equity": [
        {"label": "Income Deprivation", "value": "income_deprivation"},
        {"label": "Charging Inequality Gap", "value": "charging_inequality_gap"},
    ],
    "Comparisons": [
        {"label": "Battery Electric (%)", "value": "Battery Electric (%)"},
        {"label": "Plug-in (%)", "value": "Plug-in (%)"},
        {"label": "Ultra Low Emissions (%)", "value": "Ultra Low Emissions (%)"},
        {"label": "Plug-in per 1,000 residents", "value": "plugin_per_1000"},
        {"label": "Population density", "value": "population_density"},
    ],
}

# Soft pastel ColorBrewer palettes supported by folium.Choropleth:
# pastel blue-to-purple for Vehicles, pastel pink-to-purple for Equity,
# pastel purple for Comparisons. Each stays a single light-to-dark ramp so
# magnitude ordering remains readable.
FOLIUM_COLOURS = {
    "Vehicles": "BuPu",
    "Equity": "RdPu",
    "Comparisons": "Purples",
}

# Fill transparency for choropleth polygons (lower = more of the base map
# shows through, softer pastel look).
CHOROPLETH_FILL_OPACITY = 0.55

# value -> human-readable label, for legends, layer names and the
# selected-dataset display.
SELECTION_LABELS = {
    opt["value"]: opt["label"]
    for opts in layer_options.values()
    for opt in opts
}


# ------------------------------------------------------------
# Data lineage / provenance (the "i" icon beside each dataset)
# ------------------------------------------------------------
# One record per source dataset. Fields follow the WIMD exemplar:
# name, identifier, landing page, download, metadata record, publisher,
# licence, designation. "Date accessed" is filled at render time from the
# disk cache's timestamp for that source's working copy.

_DFT_TABLES_PAGE = "https://www.gov.uk/government/statistical-data-sets/vehicle-licensing-statistics-data-tables"
_DFT_FILES_PAGE = "https://www.gov.uk/government/statistical-data-sets/vehicle-licensing-statistics-data-files"
_DFT_INDEX_PAGE = "https://www.gov.uk/government/statistical-data-sets/vehicles-statistical-tables-index"
_OGL_URL = "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
_DFT_COMMON = {
    "publisher": "Department for Transport and Driver and Vehicle Licensing Agency",
    "licence": ("Open Government Licence v3.0", _OGL_URL),
    "designation": ("Official statistics in DfT's Vehicle licensing statistics series "
                    "(accreditation status not stated on the data-set page)"),
    "metadata": ("Vehicles statistical tables index", _DFT_INDEX_PAGE),
    "download": ("Release-specific asset URLs on GOV.UK (not persistent); this dashboard "
                 "reads a curated CSV extract hosted on Google Drive"),
}

PROVENANCE = {
    "veh0105": {
        "name": "VEH0105: Licensed vehicles by body type, fuel, keepership and local authority (district level)",
        "identifier": "VEH0105 (DfT table code)",
        "landing": _DFT_TABLES_PAGE,
        "url": VEH0105_URL, **_DFT_COMMON,
    },
    "veh0132": {
        "name": "VEH0132: Licensed ultra low emission vehicles by fuel type, keepership and local authority (district level)",
        "identifier": "VEH0132 (DfT table code)",
        "landing": _DFT_TABLES_PAGE,
        "url": VEH0132_URL, **_DFT_COMMON,
    },
    "veh0141": {
        "name": "VEH0141: Licensed plug-in vehicles by body type and fuel type, by UK country (national level; Trends view)",
        "identifier": "VEH0141 (DfT table code)",
        "landing": _DFT_TABLES_PAGE,
        "url": VEH0141_URL, **_DFT_COMMON,
    },
    "veh0142": {
        "name": "VEH0142: Licensed plug-in vehicles by body type, fuel type, keepership and upper and lower tier local authority (district level)",
        "identifier": "VEH0142 (DfT table code)",
        "landing": _DFT_TABLES_PAGE,
        "url": VEH0142_URL, **_DFT_COMMON,
    },
    "veh0125": {
        "name": "VEH0125 (df_VEH0125): Licensed vehicles by LSOA (neighbourhood level)",
        "identifier": "df_VEH0125 (DfT data file, ~230 MB CSV)",
        "landing": _DFT_FILES_PAGE,
        "url": VEH0125_URL, **_DFT_COMMON,
    },
    "veh0135": {
        "name": "VEH0135 (df_VEH0135): Licensed ultra low emission vehicles by fuel type and LSOA (neighbourhood level)",
        "identifier": "df_VEH0135 (DfT data file, ~59 MB CSV)",
        "landing": _DFT_FILES_PAGE,
        "url": VEH0135_URL, **_DFT_COMMON,
    },
    "veh0145": {
        "name": "VEH0145 (df_VEH0145): Licensed plug-in vehicles by fuel type and LSOA (neighbourhood level)",
        "identifier": "df_VEH0145 (DfT data file, ~58 MB CSV)",
        "landing": _DFT_FILES_PAGE,
        "url": VEH0145_URL, **_DFT_COMMON,
    },
    "wimd2025": {
        "name": "Welsh Index of Multiple Deprivation (WIMD) 2025 indicator data for local authorities and Wales (income domain used here)",
        "identifier": "306808da-47db-4e4e-8ff2-66a348505f08 (StatsWales dataset ID)",
        "landing": "https://stats.gov.wales/en-GB/306808da-47db-4e4e-8ff2-66a348505f08/data",
        "download": ("None persistent; generated from the \"Download data\" form (CSV / Excel / "
                     "JSON; formatted or unformatted; with or without reference codes; English "
                     "or Welsh). This dashboard reads a curated CSV extract hosted on Google Drive"),
        "metadata": (None,
                     "https://stats.gov.wales/en-GB/306808da-47db-4e4e-8ff2-66a348505f08/download/metadata"),
        "publisher": ("Welsh Government; seventeen data providers are listed, including DHCW, "
                      "Public Health Wales, ONS, DWP, HMRC and Natural Resources Wales"),
        "licence": ("Open Government Licence v3.0, per the Welsh Government copyright statement",
                    "https://www.gov.wales/copyright-statement"),
        "designation": ("Accredited official statistics; first published 11 March 2026; "
                        "not expected to be updated or replaced"),
        "url": WIMD_WALES_URL,
    },
    "imd2025": {
        "name": "English indices of deprivation 2025 (overall IMD rank and decile extract, 2021 LSOAs)",
        "identifier": "IoD2025",
        "landing": "https://www.gov.uk/government/statistics/english-indices-of-deprivation-2025",
        "download": ("Published files on GOV.UK; this dashboard reads a curated CSV extract "
                     "(LSOA 2021 codes, LAD 2024 codes, overall IMD rank and decile) hosted on "
                     "Google Drive"),
        "metadata": ("Technical and research reports linked from the statistics page",
                     "https://www.gov.uk/government/statistics/english-indices-of-deprivation-2025"),
        "publisher": "Ministry of Housing, Communities and Local Government",
        "licence": ("Open Government Licence v3.0", _OGL_URL),
        "designation": ("Accredited official statistics; released 30 October 2025; updated "
                        "17 November 2025 to correct an LSOA allocation error identified by the ONS"),
        "url": IMD_ENGLAND_URL,
    },
    "mye2": {
        "name": "ONS mid-2024 population estimates, MYE2: persons by single year of age and sex for local authorities in England and Wales",
        "identifier": "MYE2 sheet of the ONS mid-year estimates dataset, mid-2024 edition",
        "landing": "https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland",
        "download": ("XLSX workbook on the ONS dataset page; this dashboard reads a curated CSV "
                     "extract hosted on Google Drive"),
        "metadata": ("Quality and methodology information linked from the ONS dataset page", None),
        "publisher": "Office for National Statistics",
        "licence": ("Open Government Licence v3.0", _OGL_URL),
        "designation": "Accredited official statistics (ONS mid-year population estimates)",
        "url": POPULATION_URL,
    },
    "mye5": {
        "name": "ONS mid-year estimates, MYE5: population density for local authorities in England and Wales, mid-2011 to mid-2024",
        "identifier": "MYE5 sheet of the ONS mid-year estimates dataset, mid-2024 edition",
        "landing": "https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland",
        "download": ("XLSX workbook on the ONS dataset page; this dashboard reads a curated CSV "
                     "extract hosted on Google Drive"),
        "metadata": ("Quality and methodology information linked from the ONS dataset page", None),
        "publisher": "Office for National Statistics",
        "licence": ("Open Government Licence v3.0", _OGL_URL),
        "designation": "Accredited official statistics (ONS mid-year population estimates)",
        "url": POP_DENSITY_URL,
    },
}

# Which source records each sidebar dataset draws on.
SELECTION_SOURCES = {
    "All": ["veh0105", "veh0125"],
    "Plug-in": ["veh0142", "veh0145"],
    "Battery Electric": ["veh0142", "veh0145"],
    "Ultra Low Emissions": ["veh0132", "veh0135"],
    "income_deprivation": ["wimd2025", "imd2025"],
    "charging_inequality_gap": ["veh0135", "veh0125", "veh0142", "veh0105"],
    "trends": ["veh0141", "veh0105", "veh0142"],
    "Battery Electric (%)": ["veh0142", "veh0105", "veh0145", "veh0125"],
    "Plug-in (%)": ["veh0142", "veh0105", "veh0145", "veh0125"],
    "Ultra Low Emissions (%)": ["veh0132", "veh0105", "veh0135", "veh0125"],
    "plugin_per_1000": ["veh0142", "mye2"],
    "population_density": ["mye5"],
}

_BOUNDARY_NOTE = (
    "Boundary geometries: Local Authority Districts (May 2024) BGC and LSOA 2021 BFE "
    "layers from the ONS Open Geography Portal ArcGIS services, Open Government Licence v3.0."
)


def _cache_accessed(url: str) -> str:
    """Date the working copy was last downloaded (disk-cache timestamp)."""
    try:
        p = _cache_path(google_drive_direct_url(url))
        if p.exists():
            return datetime.fromtimestamp(p.stat().st_mtime).strftime("%d %B %Y")
    except Exception:
        pass
    return "not yet downloaded in this session"


def _lineage_li(label: str, value, href: Optional[str] = None):
    parts = [html.Strong(f"{label}: ")]
    if href:
        parts += [value and f"{value} " or "", html.A(href, href=href, target="_blank")]
    else:
        parts.append(value)
    return html.Li(parts, style={"marginBottom": "6px", "lineHeight": "1.5"})


def lineage_content(selection: str) -> list:
    """Provenance blocks for every source dataset behind a sidebar selection."""
    blocks = []
    for key in SELECTION_SOURCES.get(selection, []):
        rec = PROVENANCE.get(key)
        if not rec:
            continue
        items = [
            _lineage_li("Dataset name", rec["name"]),
            _lineage_li("Dataset identifier", rec["identifier"]),
            _lineage_li("Source landing page", None, href=rec["landing"]),
            _lineage_li("Direct download URL", rec["download"]),
        ]
        meta = rec.get("metadata")
        if meta:
            label, href = meta if isinstance(meta, tuple) else (meta, None)
            items.append(_lineage_li("Stable metadata record", label, href=href))
        items.append(_lineage_li("Publisher", rec["publisher"]))
        lic, lic_href = rec["licence"] if isinstance(rec["licence"], tuple) else (rec["licence"], None)
        items.append(_lineage_li("Licence", lic, href=lic_href))
        items.append(_lineage_li("Designation", rec["designation"]))
        items.append(_lineage_li("Date accessed (working copy cached)", _cache_accessed(rec["url"])))
        blocks.append(html.Div(
            [html.H6(rec["name"], style={"color": BRAND_BLUE, "fontSize": "14.5px", "marginBottom": "8px"}),
             html.Ul(items, style={"paddingLeft": "18px", "marginBottom": "0"})],
            style={"border": "1px solid #B6D7F2", "borderRadius": "8px",
                   "padding": "12px 14px", "marginBottom": "12px",
                   "backgroundColor": "#F8FBFF", "fontSize": FS_SMALL},
        ))
    blocks.append(html.Div(_BOUNDARY_NOTE, style={"fontSize": "12.5px", "color": "#505a5f",
                                                  "lineHeight": "1.5"}))
    return blocks


# ------------------------------------------------------------
# Per-visualisation methodology notes ("How these data were analysed")
# ------------------------------------------------------------

_GENERAL_HANDLING = (
    "General handling: suppressed or missing entries ([c], [x], [z], blanks) are treated as "
    "missing and excluded; geography codes are validated against the expected ONS pattern "
    "(E06/E07/E08/E09/W06 for districts, E01/W01 for LSOAs); values are taken from the most "
    "recent quarter column present in each source file, so a table released ahead of the "
    "others (currently VEH0142, at 2026 Q1) plots its newest quarter directly. Derived "
    "ratios are never mixed across quarters: numerator and denominator use the latest "
    "quarter common to both sources. Choropleth colour classes use sextile "
    "(quantile) bins when at least 12 distinct values exist, so a small number of extreme "
    "areas, typically fleet-registration districts, do not compress the colour scale. LSOA "
    "maps keep every Welsh LSOA and cap the total drawn at 2,500 areas for responsiveness; "
    "the area filter searches the full dataset."
)


def analysis_notes(group: Optional[str], selection: Optional[str], geo_level: str,
                   keepership: str = "Total") -> list[str]:
    """Short methodology paragraphs for the currently selected visualisation."""
    if not group or not selection:
        return ["Select a dataset from the sidebar to see how its values are derived."]

    notes: list[str] = []

    if group == "Output" and selection == "trends":
        return [
            "Chart 1: DfT VEH0141, quarterly licensed plug-in vehicles by UK country, "
            "BodyType = Total, restricted to the four countries (GB and UK aggregates dropped "
            "to avoid double counting). Raw end-of-quarter stock, no smoothing; dotted "
            "battery-electric series are toggled from the legend.",
            "Chart 2: the same VEH0141 plug-in stock divided by the total licensed stock for "
            "the matching country and quarter from VEH0105 (BodyType, Fuel and Keepership all "
            "Total, thousands scaled to vehicles), times 100. This expresses uptake as a share "
            "of each country's own fleet, so small countries are comparable with England.",
            "Chart 3: battery-electric share of licensed vehicles per district (VEH0132 over "
            "VEH0105, latest quarter), ranked; the ten highest and ten lowest districts are "
            "shown. Extremes are driven by company and lease registrations concentrated in a "
            "few districts.",
        ]

    if group == "Output":
        return ["Select a Vehicles, Equity or Comparisons dataset to populate the map."]

    if group == "Vehicles":
        if geo_level == "LSOA":
            src = {
                "All": "DfT VEH0125 (all licensed vehicles by LSOA): rows filtered to "
                       "BodyType = Total, Keepership = Total and LicenceStatus = Licensed, so "
                       "SORN vehicles are excluded.",
                "Plug-in": "DfT VEH0145 (plug-in vehicles by LSOA): rows filtered to "
                           "Fuel = Total and Keepership = Total, covering battery electric, "
                           "plug-in hybrid and range-extended vehicles.",
                "Battery Electric": "DfT VEH0145 (plug-in vehicles by LSOA): rows filtered to "
                                    "Fuel = BATTERY ELECTRIC and Keepership = Total.",
                "Ultra Low Emissions": "DfT VEH0135 (ULEVs by LSOA): rows filtered to "
                                       "Fuel = Total and Keepership = Total.",
            }.get(selection)
        else:
            src = {
                "All": "DfT VEH0105 (licensed vehicles by local authority): rows filtered to "
                       "BodyType = Total, Fuel = Total and Keepership = Total so that breakdown "
                       "rows are not double counted, then multiplied by 1,000 because the file "
                       "reports thousands.",
                "Plug-in": "DfT VEH0142 (licensed plug-in vehicles by body type, fuel type, "
                           "keepership and local authority): BodyType = Total, Fuel = Total "
                           "(battery electric, plug-in hybrids and range-extended), filtered "
                           "to the selected keepership.",
                "Battery Electric": "DfT VEH0142 (licensed plug-in vehicles by body type, fuel "
                                    "type, keepership and local authority): BodyType = Total, "
                                    "Fuel = Battery electric, filtered to the selected "
                                    "keepership.",
                "Ultra Low Emissions": "DfT VEH0132 (licensed ULEVs by fuel type and local "
                                       "authority): rows with Fuel = Total and Keepership = Total.",
            }.get(selection)
        if src:
            notes.append("Source and filters: " + src)
        notes.append(
            "Processing: values are parsed numerically, matched to the geography code column, "
            "aggregated by area (sum), and joined to ONS boundary geometries (BGC districts, "
            "2021 LSOAs) fetched from the ONS Open Geography ArcGIS services."
        )

    elif group == "Equity" and selection == "income_deprivation":
        notes += [
            "Sources: the WIMD income domain for Wales (Rank rows only; the file's Decile, "
            "Quintile, Quartile and Group rows are ignored to avoid mixing measures) and an "
            "English IMD extract carrying the overall IMD rank. Note the asymmetry: Wales is an "
            "income-specific measure, England is the composite index; swapping in an English "
            "income-domain extract would make the two strictly comparable.",
            "Harmonisation: ranks are not comparable across countries (Wales ranks run to about "
            "1,900, England to 33,755), so each rank is converted to a within-country percentile "
            "via (1 − (rank − 1)/(n − 1)) × 100, giving 100 = most deprived in that country. The "
            "map shows this percentile at LSOA level.",
        ]

    elif group == "Equity" and selection == "charging_inequality_gap":
        notes += [
            "Step 1, the distribution (manuscript-consistent): BEV% per area is privately kept "
            "battery-electric vehicles divided by all privately kept vehicles, times 100. At "
            "neighbourhood level the denominator is VEH0125 (BodyType Total, licensed plus "
            "SORN) and the numerator VEH0135; disclosure-suppressed BEV counts are imputed as "
            "2.5 (suppressed cell in an area elsewhere above 5) or 2.0 (area never above 5), "
            "only where the area's vehicle count exceeds 50. At district level: VEH0142 BEV over "
            "VEH0105 total. The sidebar's keepership filter selects the scope; Private "
            "reproduces the manuscript.",
            "Step 2, the measures: Hoover, Gini and GE(2) are computed over the full "
            "distribution, zeros included; GE(1) uses the 0 ln 0 = 0 limit; GE(0) is computed "
            "over the strictly positive subsample and the excluded zero count is reported.",
            "Step 3, the trend: the five indices are recomputed for the fourth quarter of every "
            "year 2011-2025 over the neighbourhood BEV% distribution, reproducing the "
            "manuscript's Figure 4.",
            "Step 4, the decomposition: GE indices are additively decomposable (Shorrocks, "
            "Econometrica 1980). At neighbourhood level each is split into between-district and "
            "within-district components (districts from the LSOA name prefix); at district "
            "level, across income-deprivation deciles (English districts only).",
        ]

    elif group == "Comparisons":
        if selection in {"Battery Electric (%)", "Plug-in (%)", "Ultra Low Emissions (%)"}:
            notes += [
                f"Derivation: the {SELECTION_LABELS.get(selection, selection)} value is the "
                "corresponding vehicle subset divided by all licensed vehicles in the same area, "
                "times 100. Numerator and denominator are prepared exactly as in the Vehicles "
                "group (same sources, filters and quarter) and joined on the ONS area code; "
                "areas missing either side are dropped.",
                "Interpretation caveat: stock registered to companies (lease and fleet "
                "registrations) is included, which is what pushes a few districts to extreme "
                "shares; restricting Keepership to PRIVATE would give a household-oriented "
                "measure.",
            ]
        elif selection == "plugin_per_1000":
            notes.append(
                "Derivation: district plug-in stock (VEH0132, battery electric plus plug-in "
                "hybrids, Keepership = Total) divided by the ONS mid-2024 all-ages population "
                "estimate for the same district (MYE2), times 1,000."
            )
        elif selection == "population_density":
            notes.append(
                "Derivation: taken directly from the ONS population density table (MYE5) as "
                "people per square kilometre, using the most recent year column in the file; no "
                "further transformation is applied."
            )

    if group in {"Vehicles", "Comparisons", "Output"} or selection == "charging_inequality_gap":
        notes.append(f"Keepership scope currently selected: {keepership}. All vehicle-based "
                     "figures on this page reflect it; population density does not use it.")
    notes.append(_GENERAL_HANDLING)
    return notes

# ------------------------------------------------------------
# Typography: one scale used everywhere in the page.
# ------------------------------------------------------------
FONT_FAMILY = "system-ui, -apple-system, 'Segoe UI', Arial, sans-serif"
FS_TITLE = "20px"     # sidebar title
FS_BODY = "14px"      # body text, radio labels, table cells, dropdowns
FS_SMALL = "13px"     # section labels, footer
FS_NOTE = "12.5px"    # notes under the map
BRAND_BLUE = "#003D7A"

# ============================================================
# Generic helpers
# ============================================================

def google_drive_direct_url(url: str) -> str:
    m = re.search(r"drive\.google\.com/file/d/([^/]+)", str(url))
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    return str(url).replace("/view?usp=drive_link", "").replace("/view?usp=sharing", "")


def _is_html(content: bytes) -> bool:
    return b"<html" in content[:1500].lower()


def _cache_path(url: str) -> Path:
    m = re.search(r"[?&]id=([\w-]+)", url)
    stem = m.group(1) if m else re.sub(r"\W+", "_", url)[-80:]
    return CACHE_DIR / f"{stem}.bin"


def _download(url: str) -> tuple[bytes, str]:
    """Download CSV/XLSX bytes, handling both Google Drive confirm flows.

    Large Drive files return an HTML interstitial. Older files expose a
    download_warning cookie; newer ones embed a confirm form posting to
    drive.usercontent.google.com. Both are handled. Successful downloads
    are cached on disk under CACHE_DIR so app restarts do not re-download
    (delete the cache folder to force a refresh).
    """
    url = google_drive_direct_url(url)

    cache_file = _cache_path(url)
    meta_file = cache_file.with_suffix(".ctype")
    if cache_file.exists():
        ctype = meta_file.read_text() if meta_file.exists() else ""
        return cache_file.read_bytes(), ctype

    session = requests.Session()
    r = session.get(url, timeout=300)
    r.raise_for_status()
    ctype = (r.headers.get("Content-Type") or "").lower()

    if _is_html(r.content):
        # Older flow: confirm token in a cookie.
        token = next((v for k, v in r.cookies.items() if k.startswith("download_warning")), None)
        if token:
            r = session.get(url, params={"confirm": token}, timeout=300)
            r.raise_for_status()
            ctype = (r.headers.get("Content-Type") or "").lower()

    if _is_html(r.content):
        # Newer flow: hidden confirm form posting to drive.usercontent.google.com.
        action = re.search(rb'action="([^"]+)"', r.content)
        inputs = re.findall(rb'name="([^"]+)" value="([^"]*)"', r.content)
        if action and inputs:
            r = session.get(
                action.group(1).decode().replace("&amp;", "&"),
                params={k.decode(): v.decode() for k, v in inputs},
                timeout=600,
            )
            r.raise_for_status()
            ctype = (r.headers.get("Content-Type") or "").lower()

    if _is_html(r.content):
        raise RuntimeError(
            "Google Drive returned an HTML page instead of a data file. "
            "Please set the file permission to 'Anyone with the link can view', "
            "or replace this URL with a direct CSV/XLSX download link."
        )

    cache_file.write_bytes(r.content)
    meta_file.write_text(ctype)
    return r.content, ctype


def load_data(url: str) -> pd.DataFrame:
    data, ctype = _download(url)

    if "spreadsheetml" in ctype or "excel" in ctype or data[:2] == b"PK":
        return pd.read_excel(io.BytesIO(data))

    try:
        return pd.read_csv(io.BytesIO(data), low_memory=False)
    except Exception:
        return pd.read_excel(io.BytesIO(data))


def _load_clean(url: str) -> pd.DataFrame:
    df = load_data(url)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def pick_col(cols, candidates) -> Optional[str]:
    low = {str(c).strip().lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
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
    return qs[0] if qs else None


def _is_total(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().eq("total")


KEEPERSHIP_OPTIONS = ["Total", "Private", "Company"]


def _keep_match(series: pd.Series, keepership: str) -> pd.Series:
    """Case-insensitive keepership match (files vary between 'PRIVATE' and
    'Private')."""
    return series.astype(str).str.strip().str.upper().eq(str(keepership).strip().upper())


def _keep_suffix(keepership: str) -> str:
    return "" if str(keepership).lower() == "total" else f", {str(keepership).lower()} keepership"


def sql_in(field: str, values: list[str]) -> str:
    vals = ["'" + str(v).replace("'", "''") + "'" for v in values]
    return f"{field} IN ({', '.join(vals)})"


def pick_field(fields, cands):
    low = {str(c).lower(): c for c in fields}
    for c in cands:
        if c in low:
            return low[c]
    return None


# ============================================================
# Inequality measures (Hoover, Gini, GE(0), GE(1), GE(2))
# ============================================================
#
# All measures operate on a single distribution y (BEV% across areas).
# They collapse the whole distribution into scalar summary statistics,
# so they are NOT per-area choropleth values. Definitions follow the
# manuscript (Equations 1-5) and Cowell (2011, Measuring Inequality).
#
# Zero handling (this matters for reproducing the manuscript's Figure 4):
#   - Hoover, Gini and GE(2) are computed over the FULL distribution,
#     zeros included.
#   - GE(1) (Theil's T) uses the 0*ln(0) = 0 limit, so zero areas
#     contribute nothing but stay in n and in the mean.
#   - GE(0) takes ln(y) directly, so it is computed over the strictly
#     positive subsample (with its own mean); n_dropped reports how many
#     zero areas were excluded from GE(0) only.

def inequality_measures(y) -> dict:
    """Return Hoover, Gini and GE(0..2) for a distribution y.

    Returns an empty dict if the distribution is unusable (empty, or
    non-positive mean).
    """
    y = np.asarray(y, dtype=float)
    y = y[np.isfinite(y)]
    n = int(len(y))
    if n == 0:
        return {}

    ybar = float(y.mean())
    if ybar <= 0:
        return {}

    # Hoover index (Eq. 1): share of total that must be redistributed.
    total = float(y.sum())
    H = float(np.abs(y - ybar).sum() / (2.0 * total)) if total > 0 else np.nan

    # Gini index (Eq. 2): O(n log n) sorted form, equal to the double sum.
    ys = np.sort(y)
    idx = np.arange(1, n + 1)
    G = float((2.0 * (idx * ys).sum()) / (n * ys.sum()) - (n + 1) / n) if ys.sum() > 0 else np.nan

    yp = y[y > 0]
    m = int(len(yp))
    n_dropped = n - m

    # GE(2) (Eq. 5): half squared coefficient of variation, zeros included.
    ratio_all = y / ybar
    GE2 = float((np.sum(ratio_all ** 2) - n) / (2.0 * n))

    # GE(1) (Eq. 4, Theil's T): zeros contribute 0 via the 0*ln(0) limit.
    if m > 0:
        rp = yp / ybar
        GE1 = float(np.sum(rp * np.log(rp)) / n)
        # GE(0) (Eq. 3, mean log deviation): positive subsample only.
        GE0 = float(-np.mean(np.log(yp / yp.mean())))
    else:
        GE0 = GE1 = np.nan

    return {
        "n": n,
        "n_positive": m,
        "n_dropped": n_dropped,
        "Hoover": H,
        "Gini": G,
        "GE(0)": GE0,
        "GE(1)": GE1,
        "GE(2)": GE2,
    }


def ge_decompose(y, groups, alpha: int = 1) -> dict:
    """Additive decomposition of GE(alpha) into within- and between-group parts.

    GE indices are additively decomposable (Shorrocks 1980):
        GE_total = GE_within + GE_between.
    Supports alpha in {0, 1, 2}. Requires strictly positive y for alpha in
    {0, 1}. Returns an empty dict if it cannot be computed.
    """
    y = np.asarray(y, dtype=float)
    groups = np.asarray(groups)

    mask = np.isfinite(y)
    y = y[mask]
    groups = groups[mask]

    # Only GE(0) requires strict positivity; GE(1) handles zeros via the
    # 0*ln(0) = 0 limit and GE(2) admits them directly.
    if alpha == 0:
        pos = y > 0
        y = y[pos]
        groups = groups[pos]

    n = int(len(y))
    if n == 0:
        return {}

    ybar = float(y.mean())
    if ybar <= 0:
        return {}

    total_stats = inequality_measures(y)
    total = total_stats.get(f"GE({alpha})", np.nan)
    if not np.isfinite(total):
        return {}

    within = 0.0
    uniq = [g for g in pd.unique(groups) if str(g) != "nan"]
    for g in uniq:
        yg = y[groups == g]
        ng = int(len(yg))
        if ng == 0:
            continue
        ybarg = float(yg.mean())
        if ybarg <= 0:
            continue

        pop_share = ng / n
        inc_share = (ng * ybarg) / (n * ybar)

        # Population-share weight depends on alpha (Shorrocks 1980).
        if alpha == 0:
            weight = pop_share
        elif alpha == 1:
            weight = inc_share
        else:  # alpha == 2
            weight = pop_share * (ybarg / ybar) ** 2

        ge_g = inequality_measures(yg).get(f"GE({alpha})", np.nan)
        if np.isfinite(ge_g):
            within += weight * ge_g

    between = total - within
    return {
        "alpha": alpha,
        "total": total,
        "within": within,
        "between": between,
        "n_groups": len(uniq),
    }


# ============================================================
# ArcGIS boundary helpers
# ============================================================

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
        LAD_FS, LAD_LAYER, field=code_field, values=list(codes_tuple),
        out_fields=out_fields, chunk_size=200,
    ), code_field, name_field


@lru_cache(maxsize=16)
def lsoa_geojson_for_codes(codes_tuple: tuple[str, ...]):
    code_field, name_field = lsoa_meta()
    out_fields = ",".join([x for x in [code_field, name_field] if x])
    return arcgis_query_geojson_in_chunks(
        LSOA_FS, LSOA_LAYER, field=code_field, values=list(codes_tuple),
        out_fields=out_fields, chunk_size=200,
    ), code_field, name_field


# ============================================================
# Data loaders (one per source file; lru_cache = per-process,
# _download's disk cache = across restarts)
# ============================================================

@lru_cache(maxsize=1)
def load_veh0105_lad_df():
    return _load_clean(VEH0105_URL)


@lru_cache(maxsize=1)
def load_veh0132_lad_df():
    return _load_clean(VEH0132_URL)


@lru_cache(maxsize=1)
def load_national_plugin_df():
    return _load_clean(VEH0141_URL)


@lru_cache(maxsize=1)
def load_veh0142_lad_df():
    return _load_clean(VEH0142_URL)


def _load_lsoa_csv(url: str, dim_cols: tuple[str, ...], quarters: str = "latest") -> pd.DataFrame:
    """Load an LSOA CSV keeping only code/name columns, the given dimension
    columns and either the LATEST quarter column (quarters="latest") or every
    fourth-quarter column (quarters="q4"). The LSOA files are wide (60+
    quarter columns) and large; parsing only what is needed keeps memory
    and startup time reasonable."""
    data, _ = _download(url)
    header = pd.read_csv(io.BytesIO(data), nrows=0)
    header.columns = [str(c).strip() for c in header.columns]
    qs = available_quarters(header)
    keep = [c for c in header.columns
            if c in {"LSOA21CD", "LSOA21NM", "LSOA11CD", "LSOA11NM", *dim_cols}]
    if quarters == "q4":
        keep += [q for q in qs if q.endswith("Q4")]
    elif qs:
        keep.append(qs[0])
    df = pd.read_csv(io.BytesIO(data), usecols=keep, low_memory=False)
    df.columns = [str(c).strip() for c in df.columns]
    return df


@lru_cache(maxsize=1)
def load_veh0125_lsoa_df():
    return _load_lsoa_csv(VEH0125_URL, ("BodyType", "Keepership", "LicenceStatus"))


@lru_cache(maxsize=1)
def load_veh0145_lsoa_df():
    return _load_lsoa_csv(VEH0145_URL, ("Fuel", "Keepership"))


@lru_cache(maxsize=1)
def load_veh0135_lsoa_df():
    return _load_lsoa_csv(VEH0135_URL, ("Fuel", "Keepership"))


@lru_cache(maxsize=1)
def load_population_df():
    return _load_clean(POPULATION_URL)


@lru_cache(maxsize=1)
def load_pop_density_df():
    return _load_clean(POP_DENSITY_URL)


# ============================================================
# Dataset preparation
# ============================================================

LAD_CODE_PATTERN = r"^(E06|E07|E08|E09|W06)\d+"
LSOA_CODE_PATTERN = r"^(E01|W01)\d+"


_METRIC_LABELS = {
    "All": "All licensed vehicles",
    "Plug-in": "Plug-in vehicles (BEV + PHEV)",
    "Battery Electric": "Battery electric vehicles",
    "Ultra Low Emissions": "Ultra low emission vehicles",
}


def _tidy(df: pd.DataFrame, code_col: str, name_col: Optional[str], value_col: str, pattern: str) -> pd.DataFrame:
    out = df[[code_col] + ([name_col] if name_col else []) + [value_col]].copy()
    out.columns = ["area_code"] + (["area_name"] if name_col else []) + ["value"]
    if "area_name" not in out.columns:
        out["area_name"] = out["area_code"]
    out["area_code"] = out["area_code"].astype(str).str.strip()
    out["area_name"] = out["area_name"].astype(str).str.strip()
    out["value"] = out["value"].apply(parse_num)
    return out[out["area_code"].str.match(pattern, na=False)].dropna(subset=["value"])


def _cap_lsoa(df: pd.DataFrame, cap: int = LSOA_RENDER_CAP) -> tuple[pd.DataFrame, bool]:
    """Cap LSOA rows for map responsiveness, keeping all Welsh LSOAs first."""
    if len(df) <= cap:
        return df, False
    wales = df[df["area_code"].str.startswith("W")]
    england = df[~df["area_code"].str.startswith("W")]
    out = pd.concat([wales, england.head(max(cap - len(wales), 0))], ignore_index=True)
    return out, True


def prepare_vehicle_data(selection: str, geo_level: str = "LAD",
                         keepership: str = "Total",
                         quarter: Optional[str] = None) -> tuple[pd.DataFrame, str]:
    """Per-area vehicle stock, filtered to the requested keepership.

    LAD level: 'All' from VEH0105 (thousands scaled to vehicles); Plug-in
    and Battery Electric from VEH0142 (body type Total); ULEV from
    VEH0132. LSOA level: VEH0125 / VEH0145 / VEH0135. `quarter` pins a
    specific quarter column (used so ratio numerators and denominators
    share one quarter); otherwise the latest available quarter is used.
    The chosen quarter is exposed as out.attrs["quarter"].
    """
    if geo_level == "LSOA":
        pattern = LSOA_CODE_PATTERN
        scale = 1.0
        if selection == "All":
            df = load_veh0125_lsoa_df().copy()
            df = df[_is_total(df["BodyType"]) & _keep_match(df["Keepership"], keepership)
                    & df["LicenceStatus"].astype(str).str.strip().str.lower().eq("licensed")]
            source = "DfT Veh0125"
        elif selection in {"Plug-in", "Battery Electric"}:
            df = load_veh0145_lsoa_df().copy()
            fuel_ok = _is_total(df["Fuel"]) if selection == "Plug-in" \
                else df["Fuel"].astype(str).str.strip().str.upper().eq("BATTERY ELECTRIC")
            df = df[fuel_ok & _keep_match(df["Keepership"], keepership)]
            source = "DfT Veh0145"
        elif selection == "Ultra Low Emissions":
            df = load_veh0135_lsoa_df().copy()
            df = df[_is_total(df["Fuel"]) & _keep_match(df["Keepership"], keepership)]
            source = "DfT Veh0135"
        else:
            return pd.DataFrame(), geo_level
        code_col = pick_col(df.columns, ["lsoa21cd", "lsoa11cd", "lsoa code"])
        name_col = pick_col(df.columns, ["lsoa21nm", "lsoa11nm", "lsoa name"])
    else:
        geo_level = "LAD"
        pattern = LAD_CODE_PATTERN
        if selection == "All":
            df = load_veh0105_lad_df().copy()
            df = df[_is_total(df["BodyType"]) & _is_total(df["Fuel"])
                    & _keep_match(df["Keepership"], keepership)]
            scale = 1000.0  # VEH0105 is in thousands
            source = "DfT Veh0105"
        elif selection in {"Plug-in", "Battery Electric"}:
            df = load_veh0142_lad_df().copy()
            fuel_ok = _is_total(df["Fuel"]) if selection == "Plug-in" \
                else df["Fuel"].astype(str).str.strip().str.upper().eq("BATTERY ELECTRIC")
            df = df[_is_total(df["BodyType"]) & fuel_ok
                    & _keep_match(df["Keepership"], keepership)]
            scale = 1.0
            source = "DfT Veh0142"
        elif selection == "Ultra Low Emissions":
            df = load_veh0132_lad_df().copy()
            df = df[_is_total(df["Fuel"]) & _keep_match(df["Keepership"], keepership)]
            scale = 1.0
            source = "DfT Veh0132"
        else:
            return pd.DataFrame(), geo_level
        code_col = pick_col(df.columns, ["ons code", "local authority code", "lad code", "code"])
        name_col = pick_col(df.columns, ["ons geography", "local authority", "lad name", "geography", "name"])

    quarter_col = quarter if (quarter and quarter in df.columns) else latest_quarter_col(df)
    if code_col is None or quarter_col is None:
        return pd.DataFrame(), geo_level

    out = _tidy(df, code_col, name_col, quarter_col, pattern)
    if out.empty:
        return pd.DataFrame(), geo_level

    out["value"] = out["value"] * scale
    out = out.groupby(["area_code", "area_name"], as_index=False)["value"].sum()
    out["metric"] = (f"{_METRIC_LABELS.get(selection, selection)}, {source}, "
                     f"{quarter_col}{_keep_suffix(keepership)}")
    out["geography"] = geo_level
    out.attrs["quarter"] = quarter_col
    return out, geo_level


@lru_cache(maxsize=1)
def load_income_deprivation_df() -> pd.DataFrame:
    """Harmonised LSOA income-deprivation percentiles for England and Wales.

    Ranks are converted to within-country percentiles (100 = most deprived)
    because Welsh WIMD ranks (income domain, 1..~1,900) and English IMD
    ranks (overall index, 1..33,755) are not directly comparable. Wales
    uses the WIMD income domain; England uses the overall IMD rank from
    the extract currently wired (an income-domain extract can replace it).
    """
    frames = []

    try:
        w = _load_clean(WIMD_WALES_URL)
        w = w[w["Domain"].astype(str).str.contains("income", case=False, na=False)]
        w = w[w["Data description"].astype(str).str.strip().eq("Rank")]
        rank = w["Data values"].apply(parse_num)
        n = rank.max()
        frames.append(pd.DataFrame({
            "area_code": w["Area code"].astype(str).str.strip(),
            "area_name": w["Area name"].astype(str).str.strip(),
            "value": (1 - (rank - 1) / (n - 1)) * 100,
        }))
    except Exception:
        pass

    try:
        e = _load_clean(IMD_ENGLAND_URL)
        code_col = pick_col(e.columns, ["lsoa code (2021)", "lsoa code (2011)", "lsoa code"])
        name_col = pick_col(e.columns, ["lsoa name (2021)", "lsoa name (2011)", "lsoa name"])
        rank_col = next((c for c in e.columns if "rank" in str(c).lower()), None)
        if code_col and rank_col:
            rank = e[rank_col].apply(parse_num)
            n = rank.max()
            frames.append(pd.DataFrame({
                "area_code": e[code_col].astype(str).str.strip(),
                "area_name": e[name_col].astype(str).str.strip() if name_col else e[code_col].astype(str).str.strip(),
                "value": (1 - (rank - 1) / (n - 1)) * 100,
            }))
    except Exception:
        pass

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out = out[out["area_code"].str.match(LSOA_CODE_PATTERN, na=False)].dropna(subset=["value"])
    return out


def prepare_income_deprivation() -> tuple[pd.DataFrame, str]:
    out = load_income_deprivation_df().copy()
    if out.empty:
        return pd.DataFrame(), "LSOA"
    out["metric"] = "Income deprivation percentile (100 = most deprived; WIMD income domain for Wales, IMD rank for England, each within country)"
    out["geography"] = "LSOA"
    return out, "LSOA"


def prepare_comparison_data(selection: str, geo_level: str = "LAD",
                            keepership: str = "Total") -> tuple[pd.DataFrame, str]:
    """Derived metrics: EV shares of the vehicle stock and population-based rates."""
    if selection == "population_density":
        df = load_pop_density_df().copy()
        code_col = pick_col(df.columns, ["code"])
        name_col = pick_col(df.columns, ["name"])
        year_cols = {int(m.group(1)): c for c in df.columns
                     if (m := re.match(r"^(\d{4}) people per sq\. km$", str(c).strip()))}
        if not year_cols or code_col is None:
            return pd.DataFrame(), "LAD"
        year = max(year_cols)
        out = _tidy(df, code_col, name_col, year_cols[year], LAD_CODE_PATTERN)
        out["area_name"] = out["area_name"].str.title()
        out["metric"] = f"Population density, people per sq km (ONS MYE5, mid-{year})"
        out["geography"] = "LAD"
        return out, "LAD"

    if selection == "plugin_per_1000":
        ev, _ = prepare_vehicle_data("Plug-in", geo_level="LAD", keepership=keepership)
        pop = load_population_df().copy()
        code_col = pick_col(pop.columns, ["code"])
        pop_col = pick_col(pop.columns, ["all ages"])
        if ev.empty or code_col is None or pop_col is None:
            return pd.DataFrame(), "LAD"
        pop = _tidy(pop, code_col, pick_col(pop.columns, ["name"]), pop_col, LAD_CODE_PATTERN)
        out = ev.merge(pop[["area_code", "value"]].rename(columns={"value": "pop"}), on="area_code")
        out["value"] = np.where(out["pop"] > 0, out["value"] / out["pop"] * 1000, np.nan)
        out = out.dropna(subset=["value"])[["area_code", "area_name", "value"]]
        out["metric"] = ("Plug-in vehicles per 1,000 residents "
                         f"(Veh0142 {ev.attrs.get('quarter', '')} / ONS MYE2 mid-2024"
                         f"{_keep_suffix(keepership)})")
        out["geography"] = "LAD"
        return out, "LAD"

    part_map = {
        "Battery Electric (%)": "Battery Electric",
        "Plug-in (%)": "Plug-in",
        "Ultra Low Emissions (%)": "Ultra Low Emissions",
    }
    part_sel = part_map.get(selection)
    if part_sel is None:
        return pd.DataFrame(), geo_level

    # Pin numerator and denominator to the SAME quarter (VEH0142 can run one
    # quarter ahead of the other tables).
    all_df, geo = prepare_vehicle_data("All", geo_level=geo_level, keepership=keepership)
    common_q = all_df.attrs.get("quarter") if not all_df.empty else None
    part_df, geo = prepare_vehicle_data(part_sel, geo_level=geo_level,
                                        keepership=keepership, quarter=common_q)
    if all_df.empty or part_df.empty:
        return pd.DataFrame(), geo

    base = all_df[["area_code", "area_name", "value"]].rename(columns={"value": "all_value"})
    part = part_df[["area_code", "value"]].rename(columns={"value": "part_value"})
    out = base.merge(part, on="area_code", how="inner")
    out["value"] = np.where(out["all_value"] > 0, (out["part_value"] / out["all_value"]) * 100, np.nan)
    out = out.dropna(subset=["value"])[["area_code", "area_name", "value"]]
    out["metric"] = (f"{_METRIC_LABELS[part_sel]} as % of all licensed vehicles"
                     f" ({common_q}{_keep_suffix(keepership)})")
    out["geography"] = geo
    return out, geo


def prepare_dataset(group: str, selection: str, geo_level: str = "LAD",
                    area_filter: Optional[str] = None,
                    keepership: str = "Total") -> tuple[pd.DataFrame, str]:
    """Return (tidy dataframe, geography). The area filter is applied BEFORE
    the LSOA render cap, so any area remains reachable via the dropdown."""
    if group == "Vehicles":
        df, geo = prepare_vehicle_data(selection, geo_level=geo_level, keepership=keepership)
    elif group == "Equity" and selection == "income_deprivation":
        df, geo = prepare_income_deprivation()
    elif group == "Comparisons":
        df, geo = prepare_comparison_data(selection, geo_level=geo_level, keepership=keepership)
    else:
        return pd.DataFrame(), "NONE"

    if area_filter and not df.empty and "area_name" in df.columns:
        df = df[df["area_name"].astype(str).eq(str(area_filter))].copy()

    if geo == "LSOA" and not df.empty:
        df, capped = _cap_lsoa(df)
        if capped:
            df = df.copy()
            df.attrs["capped"] = True

    return df, geo


# ============================================================
# Manuscript-consistent BEV% (private keepership) and inequality trend
# ============================================================
#
# These follow the manuscript's Methods so the dashboard reproduces its
# Figure 4 series and inequality values:
#   - Private keepership only (company records carry large outliers).
#   - Neighbourhood denominator: VEH0125, BodyType Total, licensed + SORN.
#   - Neighbourhood numerator: VEH0135, BATTERY ELECTRIC.
#   - Fixed imputation for disclosure-suppressed BEV cells where the
#     area's vehicle count exceeds 50: 2.5 for a suppressed cell in an
#     area that elsewhere exceeds 5; 2.0 for areas absent from the BEV
#     table (counts never above 5).
#   - Fourth quarter of each year, 2011-2025.

PAPER_Q4S = [f"{y} Q4" for y in range(2011, 2026)]


@lru_cache(maxsize=1)
def load_veh0125_q4_df():
    return _load_lsoa_csv(VEH0125_URL, ("BodyType", "Keepership", "LicenceStatus"), quarters="q4")


@lru_cache(maxsize=1)
def load_veh0135_q4_df():
    return _load_lsoa_csv(VEH0135_URL, ("Fuel", "Keepership"), quarters="q4")


@lru_cache(maxsize=3)
def paper_bev_panel(keepership: str = "Private") -> tuple[pd.DataFrame, pd.Series]:
    """Return (BEV% panel, district labels) for a keepership scope.

    The panel is indexed by LSOA code with one column per fourth quarter
    2011-2025; districts are derived from the LSOA name prefix (an LSOA
    name is its district name plus a numeric suffix). Private keepership
    reproduces the manuscript."""
    q4s = [q for q in PAPER_Q4S]

    den = load_veh0125_q4_df().copy()
    den = den[_is_total(den["BodyType"]) & _keep_match(den["Keepership"], keepership)]
    den = den[den["LSOA21CD"].astype(str).str.match(LSOA_CODE_PATTERN)]
    names = den.drop_duplicates("LSOA21CD").set_index("LSOA21CD")["LSOA21NM"].astype(str)
    q4s = [q for q in q4s if q in den.columns]
    for q in q4s:
        den[q] = den[q].apply(parse_num)
    den = den.groupby("LSOA21CD")[q4s].sum(min_count=1)  # Licensed + SORN

    num = load_veh0135_q4_df().copy()
    num = num[num["Fuel"].astype(str).str.strip().str.upper().eq("BATTERY ELECTRIC")
              & _keep_match(num["Keepership"], keepership)]
    num = num[num["LSOA21CD"].astype(str).str.match(LSOA_CODE_PATTERN)]
    num = num.set_index("LSOA21CD")[[q for q in q4s if q in num.columns]]

    suppressed = num.apply(lambda c: c.astype(str).str.strip().str.lower().eq("[c]"))
    suppressed = suppressed.reindex(index=den.index, columns=q4s, fill_value=False)
    numeric = num.apply(lambda c: c.apply(parse_num)).reindex(index=den.index, columns=q4s)

    ever_over5 = (numeric > 5).any(axis=1)
    impute_val = pd.Series(np.where(ever_over5, 2.5, 2.0), index=den.index)

    panel = {}
    for q in q4s:
        dq = den[q]
        n = numeric[q]
        needs = suppressed[q] | n.isna()
        n = n.where(~(needs & (dq > 50)), impute_val)
        bevpct = np.where((dq > 0) & n.notna(), n / dq * 100, np.nan)
        panel[q] = pd.Series(bevpct, index=den.index)

    districts = names.str.replace(r"\s\d{3}[A-Z]$", "", regex=True)
    return pd.DataFrame(panel), districts


@lru_cache(maxsize=3)
def paper_inequality_trend(keepership: str = "Private") -> pd.DataFrame:
    """The manuscript's Figure 4 series: five inequality indices over the
    neighbourhood BEV% distribution, Q4 2011-2025. Private keepership
    reproduces the manuscript."""
    panel, _ = paper_bev_panel(keepership)
    rows = []
    for q in panel.columns:
        y = panel[q].dropna().to_numpy()
        m = inequality_measures(y)
        if m:
            rows.append({"quarter": q, "areas": m["n"], "zero_areas": m["n_dropped"],
                         "Hoover": m["Hoover"], "Gini": m["Gini"],
                         "GE(0)": m["GE(0)"], "GE(1)": m["GE(1)"], "GE(2)": m["GE(2)"]})
    return pd.DataFrame(rows)


@lru_cache(maxsize=3)
def paper_lad_bev_pct(keepership: str = "Private") -> pd.DataFrame:
    """District-level BEV% (VEH0142 BEV over VEH0105 total, same quarter)
    for a keepership scope. Private matches the manuscript's district scope."""
    all_df = load_veh0105_lad_df().copy()
    all_df = all_df[_is_total(all_df["BodyType"]) & _is_total(all_df["Fuel"])
                    & _keep_match(all_df["Keepership"], keepership)]
    code_col = pick_col(all_df.columns, ["ons code", "code"])
    name_col = pick_col(all_df.columns, ["ons geography", "geography", "name"])
    quarter = latest_quarter_col(all_df)
    base = _tidy(all_df, code_col, name_col, quarter, LAD_CODE_PATTERN)
    base["value"] = base["value"] * 1000.0
    base = base.groupby(["area_code", "area_name"], as_index=False)["value"].sum()

    bev = load_veh0142_lad_df().copy()
    bev = bev[_is_total(bev["BodyType"])
              & bev["Fuel"].astype(str).str.strip().str.upper().eq("BATTERY ELECTRIC")
              & _keep_match(bev["Keepership"], keepership)]
    code_col = pick_col(bev.columns, ["ons code", "code"])
    quarter_b = quarter if quarter in bev.columns else latest_quarter_col(bev)
    part = _tidy(bev, code_col, None, quarter_b, LAD_CODE_PATTERN)
    part = part.groupby("area_code", as_index=False)["value"].sum().rename(columns={"value": "bev"})

    out = base.merge(part, on="area_code", how="left")
    out["bev"] = out["bev"].fillna(0.0)
    out["value"] = np.where(out["value"] > 0, out["bev"] / out["value"] * 100, np.nan)
    out = out.dropna(subset=["value"])[["area_code", "area_name", "value"]]
    out.attrs["quarter"] = quarter
    return out


# ============================================================
# Inequality-gap dataset (Equity -> Charging Inequality Gap)
# ============================================================
#
# This selection does NOT produce a choropleth. It computes the BEV%
# distribution across areas and returns the scalar inequality measures
# from Inequality_Measures.docx, plus GE decompositions by income
# deprivation band where that source is available.

def _deprivation_bands(geo_level: str) -> tuple[Optional[pd.DataFrame], str]:
    """Return (area_code -> deprivation decile 1..10, grouping label)."""
    label = "Income deprivation deciles"
    try:
        if geo_level == "LSOA":
            dep = load_income_deprivation_df()[["area_code", "value"]].dropna().copy()
        else:
            # LAD level: aggregate the English IMD extract (it carries LAD
            # codes); Welsh LADs are absent until a Welsh LSOA->LAD lookup
            # or LAD-level WIMD extract is wired.
            e = _load_clean(IMD_ENGLAND_URL)
            lad_col = pick_col(e.columns, ["local authority district code (2024)", "local authority district code (2019)"])
            rank_col = next((c for c in e.columns if "rank" in str(c).lower()), None)
            if lad_col is None or rank_col is None:
                return None, label
            e["value"] = e[rank_col].apply(parse_num)
            dep = e.groupby(lad_col, as_index=False)["value"].mean().rename(columns={lad_col: "area_code"})
            label = "Income deprivation deciles (English LADs only)"
    except Exception:
        return None, label

    if dep.empty:
        return None, label

    try:
        dep["dep_band"] = pd.qcut(dep["value"].rank(method="first"), 10, labels=False) + 1
    except Exception:
        return None, label

    return dep[["area_code", "dep_band"]], label


def _diagnose_bev_pipeline(geo_level: str) -> str:
    """Return a human-readable reason the BEV% distribution is empty."""
    lines = []

    try:
        all_df, _ = prepare_vehicle_data("All", geo_level=geo_level)
        if all_df.empty:
            lines.append(
                f"All-vehicle dataset ({geo_level}) produced 0 usable rows — the file "
                "downloaded but no geography-code column or quarter/value column could be "
                "matched, or no rows matched the expected area-code pattern."
            )
        else:
            lines.append(f"All-vehicle dataset: {len(all_df)} areas loaded ✓")
    except Exception as e:
        lines.append(f"All-vehicle dataset failed to load: {e}")
        all_df = pd.DataFrame()

    try:
        part_df, _ = prepare_vehicle_data("Battery Electric", geo_level=geo_level)
        if part_df.empty:
            lines.append(
                f"Battery-electric dataset ({geo_level}) produced 0 usable rows — check that the "
                "Google Drive link is a direct download and is shared as "
                "'Anyone with the link can view'."
            )
        else:
            lines.append(f"Battery-electric dataset: {len(part_df)} areas loaded ✓")
    except Exception as e:
        lines.append(f"Battery-electric dataset failed to load: {e}")
        part_df = pd.DataFrame()

    if not all_df.empty and not part_df.empty:
        overlap = set(all_df["area_code"]) & set(part_df["area_code"])
        if not overlap:
            lines.append(
                "Both datasets loaded, but they share no common area codes — the "
                "All-vehicle and BEV files likely use different geography vintages "
                "or levels. The BEV% ratio needs matching codes in both."
            )
        else:
            lines.append(f"Matching areas between the two datasets: {len(overlap)} ✓")

    return "<br>".join(lines)


def prepare_inequality_gap(geo_level: str = "LAD", keepership: str = "Private") -> tuple[pd.DataFrame, str, dict]:
    """Compute BEV% inequality measures and decompositions.

    Returns (summary_table, geo_level, extras) where summary_table is a
    tidy per-measure table suitable for the DataTable, and extras carries
    the decomposition rows for display.
    """
    # Manuscript-consistent BEV% under the selected keepership; Private
    # reproduces the manuscript. At neighbourhood level the full
    # imputation rules apply (paper_bev_panel).
    geo = geo_level
    if geo_level == "LSOA":
        panel, districts = paper_bev_panel(keepership)
        latest_q = panel.columns[-1]
        series = panel[latest_q].dropna()
        bev = pd.DataFrame({"area_code": series.index, "value": series.to_numpy()})
        group_labels = districts.reindex(series.index)
        group_name = "Between/within districts (Shorrocks)"
    else:
        geo = "LAD"
        bev = paper_lad_bev_pct(keepership)
        group_labels = None
        group_name = "Income deprivation deciles (English LADs only)"

    if bev.empty or "value" not in bev.columns:
        return pd.DataFrame(), geo, {}

    y = bev["value"].to_numpy(dtype=float)
    measures = inequality_measures(y)
    if not measures:
        return pd.DataFrame(), geo, {}

    label_map = {
        "Hoover": "Hoover index (H)",
        "Gini": "Gini index (G)",
        "GE(0)": "GE(0) — mean log deviation / Theil's L",
        "GE(1)": "GE(1) — Theil's T",
        "GE(2)": "GE(2) — ½ squared coeff. of variation",
    }
    range_map = {
        "Hoover": "0 (equal) – 0.5",
        "Gini": "0 (equal) – 1",
        "GE(0)": "0 (equal) – ∞",
        "GE(1)": "0 (equal) – ∞",
        "GE(2)": "0 (equal) – ∞",
    }

    rows = []
    for key in ["Hoover", "Gini", "GE(0)", "GE(1)", "GE(2)"]:
        val = measures.get(key, np.nan)
        rows.append({
            "measure": label_map[key],
            "value": round(val, 4) if np.isfinite(val) else "NA",
            "range": range_map[key],
        })

    summary = pd.DataFrame(rows)

    # GE decomposition: between/within districts at neighbourhood level
    # (as in the manuscript); deprivation deciles at district level.
    decomposition_rows = []
    if group_labels is not None:
        gl = group_labels.to_numpy()
        for alpha in (0, 1, 2):
            d = ge_decompose(y, gl, alpha=alpha)
            if d:
                decomposition_rows.append({
                    "Grouping": group_name,
                    "Index": f"GE({alpha})",
                    "Total": round(d["total"], 4),
                    "Between": round(d["between"], 4),
                    "Within": round(d["within"], 4),
                    "Groups": d["n_groups"],
                })
    else:
        dep, dep_label = _deprivation_bands(geo)
        if dep is not None:
            merged = bev.merge(dep, on="area_code", how="inner").dropna(subset=["value", "dep_band"])
            if not merged.empty and merged["dep_band"].nunique() > 1:
                for alpha in (0, 1, 2):
                    d = ge_decompose(merged["value"].to_numpy(float), merged["dep_band"].to_numpy(), alpha=alpha)
                    if d:
                        decomposition_rows.append({
                            "Grouping": dep_label,
                            "Index": f"GE({alpha})",
                            "Total": round(d["total"], 4),
                            "Between": round(d["between"], 4),
                            "Within": round(d["within"], 4),
                            "Groups": d["n_groups"],
                        })

    try:
        trend = paper_inequality_trend(keepership)
    except Exception:
        trend = pd.DataFrame()

    extras = {
        "measures": measures,
        "n_areas": int(measures.get("n", 0)),
        "n_dropped": int(measures.get("n_dropped", 0)),
        "decomposition": decomposition_rows,
        "geo": geo,
        "trend": trend,
    }
    return summary, geo, extras


# ============================================================
# Folium map builders
# ============================================================

def add_base_layers(m: folium.Map):
    # OpenStreetMap is the default: recent folium versions warn that CartoDB
    # tiles now require an API key, so the CartoDB styles are optional extras.
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap", show=True).add_to(m)
    folium.TileLayer("cartodbpositron", name="CartoDB Positron", show=False).add_to(m)
    folium.TileLayer("cartodbdark_matter", name="CartoDB Dark", show=False).add_to(m)


def add_css(m: folium.Map):
    css = """
    <style>
    .leaflet-tooltip {
        font-size: 14px !important;
        font-weight: 600 !important;
        line-height: 1.35 !important;
        padding: 8px 10px !important;
    }
    .leaflet-control-layers {
        font-size: 13px !important;
    }
    </style>
    """
    m.get_root().html.add_child(folium.Element(css))


def _colour_ramp_for_group(group: str) -> str:
    """Approximate the selected Folium ColorBrewer palette as a CSS gradient."""
    ramps = {
        "Vehicles": "linear-gradient(to right, #edf8fb, #b3cde3, #8c96c6, #8856a7, #810f7c)",       # BuPu
        "Equity": "linear-gradient(to right, #feebe2, #fbb4b9, #f768a1, #c51b8a, #7a0177)",         # RdPu
        "Comparisons": "linear-gradient(to right, #f2f0f7, #cbc9e2, #9e9ac8, #756bb1, #54278f)",    # Purples
    }
    return ramps.get(group, ramps["Vehicles"])


def add_hover_value_legend(m: folium.Map, df: pd.DataFrame, group: str, selection: str, hover_layer_name: str):
    """Add an ONS-style legend whose marker moves on feature hover."""
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
        padding: 16px 18px 14px 18px;
        font-family: Arial, sans-serif;
        color: #0b0c0c;
        pointer-events: none;
    ">
        <div style="font-size:15px; font-weight:700; margin-bottom:6px; line-height:1.4;">{group}: {selection}</div>
        <div style="font-size:12.5px; margin-bottom:12px; color:#505a5f; line-height:1.45;">{metric}</div>
        <div id="ons-hover-area" style="font-size:14px; font-weight:700; min-height:19px; margin-bottom:7px; line-height:1.4;">Hover over an area</div>
        <div id="ons-hover-value" style="font-size:19px; font-weight:800; margin-bottom:10px; line-height:1.35;">Mean: {vmean:,.0f}</div>
        <div style="position:relative; height:30px; margin:0 2px 6px 2px;">
            <div style="position:absolute; left:0; right:0; top:12px; height:12px; background:{ramp}; border:1px solid #6b7280;"></div>
            <div id="ons-hover-marker" style="position:absolute; left:{initial_pct:.2f}%; top:0; transform:translateX(-50%); width:3px; height:28px; background:#0b0c0c; box-shadow:0 0 0 1px rgba(255,255,255,0.9);"></div>
        </div>
        <div style="display:flex; justify-content:space-between; font-size:12.5px; color:#0b0c0c; line-height:1.4;">
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
        // The GeoJson layer variable is created by a later script, so poll for
        // it instead of assuming it exists (a one-shot lookup silently fails
        // and the marker never moves). Listen at the layer-group level so the
        // handler survives sublayer rebuilds.
        var tries = 0;
        (function attach() {{
            var hoverLayer = window['{hover_layer_name}'];
            if (!hoverLayer || !hoverLayer.on) {{
                if (++tries < 150) return setTimeout(attach, 100);
                return;
            }}
            function handleOver(e) {{
                try {{
                    var target = e.propagatedFrom || e.layer || e.target;
                    var props = target && target.feature ? target.feature.properties : null;
                    if (props) setLegend(props.Area, Number(props.ValueRaw));
                }} catch (err) {{ /* keep the legend alive */ }}
            }}
            hoverLayer.on('mouseover', handleOver);
            hoverLayer.on('mouseout', resetLegend);
            if (hoverLayer.eachLayer) {{
                hoverLayer.eachLayer(function(layer) {{
                    layer.on('mouseover', function(e) {{
                        var props = e.target && e.target.feature ? e.target.feature.properties : null;
                        if (props) setLegend(props.Area, Number(props.ValueRaw));
                    }});
                    layer.on('mouseout', resetLegend);
                }});
            }}
        }})();
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
    display = SELECTION_LABELS.get(selection, selection)
    layer_name = f"{group}: {display}"

    # Quantile bins keep skewed distributions readable (a handful of
    # fleet-registration areas would otherwise compress the whole scale
    # into one pale class). Falls back to folium's default equal-interval
    # bins when there are too few distinct values.
    vals = pd.to_numeric(df["value"], errors="coerce").dropna()
    bins = None
    if vals.nunique() >= 12:
        edges = np.unique(np.quantile(vals, np.linspace(0, 1, 7)))
        if len(edges) >= 4:
            bins = [float(b) for b in edges]

    ch = folium.Choropleth(
        geo_data=gj,
        data=df,
        columns=["area_code", "value"],
        key_on=f"feature.properties.{code_field}",
        fill_color=colour,
        fill_opacity=CHOROPLETH_FILL_OPACITY,
        line_opacity=0.6,
        line_weight=0.8,
        nan_fill_color="#f0f0f0",
        name=layer_name,
        show=True,
        **({"bins": bins} if bins else {}),
    )
    # Drop the built-in top colour bar: with quantile bins its tick labels
    # jam together; the hover legend (bottom left) carries the scale instead.
    for key in list(ch._children):
        if key.startswith("color_map"):
            del ch._children[key]
    ch.add_to(m)

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
            "Value": f"{float(value):,.1f}" if pd.notna(value) else "NA",
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
                "background-color:white; color:black; font-size:14px; "
                "font-weight:600; padding:8px; border:2px solid #444; border-radius:6px;"
            ),
        ),
    ).add_to(m)

    add_hover_value_legend(m, df, group, display, hover_layer.get_name())

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


# ============================================================
# Trends view (Output -> Trends): national plug-in uptake, VEH0141
# ============================================================

# Fixed categorical colours per country (identity follows the entity across
# every chart; order and hexes validated for colour-vision-deficiency
# separation on a white surface).
COUNTRY_ORDER = ["England", "Wales", "Scotland", "Northern Ireland"]
COUNTRY_COLOURS = {
    "England": "#2a78d6",
    "Wales": "#eb6834",
    "Scotland": "#1baf7a",
    "Northern Ireland": "#eda100",
}

_COUNTRY_CODES = {
    "E92000001": "England",
    "W92000004": "Wales",
    "S92000003": "Scotland",
    "N92000002": "Northern Ireland",
}


@lru_cache(maxsize=1)
def national_total_stock_df() -> pd.DataFrame:
    """Country-level total licensed vehicles per quarter from VEH0105
    (BodyType/Fuel/Keepership all Total, thousands scaled to vehicles)."""
    df = load_veh0105_lad_df().copy()
    df = df[_is_total(df["BodyType"]) & _is_total(df["Fuel"]) & _is_total(df["Keepership"])]
    code_col = pick_col(df.columns, ["ons code", "code"])
    if code_col is None:
        return pd.DataFrame()
    df[code_col] = df[code_col].astype(str).str.strip()
    df = df[df[code_col].isin(_COUNTRY_CODES)]
    rows = []
    for _, r in df.iterrows():
        for q in available_quarters(df):
            v = parse_num(r.get(q))
            if pd.notna(v):
                rows.append({"Geography": _COUNTRY_CODES[r[code_col]],
                             "quarter": q, "total_stock": v * 1000.0})
    return pd.DataFrame(rows)


def _trends_layout(fig, title, ytitle, show_legend=True):
    fig.update_layout(
        title=dict(text=title, font=dict(size=16)),
        template="plotly_white",
        font=dict(family="system-ui, 'Segoe UI', Arial, sans-serif", size=13.5),
        hovermode="x unified",
        yaxis_title=ytitle,
        xaxis_title=None,
        showlegend=show_legend,
        legend=dict(orientation="h", yanchor="top", y=-0.14, x=0),
        margin=dict(l=60, r=20, t=52, b=90 if show_legend else 40),
    )


def _render_trends(keepership: str = "Total"):
    """Trends page: three charts from the national and district datasets."""
    try:
        df = load_national_plugin_df().copy()
    except Exception as e:
        page = _inequality_message_page("Trends", f'<div class="note">Could not load VEH0141: {e}</div>')
        return page, [], f"Trends could not be loaded: {e}"

    df = df[_is_total(df["BodyType"])]
    df = df[df["Geography"].isin(COUNTRY_ORDER)].copy()
    for c in ["Battery electric", "Total"]:
        df[c] = df[c].apply(parse_num)
    df["q"] = df["Quarter"].astype(str).str.strip().str[1].astype(int)
    df["quarter"] = df["Date"].astype(int).astype(str) + " Q" + df["q"].astype(str)
    df["t"] = pd.PeriodIndex(
        df["Date"].astype(int).astype(str) + "Q" + df["q"].astype(str), freq="Q"
    ).to_timestamp()
    df = df.sort_values("t")

    # Chart 1: plug-in stock by country.
    fig1 = go.Figure()
    for geog in COUNTRY_ORDER:
        gdf = df[df["Geography"] == geog]
        colour = COUNTRY_COLOURS[geog]
        fig1.add_trace(go.Scatter(x=gdf["t"], y=gdf["Total"], mode="lines",
                                  name=f"{geog}: all plug-ins",
                                  line=dict(color=colour, width=2)))
        fig1.add_trace(go.Scatter(x=gdf["t"], y=gdf["Battery electric"], mode="lines",
                                  name=f"{geog}: battery electric",
                                  line=dict(color=colour, width=2, dash="dot"),
                                  visible="legendonly"))
    _trends_layout(fig1, "Licensed plug-in vehicles by country (DfT VEH0141)",
                   "Licensed plug-in vehicles")

    # Chart 2: plug-in share of the total licensed stock, by country.
    share_rows = []
    totals = national_total_stock_df()
    if not totals.empty:
        share = df.merge(totals, on=["Geography", "quarter"], how="inner")
        share["share"] = np.where(share["total_stock"] > 0,
                                  share["Total"] / share["total_stock"] * 100, np.nan)
        share = share.dropna(subset=["share"]).sort_values("t")
        fig2 = go.Figure()
        for geog in COUNTRY_ORDER:
            gdf = share[share["Geography"] == geog]
            fig2.add_trace(go.Scatter(x=gdf["t"], y=gdf["share"], mode="lines",
                                      name=geog,
                                      line=dict(color=COUNTRY_COLOURS[geog], width=2)))
        _trends_layout(fig2, "Plug-in share of the total licensed stock (VEH0141 / VEH0105)",
                       "Plug-in share (%)")
        latest_share = share[share["t"] == share["t"].max()]
        share_rows = {r["Geography"]: r["share"] for _, r in latest_share.iterrows()}
    else:
        fig2 = None

    # Chart 3: highest and lowest districts by BEV%, latest quarter.
    fig3 = None
    try:
        bev, _ = prepare_comparison_data("Battery Electric (%)", geo_level="LAD", keepership=keepership)
    except Exception:
        bev = pd.DataFrame()
    if not bev.empty:
        from plotly.subplots import make_subplots
        srt = bev.sort_values("value", ascending=False)
        top = srt.head(10).iloc[::-1]
        bottom = srt.tail(10)
        fig3 = make_subplots(rows=1, cols=2, horizontal_spacing=0.16,
                             subplot_titles=("Highest 10 districts", "Lowest 10 districts"))
        fig3.add_trace(go.Bar(x=top["value"], y=top["area_name"], orientation="h",
                              marker_color="#2a78d6", text=[f"{v:.1f}%" for v in top["value"]],
                              textposition="outside", cliponaxis=False, showlegend=False,
                              hovertemplate="%{y}: %{x:.2f}%<extra></extra>"), 1, 1)
        fig3.add_trace(go.Bar(x=bottom["value"], y=bottom["area_name"], orientation="h",
                              marker_color="#2a78d6", text=[f"{v:.1f}%" for v in bottom["value"]],
                              textposition="outside", cliponaxis=False, showlegend=False,
                              hovertemplate="%{y}: %{x:.2f}%<extra></extra>"), 1, 2)
        fig3.update_yaxes(autorange="reversed", col=2)
        _trends_layout(fig3, "Battery-electric share of licensed vehicles by district, latest quarter",
                       None, show_legend=False)
        fig3.update_layout(bargap=0.35, margin=dict(l=10, r=40, t=70, b=40))

    figs = [f for f in [fig1, fig2, fig3] if f is not None]
    sections = []
    for i, f in enumerate(figs):
        sections.append(f.to_html(full_html=False, include_plotlyjs=("cdn" if i == 0 else False),
                                  default_height="470px"))
    caps = (
        "<p class='cap'>Sources: DfT VEH0141 (plug-in stock by country), DfT VEH0105 "
        "(total licensed stock, denominator of the share chart), DfT VEH0132 and VEH0105 "
        "(district BEV%). District extremes reflect company and lease registrations; see the "
        "methods box below the table.</p>"
    )
    page = (
        "<!DOCTYPE html><html><head><meta charset='utf-8'><style>"
        "body{font-family:system-ui,'Segoe UI',Arial,sans-serif;margin:0;padding:16px 20px;"
        "background:#ffffff;color:#0b0c0c;}"
        "h2{color:#003D7A;font-size:19px;margin:0 0 10px 0;}"
        ".cap{font-size:12.5px;color:#505a5f;line-height:1.5;margin:2px 0 14px 0;}"
        "</style></head><body>"
        "<h2>National and district EV uptake trends</h2>"
        + "".join(sections) + caps +
        "</body></html>"
    )

    latest = df[df["t"] == df["t"].max()]
    rows = []
    for _, r in latest.iterrows():
        row = {
            "Geography": r["Geography"],
            "Quarter": f"{int(r['Date'])} {r['Quarter']}",
            "Battery electric": r["Battery electric"],
            "All plug-ins": r["Total"],
        }
        if share_rows and r["Geography"] in share_rows:
            row["Plug-in share of stock (%)"] = round(share_rows[r["Geography"]], 2)
        rows.append(row)

    note = ("Selected view: Output → Trends. Plug-in stock and share of the total licensed "
            "stock by UK country, plus the highest and lowest districts by BEV%. "
            "Battery-electric series are toggled via the legend of the first chart.")
    return page, rows, note


# ============================================================
# Inequality-gap rendering
# ============================================================

_INEQUALITY_EXPLAINER = """
<details class="explainer">
  <summary>How to read these measures</summary>
  <p class="note">
    Every measure below is computed over <b>BEV%</b> — the share of vehicles in
    each area that are battery electric — and compares the observed spread to a
    hypothetically equal distribution. Higher values always mean more unequal.
  </p>

  <div class="mcard">
    <div class="mtitle">Hoover index (H) — the "Robin Hood" index</div>
    <div class="note">
      The proportion of all BEVs that would have to be moved from richer areas to
      poorer ones to make every area equal. Ranges <b>0 (perfect equality) to 0.5</b>.
      Intuitive as a "how much to redistribute" figure.
      <br><i>Example:</i> H = 0.30 means 30% of BEVs would need to be reallocated
      across areas to equalise BEV%.
    </div>
  </div>

  <div class="mcard">
    <div class="mtitle">Gini index (G)</div>
    <div class="note">
      The average difference in BEV% between every pair of areas, standardised to
      <b>0 (all areas equal) to 1 (one area has everything)</b>. Most sensitive to
      the middle of the distribution rather than the extremes.
      <br><i>Example:</i> G = 0.45 is a moderately unequal spread; G = 0.15 means
      areas are fairly similar to one another.
    </div>
  </div>

  <div class="mcard">
    <div class="mtitle">GE(0) — mean log deviation (Theil's L)</div>
    <div class="note">
      A generalised-entropy measure that is <b>most sensitive to the poorest areas</b>
      (those with very low BEV%). Ranges <b>0 (equal) upward with no fixed ceiling</b>.
      Good for spotting a long tail of left-behind areas.
      <br><i>Example:</i> a cluster of areas near 0% BEV pushes GE(0) up sharply,
      even if the top of the distribution looks fine.
    </div>
  </div>

  <div class="mcard">
    <div class="mtitle">GE(1) — Theil's T</div>
    <div class="note">
      Weights each area by its own BEV%, so it is <b>evenly sensitive across the
      distribution</b>. Ranges <b>0 (equal) upward</b>. Its key strength is being
      additively decomposable — total inequality splits cleanly into
      <i>between-group</i> and <i>within-group</i> parts (see the decomposition table).
      <br><i>Example:</i> if "between deprivation deciles" is large relative to
      "within", the inequality is driven by deprivation rather than local variation.
    </div>
  </div>

  <div class="mcard">
    <div class="mtitle">GE(2) — half the squared coefficient of variation</div>
    <div class="note">
      <b>Most sensitive to the richest areas</b> (those with unusually high BEV%).
      Ranges <b>0 (equal) upward</b>. Closely related to statistical variance.
      <br><i>Example:</i> a few affluent areas with very high BEV% inflate GE(2)
      more than they inflate GE(0) or GE(1).
    </div>
  </div>

  <p class="note">
    <b>Reading them together:</b> if GE(2) is high but GE(0) is low, inequality is
    concentrated among high-uptake areas; if GE(0) is high but GE(2) is low, the
    concern is a tail of very low-uptake areas. Hoover and Gini give the headline
    magnitude; the GE family tells you <i>where</i> in the distribution it sits.
  </p>
</details>
"""


def _inequality_message_page(title: str, body_html: str) -> str:
    """A standalone HTML page shown inside the map iframe for the
    Charging Inequality Gap selection (which has no choropleth)."""
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
  body {{ font-family: Arial, sans-serif; color:#0b0c0c; margin:0; padding:24px; background:#ffffff; }}
  h2 {{ color:#003D7A; margin:0 0 4px 0; }}
  .sub {{ color:#505a5f; font-size:14px; margin-bottom:18px; }}
  table {{ border-collapse: collapse; width:100%; margin-bottom:22px; }}
  th, td {{ text-align:left; padding:8px 10px; border-bottom:1px solid #d6d6d6; font-size:14px; }}
  th {{ background:#E8F4FD; color:#003D7A; }}
  td.num {{ font-variant-numeric: tabular-nums; font-weight:700; }}
  .note {{ font-size:13px; color:#505a5f; line-height:1.4; }}
  .card {{ border:1px solid #B6D7F2; border-radius:8px; padding:16px 18px; margin-bottom:18px; background:#F8FBFF; }}
  details.explainer {{ border:1px solid #B6D7F2; border-radius:8px; padding:6px 18px; margin-bottom:18px; background:#ffffff; }}
  details.explainer > summary {{ cursor:pointer; font-weight:700; color:#003D7A; font-size:15px; padding:8px 0; }}
  .mcard {{ border-left:3px solid #B6D7F2; padding:4px 0 4px 12px; margin:12px 0; }}
  .mtitle {{ font-weight:700; color:#0b0c0c; font-size:14px; margin-bottom:3px; }}
</style></head>
<body>
  <h2>{title}</h2>
  {body_html}
</body></html>"""


def _render_inequality_gap(geo_level: str, keepership: str = "Private"):
    """Build the inequality-gap iframe HTML, table rows and note.

    Returns (iframe_html, table_rows, note). The choropleth is intentionally
    replaced by a distribution-level summary because these measures collapse
    the whole distribution into scalar statistics.
    """
    try:
        summary, geo, extras = prepare_inequality_gap(geo_level=geo_level, keepership=keepership)
    except Exception as e:
        page = _inequality_message_page(
            "Charging Inequality Gap",
            f'<div class="note">Could not compute inequality measures: {e}</div>',
        )
        return page, [], f"Charging Inequality Gap could not be computed: {e}"

    if summary.empty:
        diag = _diagnose_bev_pipeline(geo_level)
        page = _inequality_message_page(
            "Charging Inequality Gap",
            "<div class='card'><div class='note'>The BEV% distribution could not be built "
            "from the currently wired vehicle sources, so inequality measures are "
            "unavailable. This requires the All-vehicle and Battery Electric datasets to load "
            "successfully.</div></div>"
            f"<div class='card'><div class='mtitle'>Diagnostic</div>"
            f"<div class='note'>{diag}</div></div>",
        )
        note = ("Selected dataset: Equity → Charging Inequality Gap. "
                "BEV% distribution unavailable from current sources.")
        return page, [], note

    measures = extras.get("measures", {})
    n_areas = extras.get("n_areas", 0)
    n_dropped = extras.get("n_dropped", 0)
    decomposition = extras.get("decomposition", [])
    geo = extras.get("geo", geo_level)

    measure_rows = "".join(
        f"<tr><td>{r['measure']}</td>"
        f"<td class='num'>{r['value']}</td>"
        f"<td class='note'>{r['range']}</td></tr>"
        for _, r in summary.iterrows()
    )
    keep_word = str(keepership).lower()
    scope_phrase = (f"{keep_word}-keepership battery-electric vehicles as a share of all "
                    f"{keep_word}-keepership vehicles" if keep_word != "total" else
                    "battery-electric vehicles as a share of all vehicles, all keeperships")
    manuscript_note = ("" if keep_word == "private" else
                       " The manuscript's analysis uses private keepership; select Private in "
                       "the sidebar's keepership filter to reproduce its numbers.")
    measures_table = (
        "<div class='card'>"
        "<table><thead><tr><th>Measure</th><th>Value</th><th>Range</th></tr></thead>"
        f"<tbody>{measure_rows}</tbody></table>"
        f"<div class='note'>Computed over BEV% ({scope_phrase}) across "
        f"{n_areas} {geo} areas, latest quarter.{manuscript_note}"
        + (f" {n_dropped} area(s) with zero BEV% are included in Hoover, Gini, GE(1) and GE(2) "
           f"but necessarily excluded from GE(0)." if n_dropped else "")
        + "</div></div>"
    )

    # Figure-4-style trend: the five indices over neighbourhood BEV%,
    # fourth quarters 2011-2025.
    trend = extras.get("trend")
    trend_block = ""
    if trend is not None and not trend.empty:
        colours = {"Hoover": "#2a78d6", "Gini": "#eb6834", "GE(0)": "#1baf7a",
                   "GE(1)": "#eda100", "GE(2)": "#e87ba4"}
        fig = go.Figure()
        for key, colour in colours.items():
            fig.add_trace(go.Scatter(x=trend["quarter"], y=trend[key], mode="lines+markers",
                                     name=key, line=dict(color=colour, width=2),
                                     marker=dict(size=5)))
        fig.update_layout(
            template="plotly_white",
            font=dict(family="Arial, sans-serif", size=13),
            hovermode="x unified",
            yaxis_title="Index value",
            xaxis_title=None,
            legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
            margin=dict(l=55, r=15, t=10, b=80),
        )
        chart = fig.to_html(full_html=False, include_plotlyjs="cdn", default_height="420px")
        trend_block = (
            "<div class='card'>"
            "<h2 style='font-size:17px;'>Neighbourhood BEV% inequality, 2011&ndash;2025</h2>"
            f"<div class='sub'>Fourth quarter of each year; {keep_word} keepership; suppressed "
            "counts imputed as in the manuscript (2.5, or 2.0 for areas never above 5, where "
            "the area's vehicle count exceeds 50)."
            + ("" if keep_word == "private" else
               " Private keepership reproduces the manuscript's Figure 4.") + "</div>"
            f"{chart}"
            "<div class='note'>All five indices fall substantially as BEV ownership "
            "proliferates. GE(2), most sensitive to high-ownership areas, falls fastest "
            "between 2012 and 2021; the GE curves converge after 2021 as zero-BEV "
            "neighbourhoods disappear.</div></div>"
        )

    if decomposition:
        decomp_rows = "".join(
            f"<tr><td>{d['Grouping']}</td><td>{d['Index']}</td>"
            f"<td class='num'>{d['Total']}</td>"
            f"<td class='num'>{d['Between']}</td>"
            f"<td class='num'>{d['Within']}</td>"
            f"<td>{d['Groups']}</td></tr>"
            for d in decomposition
        )
        decomp_table = (
            "<div class='card'>"
            "<h2 style='font-size:17px;'>GE decomposition</h2>"
            "<div class='sub'>Additively decomposable inequality: total = between + within.</div>"
            "<table><thead><tr><th>Grouping</th><th>Index</th><th>Total</th>"
            "<th>Between</th><th>Within</th><th>Groups</th></tr></thead>"
            f"<tbody>{decomp_rows}</tbody></table></div>"
        )
    else:
        decomp_table = (
            "<div class='card'><div class='note'>GE decomposition by income-deprivation "
            "band is unavailable for this geography level.</div></div>"
        )

    page = _inequality_message_page(
        "Charging Inequality Gap",
        f"<div class='sub'>Distribution-level inequality of BEV% "
        f"(Hoover, Gini, GE(0), GE(1), GE(2)).</div>{measures_table}{trend_block}{decomp_table}{_INEQUALITY_EXPLAINER}",
    )

    table_rows = summary.rename(
        columns={"measure": "Measure", "value": "Value", "range": "Range"}
    ).to_dict("records")

    note = (
        f"Selected dataset: Equity → Charging Inequality Gap. Geography: {geo}. "
        f"Areas: {n_areas}. "
        f"Hoover={measures.get('Hoover', float('nan')):.4f}, "
        f"Gini={measures.get('Gini', float('nan')):.4f}, "
        f"GE(0)={measures.get('GE(0)', float('nan')):.4f}, "
        f"GE(1)={measures.get('GE(1)', float('nan')):.4f}, "
        f"GE(2)={measures.get('GE(2)', float('nan')):.4f}."
    )
    return page, table_rows, note


# ============================================================
# Map orchestration
# ============================================================

def build_map(group: Optional[str], selection: Optional[str], area_filter: Optional[str] = None, geo_level: str = "LAD", keepership: str = "Total"):
    def make_base_map(level: str):
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

    # Charging Inequality Gap is a distribution-level summary, not a map.
    if group == "Equity" and selection == "charging_inequality_gap":
        return _render_inequality_gap(geo_level, keepership=keepership)

    # Trends is a national time-series chart, not a map.
    if group == "Output" and selection == "trends":
        return _render_trends(keepership)

    if group == "Output":
        m = make_base_map(geo_level)
        add_base_layers(m)
        add_css(m)
        folium.Marker(
            [52.7, -2.8],
            tooltip="Select a Vehicles, Equity or Comparisons dataset to display.",
        ).add_to(m)
        folium.LayerControl(collapsed=False).add_to(m)
        note = (f"Output → {selection} is a view option. "
                "Select a dataset from another group to populate the map.")
        return m.get_root().render(), [], note

    try:
        df, geo_level = prepare_dataset(group, selection, geo_level=geo_level, area_filter=area_filter, keepership=keepership)
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

    if df.empty:
        folium.Marker(
            [52.7, -2.8],
            tooltip=f"{group} → {SELECTION_LABELS.get(selection, selection)} returned no rows for this geography level.",
        ).add_to(m)
        folium.LayerControl(collapsed=False).add_to(m)
        note = f"Selected dataset: {group} → {SELECTION_LABELS.get(selection, selection)}. No rows available at {geo_level} level."
        return m.get_root().render(), [], note

    capped = bool(df.attrs.get("capped"))
    add_choropleth(m, df, geo_level, group, selection)
    folium.LayerControl(collapsed=False).add_to(m)

    note = (f"Selected dataset: {group} → {SELECTION_LABELS.get(selection, selection)}. Geography: {geo_level}. "
            f"Records: {len(df)}. Coverage: England and Wales where matching "
            f"geography codes are present in the selected dataset.")
    if capped:
        note += (f" Showing all Welsh LSOAs plus English LSOAs up to a cap of "
                 f"{LSOA_RENDER_CAP:,} areas for map responsiveness; use the area "
                 f"filter to reach any specific area.")

    if group == "Vehicles" and selection == "All" and geo_level == "LAD" and len(df) < 250:
        note += " Warning: fewer LADs than expected; check that the Veh0105 file contains all England and Wales LAD rows."

    return m.get_root().render(), df.to_dict("records"), note


# ============================================================
# UI
# ============================================================

# Kept markup-agnostic (no flex dependence) so they render identically
# across dcc versions: a fixed 16px control, an 8px gap, aligned baselines.
RADIO_LABEL_STYLE = {
    "display": "block",
    "marginBottom": "9px",
    "fontSize": FS_BODY,
    "lineHeight": "1.5",
    "cursor": "pointer",
}
RADIO_INPUT_STYLE = {
    "width": "16px",
    "height": "16px",
    "marginRight": "8px",
    "verticalAlign": "-3px",
    "accentColor": BRAND_BLUE,
    "cursor": "pointer",
}


LINEAGE_ICON_STYLE = {
    "marginLeft": "7px",
    "border": f"1px solid {BRAND_BLUE}",
    "borderRadius": "50%",
    "width": "17px",
    "height": "17px",
    "lineHeight": "15px",
    "padding": "0",
    "fontSize": "11px",
    "fontWeight": "700",
    "fontStyle": "italic",
    "fontFamily": "Georgia, serif",
    "color": BRAND_BLUE,
    "background": "transparent",
    "cursor": "pointer",
    "verticalAlign": "1px",
}


def dataset_selector(group):
    # Each option label carries an "i" button opening the data-lineage
    # modal. A button inside a <label> does not activate the radio, so
    # viewing provenance never changes the selection.
    options = [
        {
            "label": html.Span([
                opt["label"],
                html.Button(
                    "i",
                    id={"type": "lineage-btn", "sel": opt["value"]},
                    n_clicks=0,
                    title=f"Data lineage for {opt['label']}",
                    style=LINEAGE_ICON_STYLE,
                ),
            ]),
            "value": opt["value"],
        }
        for opt in layer_options[group]
    ]
    return dcc.RadioItems(
        id=f"{group.lower()}-selection",
        options=options,
        value=None,
        labelStyle=RADIO_LABEL_STYLE,
        inputStyle=RADIO_INPUT_STYLE,
    )


def accordion_item(group):
    children = [
        html.Div(
            "Select one dataset",
            style={"fontWeight": "600", "fontSize": FS_SMALL, "color": "#505a5f", "marginBottom": "8px"},
        ),
        dataset_selector(group),
    ]
    if group != "Output":
        children += [
            html.Div(
                f"Filter {group}",
                style={"fontWeight": "600", "fontSize": FS_SMALL, "marginTop": "14px", "marginBottom": "6px"},
            ),
            dcc.Dropdown(
                id=f"{group.lower()}-filter",
                options=[],
                placeholder=f"Select {group} area",
                clearable=True,
                style={"fontSize": FS_BODY},
            ),
        ]
    return dbc.AccordionItem(children, title=group)


sidebar = html.Div(
    [
        html.Img(
            src="/assets/cleets_logo.png",
            style={"width": "100%", "maxWidth": "280px", "display": "block", "margin": "0 auto"},
        ),

        html.Div(
            [
                html.H4(
                    "Electric Vehicle Equity Mapping Dashboard",
                    style={
                        "fontWeight": "bold",
                        "textAlign": "center",
                        "fontSize": FS_TITLE,
                        "marginBottom": "8px",
                        "color": BRAND_BLUE,
                    },
                ),
                html.P(
                    "Use this dashboard to explore electric vehicle uptake, equity indicators, and spatial patterns across England and Wales.",
                    style={"textAlign": "left", "fontSize": FS_BODY, "marginBottom": "0px", "color": "#333333"},
                ),
            ],
            style={
                "backgroundColor": "#E8F4FD",
                "border": "1px solid #B6D7F2",
                "borderRadius": "8px",
                "padding": "12px",
                "marginBottom": "16px",
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
            label="LSOA view (small areas)",
            value=False,
            style={"marginBottom": "10px", "fontSize": FS_BODY},
        ),

        html.Div(
            [
                html.Div(
                    "Keepership",
                    style={"fontWeight": "600", "fontSize": FS_SMALL, "color": "#505a5f",
                           "marginBottom": "4px"},
                ),
                dcc.RadioItems(
                    id="keepership-selector",
                    options=[{"label": k, "value": k} for k in KEEPERSHIP_OPTIONS],
                    value="Total",
                    labelStyle={"display": "inline-block", "marginRight": "16px",
                                "fontSize": FS_BODY, "cursor": "pointer"},
                    inputStyle=RADIO_INPUT_STYLE,
                ),
            ],
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
                "fontSize": FS_BODY,
                "marginBottom": "10px",
            },
        ),

        dbc.Accordion(
            [accordion_item(g) for g in groups],
            always_open=True,
        ),

        html.Div(
            [
                html.A(
                    "CLEETS Global Center",
                    href="https://cleets-global-center.org/",
                    target="_blank",
                    style={"color": "#FFFFFF", "fontWeight": "bold", "display": "block",
                           "marginBottom": "6px", "textDecoration": "none"},
                ),
                # The CLEETS Global Center site is hosted by the University of
                # Illinois System, whose legal pages cover it; its own footer
                # links to the System privacy statement.
                html.A(
                    "Accessibility",
                    href="https://www.vpaa.uillinois.edu/resources/accessibility",
                    target="_blank",
                    style={"color": "#FFFFFF", "display": "block", "marginBottom": "4px"},
                ),
                html.A(
                    "Terms of Use",
                    href="https://www.vpaa.uillinois.edu/resources/terms_of_use",
                    target="_blank",
                    style={"color": "#FFFFFF", "display": "block", "marginBottom": "4px"},
                ),
                html.A(
                    "Privacy Statement",
                    href="https://www.vpaa.uillinois.edu/resources/web_privacy",
                    target="_blank",
                    style={"color": "#FFFFFF", "display": "block", "marginBottom": "4px"},
                ),
                # Data licence: the DfT / ONS / StatsWales tables shown here are
                # published under the Open Government Licence v3.0.
                html.A(
                    "License (data: OGL v3.0)",
                    href="https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
                    target="_blank",
                    style={"color": "#FFFFFF", "display": "block"},
                ),
            ],
            style={
                "backgroundColor": "#000000",
                "color": "#FFFFFF",
                "padding": "12px",
                "borderRadius": "6px",
                "marginTop": "14px",
                "lineHeight": "1.4",
                "fontSize": FS_SMALL,
            },
        ),

        dcc.Store(id="current-table-data", data=[]),

        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle(id="lineage-title", style={"fontSize": "17px", "color": BRAND_BLUE})),
                dbc.ModalBody(id="lineage-body", style={"fontFamily": FONT_FAMILY}),
            ],
            id="lineage-modal",
            size="lg",
            scrollable=True,
            is_open=False,
        ),
    ],
    style={
        "width": "380px",
        "flex": "0 0 380px",
        "height": "100vh",
        "overflowY": "auto",
        "padding": "16px",
        "fontFamily": FONT_FAMILY,
        "fontSize": FS_BODY,
    },
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
                        style={"width": "100%", "height": "72vh", "border": "0"},
                    ),

                    html.Div(
                        [
                            html.H5("Dataset Table", style={"fontSize": "16px", "fontWeight": "600"}),
                            html.Div(
                                id="map-level-note",
                                style={"fontSize": FS_NOTE, "color": "#555", "marginBottom": "6px"},
                            ),
                            html.Details(
                                [
                                    html.Summary(
                                        "How these data were analysed",
                                        style={
                                            "cursor": "pointer",
                                            "fontWeight": "600",
                                            "fontSize": FS_BODY,
                                            "color": BRAND_BLUE,
                                            "padding": "8px 12px",
                                        },
                                    ),
                                    html.Div(
                                        id="method-box",
                                        style={
                                            "padding": "0 12px 10px 12px",
                                            "fontSize": FS_SMALL,
                                            "color": "#333333",
                                            "lineHeight": "1.5",
                                        },
                                    ),
                                ],
                                style={
                                    "backgroundColor": "#F8FBFF",
                                    "border": "1px solid #B6D7F2",
                                    "borderRadius": "8px",
                                    "marginBottom": "10px",
                                },
                            ),
                            dash_table.DataTable(
                                id="data-table",
                                columns=[],
                                data=[],
                                page_size=10,
                                filter_action="native",
                                sort_action="native",
                                style_table={"height": "24vh", "overflowY": "auto", "overflowX": "auto"},
                                style_cell={
                                    "fontSize": FS_BODY,
                                    "fontFamily": FONT_FAMILY,
                                    "textAlign": "left",
                                    "padding": "6px",
                                    "minWidth": "120px",
                                    "maxWidth": "260px",
                                    "whiteSpace": "normal",
                                },
                                style_header={"fontWeight": "bold", "backgroundColor": "#E8F4FD"},
                            ),
                        ],
                        style={"padding": "10px", "fontFamily": FONT_FAMILY},
                    ),
                ],
                style={"width": "100%"},
            ),
        ),
    ],
    style={"display": "flex", "fontFamily": FONT_FAMILY},
)


# ============================================================
# Callbacks
# ============================================================

def active_selection(output_value, vehicles_value, equity_value, comparisons_value):
    if output_value:
        return "Output", output_value
    if vehicles_value:
        return "Vehicles", vehicles_value
    if equity_value:
        return "Equity", equity_value
    if comparisons_value:
        return "Comparisons", comparisons_value
    return None, None


@callback(
    Output("output-selection", "value"),
    Output("vehicles-selection", "value"),
    Output("equity-selection", "value"),
    Output("comparisons-selection", "value"),
    Input("output-selection", "value"),
    Input("vehicles-selection", "value"),
    Input("equity-selection", "value"),
    Input("comparisons-selection", "value"),
    prevent_initial_call=True,
)
def enforce_single_dataset(output_value, vehicles_value, equity_value, comparisons_value):
    trigger = ctx.triggered_id

    # Leave the radio the user just touched untouched (no_update) and clear
    # only the others. Writing no_update to the triggered control prevents the
    # callback from re-firing on its own output, which is what caused flicker.
    ids = [
        "output-selection",
        "vehicles-selection",
        "equity-selection",
        "comparisons-selection",
    ]

    if trigger not in ids:
        return (no_update,) * 4

    current = {
        "output-selection": output_value,
        "vehicles-selection": vehicles_value,
        "equity-selection": equity_value,
        "comparisons-selection": comparisons_value,
    }

    result = []
    for cid in ids:
        if cid == trigger:
            result.append(no_update)
        elif current[cid] is not None:
            result.append(None)
        else:
            result.append(no_update)

    return tuple(result)


@callback(
    Output("map-frame", "srcDoc"),
    Output("data-table", "data"),
    Output("data-table", "columns"),
    Output("vehicles-filter", "options"),
    Output("equity-filter", "options"),
    Output("comparisons-filter", "options"),
    Output("current-table-data", "data"),
    Output("map-level-note", "children"),
    Output("method-box", "children"),
    Output("selected-dataset-display", "children"),
    Input("output-selection", "value"),
    Input("vehicles-selection", "value"),
    Input("equity-selection", "value"),
    Input("comparisons-selection", "value"),
    Input("vehicles-filter", "value"),
    Input("equity-filter", "value"),
    Input("comparisons-filter", "value"),
    Input("geo-level-switch", "value"),
    Input("keepership-selector", "value"),
)
def update_map(
    output_value,
    vehicles_value,
    equity_value,
    comparisons_value,
    vehicles_filter,
    equity_filter,
    comparisons_filter,
    geo_switch,
    keepership,
):
    keepership = keepership or "Total"
    group, selection = active_selection(output_value, vehicles_value, equity_value, comparisons_value)

    area_filter = {
        "Vehicles": vehicles_filter,
        "Equity": equity_filter,
        "Comparisons": comparisons_filter,
    }.get(group)

    requested_geo_level = "LSOA" if geo_switch else "LAD"

    trigger = ctx.triggered_id
    filter_triggered = isinstance(trigger, str) and trigger.endswith("-filter")

    map_html, rows, note = build_map(group, selection, area_filter,
                                     geo_level=requested_geo_level, keepership=keepership)

    columns = [{"name": c, "id": c} for c in rows[0].keys()] if rows else []

    # Dropdown options come from the UNFILTERED dataset (data preparation
    # only, no boundary fetch or map render), so selecting an area never
    # collapses the dropdown to a single option.
    if filter_triggered:
        vehicles_options = no_update
        equity_options = no_update
        comparisons_options = no_update
    else:
        area_options = []
        if group in {"Vehicles", "Equity", "Comparisons"}:
            try:
                full_df, _ = prepare_dataset(group, selection, geo_level=requested_geo_level,
                                             keepership=keepership)
                if not full_df.empty and "area_name" in full_df.columns:
                    names = sorted(full_df["area_name"].dropna().astype(str).unique())
                    area_options = [{"label": n, "value": n} for n in names]
            except Exception:
                area_options = []
        vehicles_options = area_options if group == "Vehicles" else []
        equity_options = area_options if group == "Equity" else []
        comparisons_options = area_options if group == "Comparisons" else []

    display = (
        html.Div([html.Strong("Selected dataset: "), html.Span(f"{group} → {SELECTION_LABELS.get(selection, selection)}")])
        if group
        else "No dataset selected."
    )

    method_children = [
        html.P(t, style={"marginBottom": "8px"})
        for t in analysis_notes(group, selection, requested_geo_level, keepership)
    ]

    return (
        map_html,
        rows,
        columns,
        vehicles_options,
        equity_options,
        comparisons_options,
        rows,
        note,
        method_children,
        display,
    )


@callback(
    Output("lineage-modal", "is_open"),
    Output("lineage-title", "children"),
    Output("lineage-body", "children"),
    Input({"type": "lineage-btn", "sel": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def open_lineage(clicks):
    trigger = ctx.triggered_id
    if not trigger or not any(c for c in clicks if c):
        return no_update, no_update, no_update
    # Only open for the button that was actually clicked this time.
    if not ctx.triggered or not ctx.triggered[0].get("value"):
        return no_update, no_update, no_update
    sel = trigger["sel"]
    title = f"Data lineage: {SELECTION_LABELS.get(sel, sel)}"
    return True, title, lineage_content(sel)


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
