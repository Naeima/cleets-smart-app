# ============================================================
# CLEETS-SMART Home Page
# ============================================================

from dash import html, register_page

register_page(__name__, path="/")

layout = html.Div([
    # Header
    html.Div(
        className="header",
        children=[
            html.Img(
                src="/assets/cleets_logo.png",
                style={
                    "height": "180px",
                    "display": "block",
                    "margin": "0 auto 12px auto"
                }
            ),
            html.H1(
                "CLEETS-SMART: Sustainable Mobility And Resilient Transport",
                style={
                    "textAlign": "center",
                    "width": "100%",
                    "margin": "0 auto",
                    "display": "block",
                    "fontSize": "45px"
                }
            )
        ],
        style={"textAlign": "center", "width": "100%"}
    ),

    # Card container
    html.Div(
        className="card-container",
        style={
            "display": "flex",
            "justifyContent": "center",
            "alignItems": "stretch",
            "gap": "25px",
            "flexWrap": "wrap",
            "padding": "20px 40px",
            "width": "100%",
            "boxSizing": "border-box"
        },
        children=[
            # Card A
            html.Div(
                className="card",
                children=[
                    html.Img(
                        src="/assets/weather.png",
                        style={
                            "height": "270px",
                            "width": "auto",
                            "display": "block",
                            "margin": "0 auto 20px auto",
                            "objectFit": "contain"
                        }
                    ),
                    html.A(
                        "A) Weather Forecaster",
                        href="/weather-forecaster",
                        style={"fontWeight": "bold", "textDecoration": "none"}
                    ),
                    html.P("Weather forecasting for Wales using live Open-Meteo APIs.")
                ],
                style={
                    "border": "1px solid #cfe2f3",
                    "borderRadius": "12px",
                    "padding": "20px",
                    "backgroundColor": "#e6f7f5",
                    "fontSize": "32px",
                    "width": "30%",
                    "minWidth": "350px",
                    "textAlign": "center"
                }
            ),

            # Card B
            html.Div(
                className="card",
                children=[
                    html.Img(
                        src="/assets/temp.png",
                        style={
                            "height": "300px",
                            "width": "auto",
                            "display": "block",
                            "margin": "0 auto 20px auto",
                            "objectFit": "contain"
                        }
                    ),
                    html.A(
                        "B) UK Average Temperature Predictions",
                        href="/heat-uk",
                        style={"fontWeight": "bold", "textDecoration": "none"}
                    ),
                    html.P("UKCP18 RCM daily mean temperature data. Source: DAFNI.")
                ],
                style={
                    "border": "1px solid #f6c28b",
                    "borderRadius": "12px",
                    "padding": "20px",
                    "backgroundColor": "#fff2e6",
                    "fontSize": "32px",
                    "width": "30%",
                    "minWidth": "350px",
                    "textAlign": "center"
                }
            ),

            # Card C
            html.Div(
                className="card",
                children=[
                    html.Img(
                        src="/assets/journey.png",
                        style={
                            "height": "300px",
                            "width": "auto",
                            "display": "block",
                            "margin": "0 auto 20px auto",
                            "objectFit": "contain"
                        }
                    ),
                    html.A(
                        "C) EV Travel Planning",
                        href="/ev-travel-planning",
                        style={"fontWeight": "bold", "textDecoration": "none"}
                    ),
                    html.P("EV chargers, flood overlays, and CLEETS-SMART journey simulator.")
                ],
                style={
                    "border": "1px solid #cfe2f3",
                    "borderRadius": "12px",
                    "padding": "20px",
                    "backgroundColor": "#eaf3fb",
                    "fontSize": "32px",
                    "width": "30%",
                    "minWidth": "350px",
                    "textAlign": "center"
                }
            ),
            # Card D
            html.Div(
                className="card",
                children=[
                    html.Img(
                        src="/assets/thrustOne.png",
                        style={
                            "height": "300px",
                            "width": "auto",
                            "display": "block",
                            "margin": "0 auto 20px auto",
                            "objectFit": "contain"
                        }
                    ),
                    html.A(
                        "D) Clean and Equitable Transportation-UK",
                        href="/thrust-one",
                        style={"fontWeight": "bold", "textDecoration": "none"}
                    ),
                    html.P("EV chargers, EV Keeperships, and levels of deprivation. Exploratory analysis of the relationship between electric vehicle uptake and socioeconomic deprivation across Wales, with a focus on South Wales.")
                ],
                style={
                    "border": "1px solid #cfe2f3",
                    "borderRadius": "12px",
                    "padding": "20px",
                    "backgroundColor": "#eaf3fb",
                    "fontSize": "32px",
                    "width": "30%",
                    "minWidth": "350px",
                    "textAlign": "center"
                }
            ),
            # Card E
            html.Div(
                className="card",
                children=[
                    html.Img(
                        src="/assets/westmidlands.png",
                        style={
                            "height": "300px",
                            "width": "auto",
                            "display": "block",
                            "margin": "0 auto 20px auto",
                            "objectFit": "contain"
                        }
                    ),
                    html.A(
                        "E) CLEETS-SMART WestMidlands",
                        href="/westmidlands",
                        style={"fontWeight": "bold", "textDecoration": "none"}
                    ),
                    html.P("EV chargers and flood overlays.")
                ],
                style={
                    "border": "1px solid #cfe2f3",
                    "borderRadius": "12px",
                    "padding": "20px",
                    "backgroundColor": "#eaf3fb",
                    "fontSize": "32px",
                    "width": "30%",
                    "minWidth": "350px",
                    "textAlign": "center"
                }
            )

        ]
    )
])
