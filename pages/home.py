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
                    "fontSize": "65px"
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
                        "A) Weather Forecast for South Wales",
                        href="/weather-forecaster",
                        style={"fontWeight": "bold", "textDecoration": "none"}
                    ),
                    html.P("A weather dashboard built using the Open-Meteo API.")
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
                        "B)  Climate and Emissions Visualization",
                        href="/heat-uk",
                        style={"fontWeight": "bold", "textDecoration": "none"}
                    ),
                    html.P("An interactive map that visualizes UK greenhouse gas emissions and projected UK average temperature trends from 1980 to 2080.")
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
                        "C) Electric Vehicle (EV) Journey Planner During Floods",
                        href="/ev-travel-planning",
                        style={"fontWeight": "bold", "textDecoration": "none"}
                    ),
                    html.P("An interactive map that helps plan EV routes during flooding events. ")
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
                        "E) West Midlands (UK) Flood and EV Monitoring",
                        href="/westmidlands",
                        style={"fontWeight": "bold", "textDecoration": "none"}
                    ),
                    html.P("A dashboard that combines EV charging-point locations in the West Midlands with flood-risk layers and live flood warnings from the UK Environment Agency.")
                ],
                style={
                    "border": "1px solid #cfe2f3",
                    "borderRadius": "12px",
                    "padding": "20px",
                    "backgroundColor": "#fbf5ea",
                    "fontSize": "32px",
                    "width": "30%",
                    "minWidth": "350px",
                    "textAlign": "center"
                }
            ),
            # Card F
            html.Div(
                className="card",
                children=[
                    html.Img(
                        src="/assets/SCOUT.png",
                        style={
                            "height": "300px",
                            "width": "auto",
                            "display": "block",
                            "margin": "0 auto 20px auto",
                            "objectFit": "contain"
                        }
                    ),
                    html.A(
                        "F) SCOUT",
                        href="https://arcade.evl.uic.edu/scout/",
                        target="_blank",
                        style={"fontWeight": "bold", "textDecoration": "none"}
                    ),
                    html.P("An Open-Access Scenario-Oriented Urban Toolkit for Decision Support. By Kazi Omar and Dr Fabio Miranda")
                ],
                style={
                    "border": "1px solid #cfe2f3",
                    "borderRadius": "12px",
                    "padding": "20px",
                    "backgroundColor": "#eafbf8",
                    "fontSize": "32px",
                    "width": "30%",
                    "minWidth": "350px",
                    "textAlign": "center"
                }
            )

        ]
    )
])
