
# ============================================================
# CLEETS-SMART Home Page (Adjusted)
# ============================================================

from dash import html, register_page

register_page(__name__, path="/")

IMG_STYLE = {
    "width": "85%",
    "height": "auto",
    "display": "block",
    "margin": "0 auto 10px auto",
    "objectFit": "contain"
}

TITLE_STYLE = {
    "fontWeight": "bold",
    "textDecoration": "none",
    "fontSize": "48px"
}

TEXT_STYLE = {
    "fontSize": "36px"
}

CARD_STYLE_BASE = {
    "border": "1px solid #cfe2f3",
    "borderRadius": "12px",
    "padding": "20px",
    "width": "30%",
    "minWidth": "280px",
    "maxWidth": "100%",
    "boxSizing": "border-box",
    "textAlign": "center"
}

layout = html.Div([

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

            html.Div(
                children=[
                    html.Img(src="/assets/weather.png", style=IMG_STYLE),
                    html.A("A) Weather Forecast for South Wales", href="/weather-forecaster", style=TITLE_STYLE),
                    html.P("A weather dashboard built using the Open-Meteo API.", style=TEXT_STYLE)
                ],
                style={**CARD_STYLE_BASE, "backgroundColor": "#e6f7f5"}
            ),

            html.Div(
                children=[
                    html.Img(src="/assets/paul.png", style=IMG_STYLE),
                    html.A("B) Weather Forecast for South Wales", href="/Paul", style=TITLE_STYLE),
                    html.P("Equity map", style=TEXT_STYLE)
                ],
                style={**CARD_STYLE_BASE, "backgroundColor": "#e6f7f5"}
            )

            # html.Div(
            #     children=[
            #         html.Img(src="/assets/temp.png", style=IMG_STYLE),
            #         html.A("B) Climate and Emissions Visualization", href="/heat-uk", style=TITLE_STYLE),
            #         html.P("An interactive map visualizing UK greenhouse gas emissions and projected temperature trends.", style=TEXT_STYLE)
            #     ],
            #     style={**CARD_STYLE_BASE, "border": "1px solid #f6c28b", "backgroundColor": "#fff2e6"}
            # ),

            # html.Div(
            #     children=[
            #         html.Img(src="/assets/journey.png", style=IMG_STYLE),
            #         html.A("C) EV Journey Planner During Floods", href="/ev-travel-planning", style=TITLE_STYLE),
            #         html.P("Plan EV routes during flooding events.", style=TEXT_STYLE)
            #     ],
            #     style={**CARD_STYLE_BASE, "backgroundColor": "#eaf3fb"}
            # ),

            # html.Div(
            #     children=[
            #         html.Img(src="/assets/thrustOne.png", style=IMG_STYLE),
            #         html.A("D) Clean and Equitable Transportation-UK", href="/thrust_one", style=TITLE_STYLE),
            #         html.P("Explores EV uptake and socioeconomic deprivation.", style=TEXT_STYLE)
            #     ],
            #     style={**CARD_STYLE_BASE, "backgroundColor": "#eaf3fb"}
            # ),

            # html.Div(
            #     children=[
            #         html.Img(src="/assets/westmidlands.png", style=IMG_STYLE),
            #         html.A("E) West Midlands Flood & EV Monitoring", href="/westmidlands", style=TITLE_STYLE),
            #         html.P("Combines EV chargers locations with flood-risk data.", style=TEXT_STYLE)
            #     ],
            #     style={**CARD_STYLE_BASE, "backgroundColor": "#fbf5ea"}
            # ),

            # html.Div(
            #     children=[
            #         html.Img(src="/assets/SCOUT.png", style=IMG_STYLE),
            #         html.A("F) SCOUT", href="https://arcade.evl.uic.edu/scout/", target="_blank", style=TITLE_STYLE),
            #         html.P("Scenario-Oriented Urban Toolkit for Decision Support.", style=TEXT_STYLE)
            #     ],
            #     style={**CARD_STYLE_BASE, "backgroundColor": "#eafbf8"}
            # ),

            # html.Div(
            #     children=[
            #         html.Img(src="/assets/team.png", style=IMG_STYLE),
            #         html.A("CLEETS-SMART Data Science Group", href="https://cleets-global-center.org/", target="_blank", style=TITLE_STYLE)
            #     ],
            #     style={**CARD_STYLE_BASE, "backgroundColor": "#eafbf8"}
            # )

        ]
    )
])
