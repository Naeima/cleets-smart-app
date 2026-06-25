# ============================================================
# CLEETS-SMART Dashboard: ClimateGPT Interface
# ============================================================

from __future__ import annotations

import requests
from dash import html, dcc, Input, Output, State, callback, register_page

register_page(__name__, path="/climategpt")


API_URL = "http://localhost:8000/v1/chat/completions"
MODEL_NAME = "climategpt-70b"
API_KEY = "dummy"  


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

layout = html.Div(
    [
        back_button(),

        html.H1(
            "ClimateGPT Assistant",
            style={"textAlign": "center", "marginBottom": "10px"},
        ),

        html.P(
            "Ask climate-related questions using ClimateGPT-7B model from huggingface.co/eci-io/climategpt-7b.",
            style={"textAlign": "center"},
        ),

        html.Div(
            [
                html.Label("Question:"),

                dcc.Textarea(
                    id="cgpt-question",
                    placeholder="Ask ClimateGPT a question...",
                    style={
                        "width": "100%",
                        "height": "140px",
                        "fontSize": "16px",
                        "padding": "12px",
                        "borderRadius": "8px",
                        "border": "1px solid #ccc",
                    },
                ),

                html.Div(
                    [
                        html.Button(
                            "Ask ClimateGPT",
                            id="cgpt-submit",
                            n_clicks=0,
                            style={
                                "height": "42px",
                                "padding": "0 22px",
                                "fontWeight": "600",
                                "borderRadius": "8px",
                                "border": "1px solid #aaa",
                                "backgroundColor": "#f8f9fa",
                                "cursor": "pointer",
                            },
                        ),

                        html.Button(
                            "Clear",
                            id="cgpt-clear",
                            n_clicks=0,
                            style={
                                "height": "42px",
                                "padding": "0 22px",
                                "fontWeight": "600",
                                "borderRadius": "8px",
                                "border": "1px solid #aaa",
                                "backgroundColor": "#fff",
                                "cursor": "pointer",
                                "marginLeft": "10px",
                            },
                        ),
                    ],
                    style={"marginTop": "12px", "textAlign": "center"},
                ),
            ],
            style={
                "maxWidth": "900px",
                "margin": "20px auto",
                "padding": "20px",
                "border": "1px solid #ddd",
                "borderRadius": "10px",
                "backgroundColor": "#fafafa",
            },
        ),

        dcc.Loading(
            id="cgpt-loading",
            type="circle",
            children=[
                html.Div(
                    id="cgpt-answer",
                    style={
                        "maxWidth": "900px",
                        "minHeight": "260px",
                        "margin": "20px auto",
                        "padding": "20px",
                        "border": "1px solid #ddd",
                        "borderRadius": "10px",
                        "backgroundColor": "white",
                        "whiteSpace": "pre-wrap",
                        "fontSize": "16px",
                        "lineHeight": "1.6",
                    },
                )
            ],
        ),

        html.Div(
            id="cgpt-info",
            style={"textAlign": "center", "marginTop": "10px", "color": "#666"},
        ),
    ]
)


def ask_climategpt(question: str) -> str:
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are ClimateGPT, an expert assistant for climate science, "
                    "sustainability, environmental policy, and climate risk."
                ),
            },
            {
                "role": "user",
                "content": question,
            },
        ],
        "temperature": 0.2,
        "max_tokens": 1024,
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}",
    }

    response = requests.post(API_URL, json=payload, headers=headers, timeout=120)
    response.raise_for_status()

    data = response.json()
    return data["choices"][0]["message"]["content"]


@callback(
    Output("cgpt-answer", "children"),
    Output("cgpt-info", "children"),
    Input("cgpt-submit", "n_clicks"),
    Input("cgpt-clear", "n_clicks"),
    State("cgpt-question", "value"),
    prevent_initial_call=True,
)
def update_climategpt(submit_clicks, clear_clicks, question):
    from dash import ctx

    if ctx.triggered_id == "cgpt-clear":
        return "", "Cleared."

    if not question or not question.strip():
        return "Please enter a question.", ""

    try:
        answer = ask_climategpt(question.strip())
        return answer, f"Model: {MODEL_NAME}. Endpoint: {API_URL}"

    except Exception as e:
        return f"Error contacting ClimateGPT API:\n\n{e}", ""


# if __name__ == "__main__":
#     from dash import Dash

#     app = Dash(__name__)
#     app.layout = layout
#     app.run(debug=True, port=8052)