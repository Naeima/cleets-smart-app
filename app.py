import os

import dash
from dash import html, dcc, page_container
import requests
from flask import request, jsonify, Response


app = dash.Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
    title="CLEETS-SMART Dashboard",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

server = app.server

CLIMATEGPT_API_URL = os.getenv(
    "CLIMATEGPT_API_URL",
    "http://climategpt-api:8000/v1/chat/completions",
)


@server.route("/climategpt/v1/chat/completions", methods=["POST"])
def climategpt_proxy():
    """Proxy dashboard requests to the ClimateGPT API."""
    try:
        payload = request.get_json(silent=True) or {}

        resp = requests.post(
            CLIMATEGPT_API_URL,
            json=payload,
            timeout=int(os.getenv("CLIMATEGPT_TIMEOUT", "1200")),
        )

        content_type = resp.headers.get("content-type", "")

        if "application/json" in content_type.lower():
            return jsonify(resp.json()), resp.status_code

        return Response(
            resp.content,
            status=resp.status_code,
            content_type=content_type or "text/plain",
        )

    except requests.exceptions.RequestException as exc:
        return jsonify(
            {
                "error": "Could not contact ClimateGPT API",
                "target": CLIMATEGPT_API_URL,
                "details": str(exc),
            }
        ), 502


app.layout = html.Div(
    [
        dcc.Location(id="url"),
        page_container,
    ],
    style={
        "width": "100%",
        "maxWidth": "100%",
        "overflowX": "hidden",
        "boxSizing": "border-box",
    },
)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8052, debug=False)
