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

# Main Flask server behind Dash
server = app.server


@server.route("/climategpt/v1/chat/completions", methods=["POST"])
def climategpt_proxy():
    r = requests.post(
        "http://172.17.0.1:8000/v1/chat/completions",
        json=request.get_json(),
        timeout=600,
    )
    return jsonify(r.json()), r.status_code


# CLEETS Data Portal / CKAN proxy
CKAN_URL = "http://172.17.0.1:5000"  # change if CKAN runs on another port/service


@server.route("/data/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@server.route("/data/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def cleets_data_proxy(path):
    target_url = f"{CKAN_URL}/{path}"

    resp = requests.request(
        method=request.method,
        url=target_url,
        headers={
            k: v for k, v in request.headers
            if k.lower() not in ["host", "content-length"]
        },
        params=request.args,
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False,
        timeout=600,
    )

    excluded_headers = {
        "content-encoding",
        "content-length",
        "transfer-encoding",
        "connection",
    }

    headers = [
        (name, value)
        for name, value in resp.headers.items()
        if name.lower() not in excluded_headers
    ]

    return Response(resp.content, resp.status_code, headers)


import pages.home
import pages.weather
import pages.westmidlands
import pages.heat_uk
import pages.ev_travel_planning
import pages.thrust_one

app.layout = html.Div([
    dcc.Location(id="url"),
    page_container
], style={
    "width": "100%",
    "maxWidth": "100%",
    "overflowX": "hidden",
    "boxSizing": "border-box"
})

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8052, debug=False)