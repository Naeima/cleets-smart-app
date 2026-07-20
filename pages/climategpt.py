# ============================================================
# CLEETS-SMART Dashboard: ClimateGPT Interface
# Async/polling version to avoid nginx 504 timeout
# ============================================================

from __future__ import annotations

import os
import threading
import uuid
from typing import Any

import requests
from dash import html, dcc, Input, Output, State, callback, register_page, ctx


register_page(__name__, path="/climategpt")


API_URL = os.getenv(
    "CLIMATEGPT_API_URL",
    "http://climategpt-api:8000/v1/chat/completions",
)
MODEL_NAME = os.getenv("CLIMATEGPT_MODEL_NAME", "climategpt-7b")
API_KEY = os.getenv("CLIMATEGPT_API_KEY", "").strip()

MAX_TOKENS = int(os.getenv("CLIMATEGPT_MAX_TOKENS", "320"))
MIN_TOKENS = int(os.getenv("CLIMATEGPT_MIN_TOKENS", "20"))
REQUEST_TIMEOUT = int(os.getenv("CLIMATEGPT_TIMEOUT", "1200"))

ANSWER_PLACEHOLDER = "Answer will appear here..."

# In-memory store. Good for one Dash process. Use Redis if you run multiple workers.
JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()


SYSTEM_PROMPT = """
You are ClimateGPT, an expert assistant in climate science, environmental policy,
renewable energy, biodiversity, sustainability, and environmental management.

Default behaviour:
- Produce clear, well-structured answers.
- Use headings and clear paragraphs.
- Keep responses concise enough for an interactive dashboard.
""".strip()


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
        dcc.Store(id="cgpt-job-id", data=None),
        dcc.Interval(
            id="cgpt-poll",
            interval=3000,
            n_intervals=0,
            disabled=True,
        ),
        back_button(),
        html.H1(
            "ClimateGPT Assistant",
            style={"textAlign": "center", "marginBottom": "10px"},
        ),
        html.P(
            "Ask climate-related questions using the ClimateGPT-7B model.",
            style={"textAlign": "center"},
        ),
        html.Div(
            [
                html.Label("Question:"),
                dcc.Textarea(
                    id="cgpt-question",
                    placeholder="Ask ClimateGPT a question...",
                    value="",
                    style={
                        "width": "100%",
                        "height": "140px",
                        "fontSize": "16px",
                        "padding": "12px",
                        "borderRadius": "8px",
                        "border": "1px solid #ccc",
                        "boxSizing": "border-box",
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
                "boxSizing": "border-box",
            },
        ),
        dcc.Loading(
            id="cgpt-loading",
            type="circle",
            children=[
                dcc.Textarea(
                    id="cgpt-answer",
                    value=ANSWER_PLACEHOLDER,
                    readOnly=True,
                    style={
                        "width": "100%",
                        "maxWidth": "900px",
                        "height": "520px",
                        "margin": "20px auto",
                        "display": "block",
                        "padding": "20px",
                        "border": "1px solid #ddd",
                        "borderRadius": "10px",
                        "backgroundColor": "white",
                        "whiteSpace": "pre-wrap",
                        "fontSize": "16px",
                        "lineHeight": "1.6",
                        "fontFamily": "inherit",
                        "boxSizing": "border-box",
                    },
                )
            ],
        ),
        html.Div(
            id="cgpt-info",
            style={
                "textAlign": "center",
                "marginTop": "10px",
                "color": "#666",
            },
        ),
    ]
)


def _extract_answer(data: dict[str, Any]) -> tuple[str, str | None]:
    if "error" in data:
        error = data.get("error", "Unknown API error")
        details = data.get("details", "")
        return f"API error: {error}\n{details}".strip(), None

    for key in ("response", "answer", "generated_text", "output", "text"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip(), None

    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0]
        finish_reason = first_choice.get("finish_reason")

        message = first_choice.get("message", {})
        if isinstance(message, dict):
            content = message.get("content", "")

            if isinstance(content, list):
                parts: list[str] = []
                for item in content:
                    if isinstance(item, str):
                        parts.append(item)
                    elif isinstance(item, dict):
                        text = item.get("text") or item.get("content") or ""
                        if text:
                            parts.append(str(text))
                content = "\n".join(parts)

            if isinstance(content, str) and content.strip():
                return content.strip(), finish_reason

        text = first_choice.get("text", "")
        if isinstance(text, str) and text.strip():
            return text.strip(), finish_reason

    return f"Could not extract answer. Raw API response:\n{data}", None


def ask_climategpt(question: str) -> str:
    detailed_question = f"""
{question.strip()}

Please write a clear, concise answer. Use headings where useful.
""".strip()

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": detailed_question},
        ],
        "temperature": 0.4,
        "top_p": 0.95,
        "max_tokens": MAX_TOKENS,
        "min_new_tokens": MIN_TOKENS,
    }

    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"

    response = requests.post(
        API_URL,
        json=payload,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    try:
        data = response.json()
    except ValueError:
        return f"API did not return JSON:\n\n{response.text}"

    answer, finish_reason = _extract_answer(data)

    if finish_reason == "length":
        answer += "\n\n[Note: The response stopped because it reached the token limit.]"

    if not answer:
        return f"The API returned an empty answer. Raw response:\n\n{data}"

    return answer


def _run_climategpt_job(job_id: str, question: str) -> None:
    try:
        answer = ask_climategpt(question)
        with JOBS_LOCK:
            JOBS[job_id] = {
                "status": "done",
                "answer": answer,
                "error": None,
            }
        print("JOB DONE:", job_id, "answer_length=", len(answer), flush=True)
    except Exception as exc:
        with JOBS_LOCK:
            JOBS[job_id] = {
                "status": "error",
                "answer": None,
                "error": str(exc),
            }
        print("JOB ERROR:", job_id, str(exc), flush=True)


@callback(
    Output("cgpt-answer", "value"),
    Output("cgpt-info", "children"),
    Output("cgpt-question", "value"),
    Output("cgpt-job-id", "data"),
    Output("cgpt-poll", "disabled"),
    Input("cgpt-submit", "n_clicks"),
    Input("cgpt-clear", "n_clicks"),
    State("cgpt-question", "value"),
    prevent_initial_call=True,
)
def submit_climategpt(submit_clicks, clear_clicks, question):
    triggered = ctx.triggered_id

    if triggered == "cgpt-clear":
        return ANSWER_PLACEHOLDER, "Cleared.", "", None, True

    if triggered != "cgpt-submit":
        return ANSWER_PLACEHOLDER, "", question, None, True

    if not question or not question.strip():
        return "Please enter a question.", "", question, None, True

    job_id = str(uuid.uuid4())
    clean_question = question.strip()

    with JOBS_LOCK:
        JOBS[job_id] = {
            "status": "running",
            "answer": None,
            "error": None,
        }

    worker = threading.Thread(
        target=_run_climategpt_job,
        args=(job_id, clean_question),
        daemon=True,
    )
    worker.start()

    print("JOB STARTED:", job_id, flush=True)

    return (
        "ClimateGPT is generating an answer. This will update automatically...",
        "Generation started. Polling every 3 seconds.",
        question,
        job_id,
        False,
    )


@callback(
    Output("cgpt-answer", "value", allow_duplicate=True),
    Output("cgpt-info", "children", allow_duplicate=True),
    Output("cgpt-poll", "disabled", allow_duplicate=True),
    Input("cgpt-poll", "n_intervals"),
    State("cgpt-job-id", "data"),
    prevent_initial_call=True,
)
def poll_climategpt(n_intervals, job_id):
    if not job_id:
        return ANSWER_PLACEHOLDER, "", True

    with JOBS_LOCK:
        job = JOBS.get(job_id)

    if not job:
        return "No active ClimateGPT job was found.", "", True

    status = job.get("status")

    if status == "running":
        return (
            f"ClimateGPT is still generating an answer...\n\nPolling check: {n_intervals}",
            "Still running...",
            False,
        )

    if status == "error":
        return (
            f"Error generating answer:\n\n{job.get('error')}",
            "Generation failed.",
            True,
        )

    answer = job.get("answer") or "ClimateGPT returned an empty answer."

    with JOBS_LOCK:
        JOBS.pop(job_id, None)

    return answer, "Generation complete.", True


