# ============================================================
# CLEETS-SMART Dashboard: ClimateGPT Interface
# Async polling + feedback + canary questions + LoRA trigger
# ============================================================

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dash import (
    ALL,
    Input,
    Output,
    State,
    callback,
    ctx,
    dcc,
    html,
    no_update,
    register_page,
)

register_page(__name__, path="/climategpt")


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

API_URL = os.getenv(
    "CLIMATEGPT_API_URL",
    "http://climategpt-api:8000/v1/chat/completions",
)
MODEL_NAME = os.getenv("CLIMATEGPT_MODEL_NAME", "climategpt-7b")
API_KEY = os.getenv("CLIMATEGPT_API_KEY", "").strip()

MAX_TOKENS = int(os.getenv("CLIMATEGPT_MAX_TOKENS", "320"))
MIN_TOKENS = int(os.getenv("CLIMATEGPT_MIN_TOKENS", "20"))
REQUEST_TIMEOUT = int(os.getenv("CLIMATEGPT_TIMEOUT", "1200"))

FEEDBACK_DB = Path(
    os.getenv("CLIMATEGPT_FEEDBACK_DB", "/app/data/climategpt_feedback.sqlite3")
)
LORA_TRIGGER_COUNT = int(os.getenv("CLIMATEGPT_LORA_TRIGGER_COUNT", "20"))
LORA_TRAIN_COMMAND = os.getenv("CLIMATEGPT_LORA_TRAIN_COMMAND", "").strip()
LORA_STATE_FILE = Path(
    os.getenv("CLIMATEGPT_LORA_STATE_FILE", "/app/data/lora_state.json")
)

ANSWER_PLACEHOLDER = "Answer will appear here..."

# One-process in-memory job store. Use Redis for multiple Dash workers.
JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()
TRAINING_LOCK = threading.Lock()


SYSTEM_PROMPT = """
You are ClimateGPT, an expert assistant in climate science, environmental policy,
renewable energy, biodiversity, sustainability, and environmental management.

Default behaviour:
- Produce clear, well-structured answers.
- Use headings and clear paragraphs.
- Keep responses concise enough for an interactive dashboard.
- State uncertainty when evidence is incomplete.
- Do not invent sources or citations.
""".strip()


CANARY_QUESTIONS = [
    "What is the difference between climate and weather?",
    "What are climate tipping points?",
    "How does deforestation contribute to climate change?",
    "What is the role of methane in global warming?",
    "Why is adaptation needed alongside mitigation?",
]


# ------------------------------------------------------------
# Persistence
# ------------------------------------------------------------

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_feedback_db() -> None:
    FEEDBACK_DB.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(FEEDBACK_DB) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS interactions (
                interaction_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                model_name TEXT NOT NULL,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                feedback TEXT,
                feedback_at TEXT,
                used_for_training INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS training_runs (
                run_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                approved_examples INTEGER NOT NULL,
                command TEXT,
                details TEXT
            )
            """
        )
        connection.commit()


def save_interaction(question: str, answer: str) -> str:
    interaction_id = str(uuid.uuid4())

    with sqlite3.connect(FEEDBACK_DB) as connection:
        connection.execute(
            """
            INSERT INTO interactions (
                interaction_id, created_at, model_name, question, answer
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (interaction_id, utc_now(), MODEL_NAME, question, answer),
        )
        connection.commit()

    return interaction_id


def save_feedback(interaction_id: str, rating: str) -> None:
    if rating not in {"up", "down"}:
        raise ValueError("Feedback rating must be 'up' or 'down'.")

    with sqlite3.connect(FEEDBACK_DB) as connection:
        connection.execute(
            """
            UPDATE interactions
            SET feedback = ?, feedback_at = ?
            WHERE interaction_id = ?
            """,
            (rating, utc_now(), interaction_id),
        )
        connection.commit()


def count_untrained_positive_examples() -> int:
    with sqlite3.connect(FEEDBACK_DB) as connection:
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM interactions
            WHERE feedback = 'up' AND used_for_training = 0
            """
        ).fetchone()

    return int(row[0] if row else 0)


def export_positive_examples(run_id: str) -> Path:
    export_dir = FEEDBACK_DB.parent / "training_exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    export_path = export_dir / f"approved_examples_{run_id}.jsonl"

    with sqlite3.connect(FEEDBACK_DB) as connection:
        rows = connection.execute(
            """
            SELECT interaction_id, question, answer
            FROM interactions
            WHERE feedback = 'up' AND used_for_training = 0
            ORDER BY created_at
            """
        ).fetchall()

    with export_path.open("w", encoding="utf-8") as handle:
        for interaction_id, question, answer in rows:
            handle.write(
                json.dumps(
                    {
                        "interaction_id": interaction_id,
                        "instruction": question,
                        "response": answer,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    return export_path


def record_training_run(
    run_id: str,
    status: str,
    approved_examples: int,
    command: str,
    details: str,
) -> None:
    with sqlite3.connect(FEEDBACK_DB) as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO training_runs (
                run_id, created_at, status, approved_examples, command, details
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, utc_now(), status, approved_examples, command, details),
        )
        connection.commit()


def mark_examples_used_for_training() -> None:
    with sqlite3.connect(FEEDBACK_DB) as connection:
        connection.execute(
            """
            UPDATE interactions
            SET used_for_training = 1
            WHERE feedback = 'up' AND used_for_training = 0
            """
        )
        connection.commit()


def write_lora_state(state: dict[str, Any]) -> None:
    LORA_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    LORA_STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


init_feedback_db()


# ------------------------------------------------------------
# UI helpers
# ------------------------------------------------------------

def back_button():
    return html.Div(
        [
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


def canary_buttons():
    return html.Div(
        [
            html.Button(
                question,
                id={"type": "cgpt-canary", "index": index},
                n_clicks=0,
                style={
                    "display": "block",
                    "width": "100%",
                    "textAlign": "left",
                    "marginBottom": "8px",
                    "padding": "10px 12px",
                    "border": "1px solid #d6d6d6",
                    "borderRadius": "8px",
                    "backgroundColor": "#fff",
                    "cursor": "pointer",
                },
            )
            for index, question in enumerate(CANARY_QUESTIONS)
        ]
    )


layout = html.Div(
    [
        dcc.Store(id="cgpt-job-id", data=None),
        dcc.Store(id="cgpt-interaction-id", data=None),
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
        html.Details(
            [
                html.Summary(
                    "How to use ClimateGPT",
                    style={"fontWeight": "700", "cursor": "pointer"},
                ),
                html.Ol(
                    [
                        html.Li("Enter a climate-related question or select a suggested question."),
                        html.Li("Select Ask ClimateGPT and keep the page open while the answer is generated."),
                        html.Li("Review the answer critically and verify important claims."),
                        html.Li("Use 👍 for a useful answer and 👎 for an unsuitable answer."),
                        html.Li(
                            "Approved answers may be exported for supervised LoRA fine-tuning. "
                            "Feedback does not immediately change the running model."
                        ),
                    ]
                ),
                html.P(
                    "The canary questions provide a fixed evaluation set for checking model behaviour "
                    "before promoting a newly trained adapter."
                ),
            ],
            open=False,
            style={
                "maxWidth": "900px",
                "margin": "20px auto",
                "padding": "16px 20px",
                "border": "1px solid #d9d9d9",
                "borderRadius": "10px",
                "backgroundColor": "#f8fbff",
            },
        ),
        html.Div(
            [
                html.H3("Suggested canary questions"),
                canary_buttons(),
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
            [
                html.Button(
                    "👍 Helpful",
                    id="cgpt-thumbs-up",
                    n_clicks=0,
                    disabled=True,
                    style={
                        "padding": "10px 18px",
                        "borderRadius": "8px",
                        "border": "1px solid #aaa",
                        "cursor": "pointer",
                    },
                ),
                html.Button(
                    "👎 Not helpful",
                    id="cgpt-thumbs-down",
                    n_clicks=0,
                    disabled=True,
                    style={
                        "padding": "10px 18px",
                        "borderRadius": "8px",
                        "border": "1px solid #aaa",
                        "cursor": "pointer",
                        "marginLeft": "10px",
                    },
                ),
            ],
            style={"textAlign": "center", "marginTop": "10px"},
        ),
        html.Div(
            id="cgpt-feedback-info",
            style={"textAlign": "center", "marginTop": "10px", "color": "#365"},
        ),
        html.Div(
            id="cgpt-info",
            style={
                "textAlign": "center",
                "marginTop": "10px",
                "color": "#666",
            },
        ),
        html.Hr(style={"maxWidth": "900px", "margin": "30px auto"}),
        html.Div(
            [
                html.H3("LoRA fine-tuning status"),
                html.P(
                    id="cgpt-training-count",
                    children=(
                        f"Approved examples awaiting training: "
                        f"{count_untrained_positive_examples()} / {LORA_TRIGGER_COUNT}"
                    ),
                ),
                html.Button(
                    "Check training readiness",
                    id="cgpt-check-training",
                    n_clicks=0,
                    style={
                        "padding": "10px 18px",
                        "borderRadius": "8px",
                        "border": "1px solid #aaa",
                        "cursor": "pointer",
                    },
                ),
                html.Div(
                    id="cgpt-training-info",
                    style={"marginTop": "10px", "whiteSpace": "pre-wrap"},
                ),
                html.P(
                    "Safety note: automatic training only runs when "
                    "CLIMATEGPT_LORA_TRAIN_COMMAND is explicitly configured. "
                    "A newly trained adapter should be evaluated on the fixed canary set "
                    "before deployment.",
                    style={"fontSize": "14px", "color": "#666"},
                ),
            ],
            style={
                "maxWidth": "900px",
                "margin": "20px auto 50px",
                "padding": "20px",
                "border": "1px solid #ddd",
                "borderRadius": "10px",
                "backgroundColor": "#fffdf6",
            },
        ),
    ]
)


# ------------------------------------------------------------
# Model API
# ------------------------------------------------------------

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
        interaction_id = save_interaction(question, answer)

        with JOBS_LOCK:
            JOBS[job_id] = {
                "status": "done",
                "answer": answer,
                "error": None,
                "interaction_id": interaction_id,
            }

        print("JOB DONE:", job_id, "answer_length=", len(answer), flush=True)
    except Exception as exc:
        with JOBS_LOCK:
            JOBS[job_id] = {
                "status": "error",
                "answer": None,
                "error": str(exc),
                "interaction_id": None,
            }

        print("JOB ERROR:", job_id, str(exc), flush=True)


# ------------------------------------------------------------
# LoRA training trigger
# ------------------------------------------------------------

def _run_lora_training() -> None:
    with TRAINING_LOCK:
        approved_count = count_untrained_positive_examples()
        if approved_count < LORA_TRIGGER_COUNT:
            return

        run_id = str(uuid.uuid4())
        export_path = export_positive_examples(run_id)

        if not LORA_TRAIN_COMMAND:
            details = (
                f"Training was not started because CLIMATEGPT_LORA_TRAIN_COMMAND "
                f"is not configured. Export created at {export_path}."
            )
            record_training_run(run_id, "ready", approved_count, "", details)
            write_lora_state(
                {
                    "run_id": run_id,
                    "status": "ready",
                    "approved_examples": approved_count,
                    "export_path": str(export_path),
                    "updated_at": utc_now(),
                }
            )
            return

        command = LORA_TRAIN_COMMAND.format(
            dataset=str(export_path),
            run_id=run_id,
            model=MODEL_NAME,
        )

        record_training_run(
            run_id,
            "running",
            approved_count,
            command,
            f"Dataset: {export_path}",
        )
        write_lora_state(
            {
                "run_id": run_id,
                "status": "running",
                "approved_examples": approved_count,
                "export_path": str(export_path),
                "command": command,
                "updated_at": utc_now(),
            }
        )

        try:
            completed = subprocess.run(
                command,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
            )
            details = (completed.stdout or "") + ("\n" + completed.stderr if completed.stderr else "")
            record_training_run(run_id, "trained_pending_canary", approved_count, command, details)
            mark_examples_used_for_training()
            write_lora_state(
                {
                    "run_id": run_id,
                    "status": "trained_pending_canary",
                    "approved_examples": approved_count,
                    "export_path": str(export_path),
                    "command": command,
                    "details": details[-5000:],
                    "updated_at": utc_now(),
                }
            )
        except Exception as exc:
            record_training_run(run_id, "failed", approved_count, command, str(exc))
            write_lora_state(
                {
                    "run_id": run_id,
                    "status": "failed",
                    "approved_examples": approved_count,
                    "error": str(exc),
                    "updated_at": utc_now(),
                }
            )


def maybe_trigger_lora_training() -> None:
    if count_untrained_positive_examples() < LORA_TRIGGER_COUNT:
        return

    worker = threading.Thread(target=_run_lora_training, daemon=True)
    worker.start()


# ------------------------------------------------------------
# Dash callbacks
# ------------------------------------------------------------

@callback(
    Output("cgpt-question", "value", allow_duplicate=True),
    Input({"type": "cgpt-canary", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def select_canary_question(_clicks):
    triggered = ctx.triggered_id

    if not isinstance(triggered, dict):
        return no_update

    index = triggered.get("index")
    if not isinstance(index, int) or index < 0 or index >= len(CANARY_QUESTIONS):
        return no_update

    return CANARY_QUESTIONS[index]


@callback(
    Output("cgpt-answer", "value"),
    Output("cgpt-info", "children"),
    Output("cgpt-question", "value"),
    Output("cgpt-job-id", "data"),
    Output("cgpt-poll", "disabled"),
    Output("cgpt-interaction-id", "data"),
    Output("cgpt-thumbs-up", "disabled"),
    Output("cgpt-thumbs-down", "disabled"),
    Output("cgpt-feedback-info", "children"),
    Input("cgpt-submit", "n_clicks"),
    Input("cgpt-clear", "n_clicks"),
    State("cgpt-question", "value"),
    prevent_initial_call=True,
)
def submit_climategpt(submit_clicks, clear_clicks, question):
    triggered = ctx.triggered_id

    if triggered == "cgpt-clear":
        return (
            ANSWER_PLACEHOLDER,
            "Cleared.",
            "",
            None,
            True,
            None,
            True,
            True,
            "",
        )

    if triggered != "cgpt-submit":
        return (
            ANSWER_PLACEHOLDER,
            "",
            question,
            None,
            True,
            None,
            True,
            True,
            "",
        )

    if not question or not question.strip():
        return (
            "Please enter a question.",
            "",
            question,
            None,
            True,
            None,
            True,
            True,
            "",
        )

    job_id = str(uuid.uuid4())
    clean_question = question.strip()

    with JOBS_LOCK:
        JOBS[job_id] = {
            "status": "running",
            "answer": None,
            "error": None,
            "interaction_id": None,
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
        None,
        True,
        True,
        "",
    )


@callback(
    Output("cgpt-answer", "value", allow_duplicate=True),
    Output("cgpt-info", "children", allow_duplicate=True),
    Output("cgpt-poll", "disabled", allow_duplicate=True),
    Output("cgpt-interaction-id", "data", allow_duplicate=True),
    Output("cgpt-thumbs-up", "disabled", allow_duplicate=True),
    Output("cgpt-thumbs-down", "disabled", allow_duplicate=True),
    Input("cgpt-poll", "n_intervals"),
    State("cgpt-job-id", "data"),
    prevent_initial_call=True,
)
def poll_climategpt(n_intervals, job_id):
    if not job_id:
        return ANSWER_PLACEHOLDER, "", True, None, True, True

    with JOBS_LOCK:
        job = JOBS.get(job_id)

    if not job:
        return "No active ClimateGPT job was found.", "", True, None, True, True

    status = job.get("status")

    if status == "running":
        return (
            f"ClimateGPT is still generating an answer...\n\nPolling check: {n_intervals}",
            "Still running...",
            False,
            None,
            True,
            True,
        )

    if status == "error":
        return (
            f"Error generating answer:\n\n{job.get('error')}",
            "Generation failed.",
            True,
            None,
            True,
            True,
        )

    answer = job.get("answer") or "ClimateGPT returned an empty answer."
    interaction_id = job.get("interaction_id")

    with JOBS_LOCK:
        JOBS.pop(job_id, None)

    return answer, "Generation complete.", True, interaction_id, False, False


@callback(
    Output("cgpt-feedback-info", "children", allow_duplicate=True),
    Output("cgpt-thumbs-up", "disabled", allow_duplicate=True),
    Output("cgpt-thumbs-down", "disabled", allow_duplicate=True),
    Output("cgpt-training-count", "children", allow_duplicate=True),
    Input("cgpt-thumbs-up", "n_clicks"),
    Input("cgpt-thumbs-down", "n_clicks"),
    State("cgpt-interaction-id", "data"),
    prevent_initial_call=True,
)
def record_user_feedback(up_clicks, down_clicks, interaction_id):
    if not interaction_id:
        return "No completed answer is available for feedback.", True, True, no_update

    triggered = ctx.triggered_id
    rating = "up" if triggered == "cgpt-thumbs-up" else "down"

    try:
        save_feedback(interaction_id, rating)
    except Exception as exc:
        return f"Could not save feedback: {exc}", False, False, no_update

    approved_count = count_untrained_positive_examples()

    if rating == "up":
        maybe_trigger_lora_training()
        message = "Thank you. This answer was approved for possible training use."
    else:
        message = "Thank you. This answer was marked as unsuitable and will not be used for training."

    return (
        message,
        True,
        True,
        f"Approved examples awaiting training: {approved_count} / {LORA_TRIGGER_COUNT}",
    )


@callback(
    Output("cgpt-training-info", "children"),
    Output("cgpt-training-count", "children", allow_duplicate=True),
    Input("cgpt-check-training", "n_clicks"),
    prevent_initial_call=True,
)
def check_training_readiness(_n_clicks):
    approved_count = count_untrained_positive_examples()
    count_text = (
        f"Approved examples awaiting training: "
        f"{approved_count} / {LORA_TRIGGER_COUNT}"
    )

    if approved_count < LORA_TRIGGER_COUNT:
        return (
            f"Not ready: {LORA_TRIGGER_COUNT - approved_count} more approved "
            "examples are required.",
            count_text,
        )

    maybe_trigger_lora_training()

    if LORA_TRAIN_COMMAND:
        return (
            "Training threshold reached. The configured LoRA command has been "
            "started in a background thread. Validate the resulting adapter on "
            "the canary set before deployment.",
            count_text,
        )

    return (
        "Training threshold reached. Approved examples will be exported, but "
        "training will not start until CLIMATEGPT_LORA_TRAIN_COMMAND is configured.",
        count_text,
    )
