"""
main.py — FastAPI app that serves the front end and the run API.

Endpoints:
  GET  /                       the single-page UI
  GET  /api/skills             the skill catalogue + model list (drives the UI)
  POST /api/run                upload files + instructions, start a run (returns run_id)
  GET  /api/runs               list this session's runs (history)
  GET  /api/runs/{id}          a run's current state (snapshot)
  GET  /api/runs/{id}/stream   live event stream for a run (SSE, reconnectable)
  POST /api/runs/{id}/stop     ask a running run to stop
  POST /api/reset              clear a skill's conversation/sandbox + its run history
  GET  /api/files/{id}         download a generated file
  GET  /api/health             liveness + whether an API key is configured

Runs execute on a background thread and are stored in memory server-side, so a
run keeps going (and its output files stay reachable) even if the browser tab is
closed. History survives page reloads but not a server restart — no database.
"""

from __future__ import annotations

import json
import mimetypes
import os
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel

import prompts
from runner import MODELS, DEFAULT_MODEL, runner

APP_DIR = Path(__file__).parent
INDEX_HTML = (APP_DIR / "index.html").read_text(encoding="utf-8")

# On serverless (Vercel) each request is a fresh, isolated invocation with no
# shared memory and no way to keep a background thread alive past the response —
# so the run store (history / reconnect / stop) can't work there. Detect it and
# fall back to running the job synchronously inside the request. Persistent hosts
# (Docker/uvicorn, Railway, Render, Fly, Cloud Run, a VM) get the full store.
PERSISTENT = not bool(os.environ.get("VERCEL") or os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))

# Guardrail so a single request can't try to push absurd volumes through the API.
MAX_FILES = 25
MAX_TOTAL_BYTES = 100 * 1024 * 1024  # 100 MB per run

app = FastAPI(title="Team Skills", docs_url=None, redoc_url=None)


class ResetRequest(BaseModel):
    session_id: str
    skill_id: str


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return INDEX_HTML


@app.get("/api/health")
def health() -> dict:
    return {"ok": True, "configured": runner.configured, "persistent": PERSISTENT}


@app.get("/api/skills")
def skills() -> dict:
    return {
        "skills": prompts.list_skills(),
        "models": MODELS,
        "default_model": DEFAULT_MODEL,
        "configured": runner.configured,
        "persistent": PERSISTENT,
    }


@app.post("/api/reload")
def reload_skills() -> dict:
    """Re-scan the skills/ folder without a full restart (useful on persistent hosts)."""
    count = prompts.reload()
    return {"ok": True, "count": count}


@app.post("/api/run")
async def run(
    session_id: str = Form(...),
    skill_id: str = Form(...),
    instructions: str = Form(""),
    model: str = Form(""),
    stream: str = Form(""),
    review: list[UploadFile] = File(default=[]),
    reference: list[UploadFile] = File(default=[]),
):
    skill = prompts.get_skill(skill_id)
    if skill is None:
        raise HTTPException(status_code=404, detail="Unknown skill")

    all_uploads = list(review) + list(reference)
    if len(all_uploads) > MAX_FILES:
        raise HTTPException(status_code=413, detail=f"Too many files (max {MAX_FILES}).")

    # Read uploads into memory now — the UploadFile objects don't survive past
    # this request, but the run keeps executing on a background thread.
    total = 0

    async def read_group(group: list[UploadFile]) -> list[tuple[str, bytes]]:
        nonlocal total
        out = []
        for f in group:
            data = await f.read()
            total += len(data)
            out.append((f.filename or "upload", data))
        return out

    review_files = await read_group(review)
    reference_files = await read_group(reference)

    if total > MAX_TOTAL_BYTES:
        raise HTTPException(status_code=413, detail="Uploads exceed the 100 MB per-run limit.")

    if not instructions.strip() and not all_uploads:
        raise HTTPException(status_code=400, detail="Add files or instructions to run.")

    # Serverless (or any client that opts in): run inside this request and stream.
    if stream or not PERSISTENT:
        def event_stream():
            for event in runner.run_stream(
                session_id, skill, instructions, review_files, reference_files, model or None
            ):
                yield f"data: {json.dumps(event)}\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # Persistent host: start on a background thread and return immediately.
    run = runner.start(session_id, skill, instructions, review_files, reference_files, model or None)
    return {"run_id": run.id, "run": run.snapshot()}


@app.get("/api/runs")
def list_runs(session_id: str, skill_id: str | None = None) -> dict:
    return {"runs": runner.list_runs(session_id, skill_id)}


@app.get("/api/runs/{run_id}")
def get_run(run_id: str) -> dict:
    run = runner.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Unknown run")
    return run.snapshot()


@app.get("/api/runs/{run_id}/stream")
def stream_run(run_id: str, cursor: int = 0) -> StreamingResponse:
    if runner.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail="Unknown run")

    def event_stream():
        for event in runner.stream(run_id, cursor):
            yield f"data: {json.dumps(event)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/runs/{run_id}/stop")
def stop_run(run_id: str) -> dict:
    if not runner.stop(run_id):
        raise HTTPException(status_code=404, detail="Unknown run")
    return {"ok": True}


@app.post("/api/reset")
def reset(req: ResetRequest) -> dict:
    runner.reset(req.session_id, req.skill_id)
    return {"ok": True}


@app.get("/api/files/{file_id}")
def download_file(file_id: str):
    try:
        name, data = runner.download(file_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail=f"File unavailable: {exc}") from exc

    media_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
    return Response(
        content=data,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{name}"'},
    )


if __name__ == "__main__":
    import os

    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
