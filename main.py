"""
main.py — FastAPI app that serves the front end and the run API.

Endpoints:
  GET  /                     the single-page UI
  GET  /api/skills           the skill catalogue (drives the UI)
  POST /api/run              upload files + instructions, stream the run (SSE)
  POST /api/reset            clear a skill's conversation/sandbox
  GET  /api/files/{id}       download a generated file
  GET  /api/health           liveness + whether an API key is configured

No database, no login — designed to run behind your own network or SSO.
"""

from __future__ import annotations

import json
import mimetypes
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel

import prompts
from runner import runner

APP_DIR = Path(__file__).parent
INDEX_HTML = (APP_DIR / "index.html").read_text(encoding="utf-8")

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
    return {"ok": True, "configured": runner.configured}


@app.get("/api/skills")
def skills() -> dict:
    return {"skills": prompts.list_skills(), "configured": runner.configured}


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
    review: list[UploadFile] = File(default=[]),
    reference: list[UploadFile] = File(default=[]),
) -> StreamingResponse:
    skill = prompts.get_skill(skill_id)
    if skill is None:
        raise HTTPException(status_code=404, detail="Unknown skill")

    all_uploads = list(review) + list(reference)
    if len(all_uploads) > MAX_FILES:
        raise HTTPException(status_code=413, detail=f"Too many files (max {MAX_FILES}).")

    # Read uploads into memory now — the UploadFile objects don't survive the
    # streaming response generator.
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

    def event_stream():
        for event in runner.run(session_id, skill, instructions, review_files, reference_files):
            yield f"data: {json.dumps(event)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


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
