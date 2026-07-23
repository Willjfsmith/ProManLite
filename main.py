"""
main.py — FastAPI app that serves the front end and a streaming chat API.

Endpoints:
  GET  /                     the single-page UI
  GET  /api/skills           the skill catalogue
  POST /api/chat             stream a response (Server-Sent Events)
  POST /api/reset            clear a conversation thread
  GET  /api/files/{id}       download a generated file
  GET  /api/health           liveness + whether an API key is configured

No database, no login — it is designed to run behind your own network or SSO.
See the README for deployment notes.
"""

from __future__ import annotations

import json
import mimetypes
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel

import prompts
from runner import runner

APP_DIR = Path(__file__).parent
INDEX_HTML = (APP_DIR / "index.html").read_text(encoding="utf-8")

app = FastAPI(title="Team Skills", docs_url=None, redoc_url=None)


class ChatRequest(BaseModel):
    session_id: str
    skill_id: str
    message: str


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


@app.post("/api/chat")
def chat(req: ChatRequest) -> StreamingResponse:
    skill = prompts.get_skill(req.skill_id)
    if skill is None:
        raise HTTPException(status_code=404, detail="Unknown skill")

    message = (req.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Empty message")

    def event_stream():
        for event in runner.stream(req.session_id, skill, message):
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
