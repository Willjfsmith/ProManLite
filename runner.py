"""
runner.py — the bridge between the web app and the Claude API.

The core flow for a "workshop" skill:
  1. Upload the user's files to the Anthropic Files API.
  2. Send Claude a message that (a) shows it the files it can view (PDFs, images)
     and (b) makes every file available in a code-execution sandbox.
  3. Claude does the work in the sandbox and saves deliverables as files.
  4. We capture those output files and hand them back for download.

"prompt" skills are the simple case: text in, text out, optional file context.

Runs are first-class, server-side objects (see `Run`). A run executes on a
background thread and records its own progress, so it survives the browser tab
being closed: the UI can disconnect and later reconnect (or just re-list the
run history) and still stream the result and download the output files.

Model, betas, and tool versions are constants at the top so they are easy to
change in one place.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Iterator

import anthropic

# -- models -----------------------------------------------------------------
# The catalogue the UI offers. `tag` drives the "Recommended / Cheapest" badge;
# order here is the order shown in the dropdown.
MODELS = [
    {
        "id": "claude-opus-4-8",
        "name": "Opus 4.8",
        "tag": "Recommended",
        "blurb": "Most capable — best for dense or vectorised P&IDs and tricky vision reads.",
    },
    {
        "id": "claude-sonnet-5",
        "name": "Sonnet 5",
        "tag": "Cheaper & faster",
        "blurb": "Great for most drawings; noticeably faster and lower cost than Opus.",
    },
    {
        "id": "claude-haiku-4-5-20251001",
        "name": "Haiku 4.5",
        "tag": "Cheapest",
        "blurb": "Fastest and lowest cost — best for simple extraction and title-block reads.",
    },
]
DEFAULT_MODEL = "claude-opus-4-8"
_MODEL_IDS = {m["id"] for m in MODELS}
_MODEL_NAMES = {m["id"]: m["name"] for m in MODELS}


def resolve_model(model_id: str | None) -> str:
    """Return a valid model id, falling back to the default for anything unknown."""
    return model_id if model_id in _MODEL_IDS else DEFAULT_MODEL


MAX_TOKENS = 32000

# Keep the in-memory run store bounded on long-lived persistent hosts.
MAX_RUNS_PER_SESSION = 100

FILES_BETA = "files-api-2025-04-14"
CODE_BETA = "code-execution-2025-08-25"
SKILLS_BETA = "skills-2025-10-02"
CODE_TOOL = {"type": "code_execution_20260521", "name": "code_execution"}

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".tif", ".tiff"}
PDF_EXTS = {".pdf"}

WORKSHOP_DIRECTIVE = (
    "\n\nYou are running inside a code-execution sandbox. The user's uploaded files "
    "are available in the working directory (run `ls` to find them); viewable files "
    "(PDFs, images) are also shown to you inline. Do the requested work, then SAVE "
    "every deliverable as a file in the working directory with a clear name. Use the "
    "right library for the deliverable (openpyxl for .xlsx, python-docx for .docx, "
    "PyMuPDF/pypdf/reportlab for PDF markup); `pip install` anything not already present. "
    "When finished, give a brief plain-text summary of what you produced and the key "
    "findings. The files you save will be returned to the user to download — so make "
    "sure the deliverables are written to disk."
)


@dataclass
class GeneratedFile:
    file_id: str
    filename: str


@dataclass
class Conversation:
    messages: list = field(default_factory=list)
    container_id: str | None = None


@dataclass
class Run:
    """A single execution of a skill. Lives on the server for the session's lifetime."""

    id: str
    session_id: str
    skill_id: str
    skill_name: str
    skill_emoji: str
    model: str
    model_name: str
    summary: list = field(default_factory=list)      # input chips, e.g. ["5 p&ids"]
    status: str = "running"                            # running | done | error | stopped
    created_at: float = 0.0
    status_text: str = "Starting…"                    # latest progress line
    text: str = ""                                     # accumulated assistant text
    files: list = field(default_factory=list)          # [{"file_id","name"}]
    error: str | None = None
    stop_flag: bool = False
    # Ordered event log (status/text/file/error/done) for live + reconnecting viewers.
    events: list = field(default_factory=list)
    cond: threading.Condition = field(default_factory=threading.Condition, repr=False)

    def snapshot(self) -> dict:
        with self.cond:
            return {
                "id": self.id,
                "skill_id": self.skill_id,
                "skill_name": self.skill_name,
                "skill_emoji": self.skill_emoji,
                "model": self.model,
                "model_name": self.model_name,
                "summary": list(self.summary),
                "status": self.status,
                "created_at": self.created_at,
                "status_text": self.status_text,
                "text": self.text,
                "files": [dict(f) for f in self.files],
                "error": self.error,
                "event_count": len(self.events),
            }


class SkillRunner:
    def __init__(self) -> None:
        self._client: anthropic.Anthropic | None = None
        self._conversations: dict[tuple[str, str], Conversation] = {}
        self._runs: dict[str, Run] = {}
        self._session_index: dict[str, list[str]] = {}
        self._lock = threading.Lock()

    # -- client -------------------------------------------------------------
    @property
    def configured(self) -> bool:
        return bool(os.environ.get("ANTHROPIC_API_KEY"))

    def _client_(self) -> anthropic.Anthropic:
        if self._client is None:
            self._client = anthropic.Anthropic()
        return self._client

    def _conversation(self, session_id: str, skill_id: str) -> Conversation:
        key = (session_id, skill_id)
        with self._lock:
            conv = self._conversations.get(key)
            if conv is None:
                conv = Conversation()
                self._conversations[key] = conv
            return conv

    def reset(self, session_id: str, skill_id: str) -> None:
        """Clear a skill's conversation/sandbox and drop its run history for this session."""
        with self._lock:
            self._conversations.pop((session_id, skill_id), None)
            keep = []
            for run_id in self._session_index.get(session_id, []):
                run = self._runs.get(run_id)
                if run and run.skill_id == skill_id:
                    self._runs.pop(run_id, None)
                else:
                    keep.append(run_id)
            self._session_index[session_id] = keep

    # -- run lifecycle ------------------------------------------------------
    def start(
        self,
        session_id: str,
        skill: dict,
        instructions: str,
        review_files: list[tuple[str, bytes]],
        reference_files: list[tuple[str, bytes]],
        model: str | None,
    ) -> Run:
        """Create a run and kick off execution on a background thread. Returns immediately."""
        model = resolve_model(model)
        uploads = [("review", n, b) for n, b in review_files] + \
                  [("reference", n, b) for n, b in reference_files]
        run = Run(
            id="r_" + uuid.uuid4().hex[:12],
            session_id=session_id,
            skill_id=skill["id"],
            skill_name=skill.get("name") or skill["id"],
            skill_emoji=skill.get("emoji") or "🧩",
            model=model,
            model_name=_MODEL_NAMES.get(model, model),
            summary=self._summary(skill, instructions, uploads),
            created_at=time.time(),
        )
        with self._lock:
            self._runs[run.id] = run
            index = self._session_index.setdefault(session_id, [])
            index.append(run.id)
            # Evict the oldest finished runs once the session's history is full.
            while len(index) > MAX_RUNS_PER_SESSION:
                oldest = self._runs.get(index[0])
                if oldest and oldest.status == "running":
                    break  # never drop a run that's still working
                self._runs.pop(index.pop(0), None)
        threading.Thread(
            target=self._execute,
            args=(run, skill, instructions, uploads, model),
            daemon=True,
        ).start()
        return run

    def get_run(self, run_id: str) -> Run | None:
        return self._runs.get(run_id)

    def list_runs(self, session_id: str, skill_id: str | None = None) -> list[dict]:
        with self._lock:
            ids = list(self._session_index.get(session_id, []))
        out = []
        for run_id in ids:
            run = self._runs.get(run_id)
            if run and (skill_id is None or run.skill_id == skill_id):
                out.append(run.snapshot())
        return out

    def stop(self, run_id: str) -> bool:
        run = self._runs.get(run_id)
        if not run:
            return False
        with run.cond:
            if run.status == "running":
                run.stop_flag = True
                run.cond.notify_all()
        return True

    def stream(self, run_id: str, cursor: int = 0) -> Iterator[dict]:
        """Yield events for a run from `cursor`, blocking for new ones until it finishes."""
        run = self._runs.get(run_id)
        if run is None:
            yield {"type": "error", "message": "Unknown run."}
            yield {"type": "done"}
            return
        i = max(0, cursor)
        while True:
            with run.cond:
                while i >= len(run.events) and run.status == "running":
                    run.cond.wait(timeout=30.0)
                pending = run.events[i:]
                i = len(run.events)
                finished = run.status != "running"
            for ev in pending:
                yield ev
            if finished and i >= len(run.events):
                return

    def _summary(self, skill: dict, instructions: str, uploads: list) -> list[str]:
        counts = {"review": 0, "reference": 0}
        for group, _name, _data in uploads:
            counts[group] = counts.get(group, 0) + 1
        parts = []
        for inp in skill.get("inputs", []):
            n = counts.get(inp["key"], 0)
            if n:
                parts.append(f"{n} {inp['label'].lower()}")
        if instructions.strip():
            parts.append("instructions")
        return parts

    def _emit(self, run: Run, event: dict) -> None:
        with run.cond:
            run.events.append(event)
            t = event.get("type")
            if t == "status":
                run.status_text = event["text"]
            elif t == "text":
                run.text += event["text"]
            elif t == "file":
                run.files.append({"file_id": event["file_id"], "name": event["name"]})
            elif t == "error":
                run.error = event["message"]
                run.status = "error"
            elif t == "done":
                if run.status == "running":
                    run.status = "stopped" if run.stop_flag else "done"
                event["status"] = run.status
            run.cond.notify_all()

    def _events(self, conv, skill, instructions, uploads, model, should_stop) -> Iterator[dict]:
        """Shared execution generator for both the background store and the
        synchronous (serverless) path. Yields status/text/file/error events; the
        caller appends the terminating 'done' event."""
        gen = None
        try:
            if skill["type"] == "workshop":
                gen = self._run_workshop(conv, skill, instructions, uploads, model)
            else:
                gen = self._run_prompt(conv, skill, instructions, uploads, model)
            for event in gen:
                yield event
                if should_stop():
                    gen.close()
                    return
        except anthropic.APIStatusError as exc:
            if conv.messages and conv.messages[-1]["role"] == "user":
                conv.messages.pop()
            yield {"type": "error", "message": f"API error ({exc.status_code}): {exc.message}"}
        except Exception as exc:  # noqa: BLE001
            if conv.messages and conv.messages[-1]["role"] == "user":
                conv.messages.pop()
            yield {"type": "error", "message": f"Unexpected error: {exc}"}
        finally:
            if gen is not None:
                gen.close()

    def _execute(self, run: Run, skill: dict, instructions: str, uploads: list, model: str) -> None:
        """Body of the background thread (persistent hosts): drain events into the run."""
        if not self.configured:
            self._emit(run, {"type": "error", "message":
                       "The server has no ANTHROPIC_API_KEY configured. Set it and restart the app."})
            self._emit(run, {"type": "done"})
            return
        conv = self._conversation(run.session_id, skill["id"])
        for event in self._events(conv, skill, instructions, uploads, model, lambda: run.stop_flag):
            self._emit(run, event)
        self._emit(run, {"type": "done"})

    def run_stream(self, session_id, skill, instructions, review_files, reference_files, model) -> Iterator[dict]:
        """Synchronous, storeless execution for serverless hosts (no shared memory
        across requests): run the job inside this request and stream the events.
        No history, reconnect, or stop — the run lives only as long as the connection."""
        model = resolve_model(model)
        uploads = [("review", n, b) for n, b in review_files] + \
                  [("reference", n, b) for n, b in reference_files]
        if not self.configured:
            yield {"type": "error", "message":
                   "The server has no ANTHROPIC_API_KEY configured. Set it and restart the app."}
            yield {"type": "done"}
            return
        conv = self._conversation(session_id, skill["id"])
        for event in self._events(conv, skill, instructions, uploads, model, lambda: False):
            yield event
        yield {"type": "done"}

    # -- uploads / blocks ---------------------------------------------------
    def _upload(self, name: str, data: bytes) -> tuple[str, str]:
        """Upload a file, return (file_id, kind) where kind is image|pdf|other."""
        client = self._client_()
        uploaded = client.beta.files.upload(file=(name, data, None), betas=[FILES_BETA])
        ext = os.path.splitext(name)[1].lower()
        kind = "image" if ext in IMAGE_EXTS else "pdf" if ext in PDF_EXTS else "other"
        return uploaded.id, kind

    def _view_block(self, file_id: str, kind: str) -> dict | None:
        if kind == "image":
            return {"type": "image", "source": {"type": "file", "file_id": file_id}}
        if kind == "pdf":
            return {"type": "document", "source": {"type": "file", "file_id": file_id}}
        return None

    # -- workshop -----------------------------------------------------------
    def _run_workshop(self, conv, skill, instructions, uploads, model) -> Iterator[dict]:
        client = self._client_()

        review_names, reference_names = [], []
        view_blocks, container_blocks = [], []

        if uploads:
            yield {"type": "status", "text": f"Uploading {len(uploads)} file(s)…"}
            for group, name, data in uploads:
                file_id, kind = self._upload(name, data)
                (review_names if group == "review" else reference_names).append(name)
                container_blocks.append({"type": "container_upload", "file_id": file_id})
                vb = self._view_block(file_id, kind)
                if vb:
                    view_blocks.append(vb)

        # Build the user turn: files first, then the manifest + instructions.
        manifest = []
        if review_names:
            manifest.append("Files to review: " + ", ".join(review_names))
        if reference_names:
            manifest.append("Reference files: " + ", ".join(reference_names))
        text = "\n".join(manifest)
        if instructions.strip():
            text += ("\n\n" if text else "") + "Instructions:\n" + instructions.strip()
        if not text:
            text = "Proceed with the uploaded files per your role."

        content = view_blocks + container_blocks + [{"type": "text", "text": text}]
        conv.messages.append({"role": "user", "content": content})

        betas = [FILES_BETA, CODE_BETA]
        container = conv.container_id
        if skill.get("skill_id"):
            betas.append(SKILLS_BETA)
            if not container:
                container = {"skills": [{"type": "anthropic", "skill_id": skill["skill_id"], "version": "latest"}]}

        kwargs = dict(
            model=model,
            max_tokens=MAX_TOKENS,
            betas=betas,
            system=skill["system"] + WORKSHOP_DIRECTIVE,
            tools=[CODE_TOOL],
            messages=conv.messages,
        )
        if container is not None:
            kwargs["container"] = container

        yield {"type": "status", "text": "Working in the sandbox…"}

        with client.beta.messages.stream(**kwargs) as stream:
            for event in stream:
                if event.type == "content_block_delta" and event.delta.type == "text_delta":
                    yield {"type": "text", "text": event.delta.text}
            final = stream.get_final_message()

        if getattr(final, "container", None):
            conv.container_id = final.container.id

        conv.messages.append({"role": "assistant", "content": final.content})

        files = self._extract_files(final)
        if files:
            yield {"type": "status", "text": f"Retrieving {len(files)} output file(s)…"}
            for gen in files:
                yield {"type": "file", "file_id": gen.file_id, "name": gen.filename}

    # -- prompt -------------------------------------------------------------
    def _run_prompt(self, conv, skill, instructions, uploads, model) -> Iterator[dict]:
        client = self._client_()

        view_blocks = []
        if uploads:
            yield {"type": "status", "text": f"Uploading {len(uploads)} file(s)…"}
            for _group, name, data in uploads:
                file_id, kind = self._upload(name, data)
                vb = self._view_block(file_id, kind)
                if vb:
                    view_blocks.append(vb)

        text = instructions.strip() or "Please respond."
        conv.messages.append({"role": "user", "content": view_blocks + [{"type": "text", "text": text}]})

        yield {"type": "status", "text": "Thinking…"}

        if view_blocks:
            ctx = client.beta.messages.stream(
                model=model, max_tokens=MAX_TOKENS, betas=[FILES_BETA],
                system=skill["system"], thinking={"type": "adaptive"}, messages=conv.messages,
            )
        else:
            ctx = client.messages.stream(
                model=model, max_tokens=MAX_TOKENS,
                system=skill["system"], thinking={"type": "adaptive"}, messages=conv.messages,
            )
        with ctx as stream:
            for text_delta in stream.text_stream:
                yield {"type": "text", "text": text_delta}
            final = stream.get_final_message()

        conv.messages.append({"role": "assistant", "content": final.content})

    # -- output file capture ------------------------------------------------
    def _extract_files(self, message) -> list[GeneratedFile]:
        found: list[GeneratedFile] = []
        seen: set[str] = set()
        for block in message.content:
            if block.type != "bash_code_execution_tool_result":
                continue
            result = getattr(block, "content", None)
            items = getattr(result, "content", None) if result else None
            if not items:
                continue
            for ref in items:
                file_id = getattr(ref, "file_id", None)
                if not file_id or file_id in seen:
                    continue
                seen.add(file_id)
                found.append(GeneratedFile(file_id=file_id, filename=self._filename_for(file_id)))
        return found

    def _filename_for(self, file_id: str) -> str:
        try:
            meta = self._client_().beta.files.retrieve_metadata(file_id, betas=[FILES_BETA])
            return os.path.basename(meta.filename) or file_id
        except Exception:  # noqa: BLE001
            return file_id

    def download(self, file_id: str) -> tuple[str, bytes]:
        client = self._client_()
        name = self._filename_for(file_id)
        content = client.beta.files.download(file_id, betas=[FILES_BETA])
        return name, content.read()


runner = SkillRunner()
