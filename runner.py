"""
runner.py — the bridge between the web app and the Claude API.

The core flow for a "workshop" skill:
  1. Upload the user's files to the Anthropic Files API.
  2. Send Claude a message that (a) shows it the files it can view (PDFs, images)
     and (b) makes every file available in a code-execution sandbox.
  3. Claude does the work in the sandbox and saves deliverables as files.
  4. We capture those output files and hand them back for download.

"prompt" skills are the simple case: text in, text out, optional file context.

Model, betas, and tool versions are constants at the top so they are easy to
change in one place.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field
from typing import Iterator

import anthropic

MODEL = "claude-opus-4-8"
MAX_TOKENS = 32000

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


class SkillRunner:
    def __init__(self) -> None:
        self._client: anthropic.Anthropic | None = None
        self._conversations: dict[tuple[str, str], Conversation] = {}
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
        with self._lock:
            self._conversations.pop((session_id, skill_id), None)

    # -- entry point --------------------------------------------------------
    def run(
        self,
        session_id: str,
        skill: dict,
        instructions: str,
        review_files: list[tuple[str, bytes]],
        reference_files: list[tuple[str, bytes]],
    ) -> Iterator[dict]:
        """
        Yield event dicts:
          {"type": "status", "text": "..."}     progress line (upload, thinking)
          {"type": "text",   "text": "..."}     incremental assistant text
          {"type": "file",   "file_id", "name"} a downloadable output file
          {"type": "error",  "message": "..."}  failure
          {"type": "done"}                       end of run
        """
        if not self.configured:
            yield {"type": "error", "message":
                   "The server has no ANTHROPIC_API_KEY configured. Set it and restart the app."}
            yield {"type": "done"}
            return

        conv = self._conversation(session_id, skill["id"])
        uploads = [("review", n, b) for n, b in review_files] + \
                  [("reference", n, b) for n, b in reference_files]

        try:
            if skill["type"] == "workshop":
                yield from self._run_workshop(conv, skill, instructions, uploads)
            else:
                yield from self._run_prompt(conv, skill, instructions, uploads)
        except anthropic.APIStatusError as exc:
            if conv.messages and conv.messages[-1]["role"] == "user":
                conv.messages.pop()
            yield {"type": "error", "message": f"API error ({exc.status_code}): {exc.message}"}
        except Exception as exc:  # noqa: BLE001
            if conv.messages and conv.messages[-1]["role"] == "user":
                conv.messages.pop()
            yield {"type": "error", "message": f"Unexpected error: {exc}"}

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
    def _run_workshop(self, conv, skill, instructions, uploads) -> Iterator[dict]:
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
            model=MODEL,
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
    def _run_prompt(self, conv, skill, instructions, uploads) -> Iterator[dict]:
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
                model=MODEL, max_tokens=MAX_TOKENS, betas=[FILES_BETA],
                system=skill["system"], thinking={"type": "adaptive"}, messages=conv.messages,
            )
        else:
            ctx = client.messages.stream(
                model=MODEL, max_tokens=MAX_TOKENS,
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
