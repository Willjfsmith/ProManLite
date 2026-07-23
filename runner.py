"""
runner.py — the bridge between the web app and the Claude API.

Responsibilities:
  * Hold per-conversation state (message history + container reuse) in memory.
  * Stream a Claude response for a given skill.
  * For document skills, capture any files Claude generates and make them
    downloadable.

Everything here targets the current Anthropic Python SDK. The model, betas, and
tool versions are pulled into constants at the top so they are easy to update.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field
from typing import Iterator

import anthropic

MODEL = "claude-opus-4-8"
MAX_TOKENS = 32000

# Betas required to run Anthropic document skills inside a code-execution container.
SKILL_BETAS = ["code-execution-2025-08-25", "skills-2025-10-02"]
FILES_BETA = "files-api-2025-04-14"
CODE_EXECUTION_TOOL = {"type": "code_execution_20260521", "name": "code_execution"}


@dataclass
class GeneratedFile:
    file_id: str
    filename: str


@dataclass
class Conversation:
    """In-memory state for one browser session + skill combination."""

    messages: list = field(default_factory=list)
    container_id: str | None = None  # reused across turns for document skills


class SkillRunner:
    def __init__(self) -> None:
        self._client: anthropic.Anthropic | None = None
        # keyed by (session_id, skill_id) so switching skills starts a fresh thread
        self._conversations: dict[tuple[str, str], Conversation] = {}
        self._lock = threading.Lock()

    # -- client -------------------------------------------------------------
    @property
    def configured(self) -> bool:
        return bool(os.environ.get("ANTHROPIC_API_KEY"))

    def _get_client(self) -> anthropic.Anthropic:
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

    # -- streaming ----------------------------------------------------------
    def stream(self, session_id: str, skill: dict, user_message: str) -> Iterator[dict]:
        """
        Yield event dicts as the response is produced:
          {"type": "text", "text": "..."}       incremental assistant text
          {"type": "file", "file_id", "name"}   a downloadable generated file
          {"type": "error", "message": "..."}   something went wrong
          {"type": "done"}                       end of turn
        """
        if not self.configured:
            yield {
                "type": "error",
                "message": "The server has no ANTHROPIC_API_KEY configured. "
                "Set it and restart the app.",
            }
            yield {"type": "done"}
            return

        conv = self._conversation(session_id, skill["id"])
        conv.messages.append({"role": "user", "content": user_message})

        try:
            if skill["type"] == "document":
                yield from self._stream_document_skill(conv, skill)
            else:
                yield from self._stream_prompt_skill(conv, skill)
        except anthropic.APIStatusError as exc:
            # Roll back the user turn so a retry starts clean.
            conv.messages.pop()
            yield {"type": "error", "message": f"API error ({exc.status_code}): {exc.message}"}
        except Exception as exc:  # noqa: BLE001 - surface anything to the UI
            conv.messages.pop()
            yield {"type": "error", "message": f"Unexpected error: {exc}"}

        yield {"type": "done"}

    def _stream_prompt_skill(self, conv: Conversation, skill: dict) -> Iterator[dict]:
        client = self._get_client()
        assistant_text: list[str] = []

        with client.messages.stream(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=skill["system"],
            thinking={"type": "adaptive"},
            messages=conv.messages,
        ) as stream:
            for text in stream.text_stream:
                assistant_text.append(text)
                yield {"type": "text", "text": text}
            final = stream.get_final_message()

        conv.messages.append({"role": "assistant", "content": final.content})
        # Guard against an assistant turn with no text (keeps history valid).
        if not any(b.type == "text" for b in final.content):
            conv.messages.append(
                {"role": "assistant", "content": "".join(assistant_text) or "(no response)"}
            )

    def _stream_document_skill(self, conv: Conversation, skill: dict) -> Iterator[dict]:
        client = self._get_client()

        kwargs = dict(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            betas=SKILL_BETAS,
            container={"skills": [{"type": "anthropic", "skill_id": skill["skill_id"], "version": "latest"}]},
            tools=[CODE_EXECUTION_TOOL],
            messages=conv.messages,
        )
        if conv.container_id:
            kwargs["container"] = conv.container_id  # reuse the same workspace

        with client.beta.messages.stream(**kwargs) as stream:
            for event in stream:
                if event.type == "content_block_delta" and event.delta.type == "text_delta":
                    yield {"type": "text", "text": event.delta.text}
            final = stream.get_final_message()

        # Remember the container so follow-up turns keep the same files/state.
        if getattr(final, "container", None):
            conv.container_id = final.container.id

        conv.messages.append({"role": "assistant", "content": final.content})

        for gen in self._extract_files(final):
            yield {"type": "file", "file_id": gen.file_id, "name": gen.filename}

    # -- file capture -------------------------------------------------------
    def _extract_files(self, message) -> list[GeneratedFile]:
        found: list[GeneratedFile] = []
        for block in message.content:
            if block.type != "bash_code_execution_tool_result":
                continue
            result = getattr(block, "content", None)
            items = getattr(result, "content", None) if result else None
            if not items:
                continue
            for ref in items:
                file_id = getattr(ref, "file_id", None)
                if not file_id:
                    continue
                name = self._filename_for(file_id)
                found.append(GeneratedFile(file_id=file_id, filename=name))
        return found

    def _filename_for(self, file_id: str) -> str:
        try:
            meta = self._get_client().beta.files.retrieve_metadata(file_id)
            return os.path.basename(meta.filename) or file_id
        except Exception:  # noqa: BLE001
            return file_id

    def download(self, file_id: str) -> tuple[str, bytes]:
        """Return (filename, bytes) for a generated file."""
        client = self._get_client()
        name = self._filename_for(file_id)
        content = client.beta.files.download(file_id)
        data = content.read()
        return name, data


runner = SkillRunner()
