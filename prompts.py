"""
Skill loader — turns the `skills/` folder into the app's skill catalogue.

Each skill is a folder with a single `SKILL.md`:

    skills/
      drawing-review/
        SKILL.md

`SKILL.md` is markdown with a small YAML front-matter header; the body below the
header is the instructions (system prompt) Claude follows.

    ---
    name: Drawing Review          # shown in the nav + header
    emoji: 📐
    order: 1                      # sort position (optional; lower = earlier)
    type: workshop                # "workshop" (files in/out) | "prompt" (text). default: workshop
    blurb: "One-line description."
    deliverable: "What the user gets back."
    instructions_placeholder: "Placeholder for the instructions box."
    skill_id: xlsx                # optional: also attach an Anthropic hosted skill
    inputs:                       # upload zones — keys must be `review` and/or `reference`
      - key: review
        label: "Files to review"
        help: "..."
        accept: .pdf,.png         # comma-separated extensions, or omit for any
        required: true            # default false
        multiple: true            # default true
      - key: reference
        label: "Reference files (optional)"
        help: "..."
    starters:                     # example instructions offered in the UI
      - "..."
    ---

    You are a senior engineering drawing reviewer ...   <- the instructions body

To add a skill: create a folder + SKILL.md, commit, and redeploy (or hit the
"Reload" button on a persistent host). The new tool appears automatically — no
Python, no other file to touch. A malformed SKILL.md is skipped with a warning
rather than breaking the app.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

SKILLS_DIR = Path(__file__).parent / "skills"
VALID_INPUT_KEYS = {"review", "reference"}

_FRONT_MATTER = re.compile(r"^---\s*\n(.*?)\n---\s*\n?(.*)$", re.DOTALL)

_SKILLS: list[dict] = []
_BY_ID: dict[str, dict] = {}


def _normalise_input(raw: dict) -> dict | None:
    key = str(raw.get("key", "")).strip()
    if key not in VALID_INPUT_KEYS:
        return None
    return {
        "key": key,
        "label": raw.get("label") or key.title(),
        "help": raw.get("help", "") or "",
        "accept": raw.get("accept", "") or "",
        "required": bool(raw.get("required", False)),
        "multiple": bool(raw.get("multiple", True)),
    }


def _parse_skill(folder: Path) -> dict | None:
    md = folder / "SKILL.md"
    if not md.exists():
        return None
    raw = md.read_text(encoding="utf-8")
    m = _FRONT_MATTER.match(raw)
    if m:
        meta = yaml.safe_load(m.group(1)) or {}
        body = m.group(2)
    else:
        meta = {}
        body = raw
    if not isinstance(meta, dict):
        raise ValueError("front matter is not a mapping")

    inputs = []
    for item in (meta.get("inputs") or []):
        if isinstance(item, dict):
            norm = _normalise_input(item)
            if norm:
                inputs.append(norm)

    return {
        "id": folder.name,
        "name": meta.get("name") or folder.name,
        "emoji": meta.get("emoji") or "🧩",
        "type": meta.get("type") or "workshop",
        "blurb": meta.get("blurb", "") or "",
        "deliverable": meta.get("deliverable", "") or "",
        "instructions_placeholder": meta.get("instructions_placeholder", "") or "",
        "inputs": inputs,
        "starters": list(meta.get("starters") or []),
        "skill_id": meta.get("skill_id"),
        "order": meta.get("order", 100),
        "system": body.strip(),
    }


def reload() -> int:
    """(Re)scan the skills folder. Returns the number of skills loaded."""
    global _SKILLS, _BY_ID
    loaded: list[dict] = []
    if SKILLS_DIR.exists():
        for folder in sorted(SKILLS_DIR.iterdir()):
            if not folder.is_dir():
                continue
            try:
                skill = _parse_skill(folder)
            except Exception as exc:  # noqa: BLE001 - one bad skill shouldn't kill the app
                print(f"[skills] skipping {folder.name}: {exc}", file=sys.stderr)
                continue
            if skill:
                loaded.append(skill)
    loaded.sort(key=lambda s: (s.get("order", 100), s["name"].lower()))
    _SKILLS = loaded
    _BY_ID = {s["id"]: s for s in loaded}
    return len(loaded)


def list_skills() -> list[dict]:
    """Public, safe-to-serialise view (no internal system prompts)."""
    return [
        {
            "id": s["id"],
            "name": s["name"],
            "emoji": s["emoji"],
            "type": s["type"],
            "blurb": s["blurb"],
            "deliverable": s["deliverable"],
            "inputs": s["inputs"],
            "instructions_placeholder": s["instructions_placeholder"],
            "starters": s["starters"],
            "produces_files": s["type"] == "workshop",
        }
        for s in _SKILLS
    ]


def get_skill(skill_id: str) -> dict | None:
    return _BY_ID.get(skill_id)


# Load on import.
reload()
