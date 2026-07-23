"""
Skill registry — the single place to define which Claude skills the team can use.

Add a new skill by appending a dict to SKILLS below. Two kinds are supported:

1. type="document"  — an Anthropic-hosted document skill (pptx, xlsx, docx, pdf).
   These run inside a code-execution container and can produce real, downloadable
   files (a .pptx deck, a .xlsx workbook, etc.).

2. type="prompt"    — a curated system prompt that turns Claude into a specialist
   (meeting minutes, an estimator, a reviewer). No file output; text answers only.
   This is the easy way to give the team a new "skill" without any API setup —
   just write a good system prompt.

Nothing else in the codebase needs to change when you add a skill here.
"""

from __future__ import annotations

# The Anthropic document skills available on the Messages API today.
# skill_id must match an Anthropic-provided skill id.
_DOCUMENT_SKILLS = [
    {
        "id": "pptx",
        "skill_id": "pptx",
        "name": "PowerPoint Builder",
        "emoji": "📊",
        "type": "document",
        "blurb": "Turn notes, an outline, or a topic into a polished slide deck (.pptx).",
        "starters": [
            "Build a 6-slide overview deck on our Q3 project status.",
            "Turn these bullet points into a clean pitch deck: ...",
        ],
    },
    {
        "id": "xlsx",
        "skill_id": "xlsx",
        "name": "Excel Builder",
        "emoji": "📈",
        "type": "document",
        "blurb": "Create spreadsheets, models, and formatted tables (.xlsx) with formulas and charts.",
        "starters": [
            "Build a simple project budget tracker with monthly columns and totals.",
            "Create a spreadsheet that models a 5-year cash flow from this data: ...",
        ],
    },
    {
        "id": "docx",
        "skill_id": "docx",
        "name": "Word Document Writer",
        "emoji": "📄",
        "type": "document",
        "blurb": "Produce formatted Word documents (.docx) — reports, letters, memos, templates.",
        "starters": [
            "Write a one-page project status report as a Word document.",
            "Draft a formal letter to a client about a schedule change.",
        ],
    },
    {
        "id": "pdf",
        "skill_id": "pdf",
        "name": "PDF Toolkit",
        "emoji": "📕",
        "type": "document",
        "blurb": "Generate, fill, and manipulate PDFs — reports, forms, merged documents.",
        "starters": [
            "Create a clean one-page PDF summary from these notes: ...",
            "Turn this content into a printable PDF handout.",
        ],
    },
]

# Prompt-only specialists. Add your own by copying an entry and rewriting `system`.
_PROMPT_SKILLS = [
    {
        "id": "assistant",
        "name": "General Assistant",
        "emoji": "💬",
        "type": "prompt",
        "blurb": "A capable general-purpose assistant for questions, drafting, and analysis.",
        "system": (
            "You are a helpful, precise assistant for a professional team. "
            "Give clear, well-structured answers. When a task is ambiguous, state your "
            "assumptions briefly and proceed rather than stalling."
        ),
        "starters": [
            "Summarise this email thread and suggest a reply.",
            "Explain the trade-offs between two approaches to ...",
        ],
    },
    {
        "id": "minutes",
        "name": "Meeting Minutes",
        "emoji": "📝",
        "type": "prompt",
        "blurb": "Paste rough notes or a transcript; get clean, structured minutes with actions.",
        "system": (
            "You are a meeting-minutes specialist. Given raw notes, an agenda, or a "
            "transcript, produce well-structured minutes with these sections: "
            "**Attendees**, **Summary**, **Decisions**, **Discussion** (grouped by topic), "
            "and **Action Items** (as a table with Owner and Due date where known). "
            "Be faithful to the source — never invent attendees, decisions, or dates. "
            "If information is missing, mark it as 'TBC' rather than guessing."
        ),
        "starters": [
            "Turn these rough notes into minutes: ...",
            "Extract just the action items from this transcript.",
        ],
    },
    {
        "id": "estimator",
        "name": "Engineering Estimator",
        "emoji": "📐",
        "type": "prompt",
        "blurb": "Sanity-check scope, build deliverables lists, and reason about engineering hours.",
        "system": (
            "You are an experienced engineering estimating specialist for industrial and "
            "minerals-processing projects. Help the team break scope into deliverables by "
            "function (ENG/DFT/MAN), discipline, and type; reason about hours per "
            "deliverable; and flag missing scope. Always show your assumptions and the basis "
            "for any hours figure. When you give numbers, present them in a clear table and "
            "note that they are indicative and require review by the responsible engineer."
        ),
        "starters": [
            "Draft a deliverables list for a small conveyor transfer upgrade study.",
            "Roughly how many hours for the mechanical scope of a 3-tank leach circuit?",
        ],
    },
    {
        "id": "reviewer",
        "name": "Document Reviewer",
        "emoji": "🔎",
        "type": "prompt",
        "blurb": "Paste a document or spec; get a structured review of clarity, gaps, and risks.",
        "system": (
            "You are a critical but constructive reviewer of technical and business documents. "
            "Review the provided text for: clarity, completeness, internal consistency, "
            "unstated assumptions, and risks. Return your findings as a prioritised list "
            "(High / Medium / Low), each with a short quote or location and a concrete "
            "suggested fix. End with a one-line overall verdict."
        ),
        "starters": [
            "Review this scope-of-work section for gaps and ambiguity: ...",
            "Check this email to a client for tone and anything risky before I send it.",
        ],
    },
]

SKILLS = _DOCUMENT_SKILLS + _PROMPT_SKILLS

_BY_ID = {s["id"]: s for s in SKILLS}


def list_skills() -> list[dict]:
    """Public, safe-to-serialise view of the skills (no internal system prompts)."""
    out = []
    for s in SKILLS:
        out.append(
            {
                "id": s["id"],
                "name": s["name"],
                "emoji": s["emoji"],
                "type": s["type"],
                "blurb": s["blurb"],
                "starters": s.get("starters", []),
                "produces_files": s["type"] == "document",
            }
        )
    return out


def get_skill(skill_id: str) -> dict | None:
    return _BY_ID.get(skill_id)
