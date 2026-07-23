"""
Skill registry — the single place to define the tools the team can use.

Most skills here are "workshop" skills: the team uploads files (things to
review, plus optional reference material), adds context/instructions, and
Claude returns files (a marked-up PDF, a review register, an extracted
spreadsheet, ...) along with a short summary. These run in a code-execution
sandbox on the Claude API.

A skill entry:

    {
      "id":   "drawing-review",         # url-safe id
      "name": "Drawing Review",
      "emoji": "📐",
      "type": "workshop",               # "workshop" (files in/out) | "prompt" (text)
      "blurb": "One-line description shown in the nav and header.",
      "deliverable": "Marked-up PDFs + a review register (.xlsx)",
      "inputs": [                       # which upload zones to show (workshop only)
        {"key": "review",   "label": "...", "help": "...", "accept": ".pdf,.png", "required": True,  "multiple": True},
        {"key": "reference","label": "...", "help": "...", "accept": "",          "required": False, "multiple": True},
      ],
      "skill_id": "xlsx",               # optional: also attach an Anthropic hosted skill
      "instructions_placeholder": "e.g. focus on ...",
      "system": "The specialist instructions that reproduce the deliverable.",
      "starters": ["example instruction", ...],
    }

Only `review` and `reference` are valid input keys (the two upload zones the
backend accepts). To add a skill, copy an entry and rewrite it — nothing else
in the codebase needs to change.
"""

from __future__ import annotations

# Reusable input-zone definitions -------------------------------------------
def _zone(key, label, help, accept="", required=False, multiple=True):
    return {"key": key, "label": label, "help": help, "accept": accept,
            "required": required, "multiple": multiple}


_REVIEW_DRAWINGS = _zone(
    "review", "Files to review",
    "The drawings/documents to be reviewed — PDF or image.",
    accept=".pdf,.png,.jpg,.jpeg,.tif,.tiff", required=True,
)
_REFERENCE = _zone(
    "reference", "Reference files (optional)",
    "Standards, specs, a previous markup, a register template — anything Claude should follow.",
    accept="", required=False,
)


# The real drawing-review markup approach: PyMuPDF, numbered red markers +
# a "REVIEW COMMENTS" panel whose height hugs its wrapped content. Handed to
# Claude verbatim so the sandbox reproduces the packaged skill's output.
_MARKUP_SNIPPET = '''\
import fitz, textwrap
RED=(0.85,0.08,0.08); WHITE=(1,1,1); DARK=(0.1,0.1,0.1)

# comments = [(marker_fx, marker_fy, "comment text"), ...]  fractions of page W/H
# panel = (x0_frac, y0_frac, x1_frac)  -> height auto from content
def mark_sheet(page, comments, panel):
    W,H = page.rect.width, page.rect.height
    X0,X1,Y0 = panel[0]*W, panel[2]*W, panel[1]*H
    hh, fs, pad = 20, 8.5, 6
    lh = fs*1.30
    chars = int((X1-X0-30)/(fs*0.50))
    wrapped = [textwrap.wrap(t, chars) or [''] for _,_,t in comments]
    total = hh + pad + sum(len(w)*lh + 5 for w in wrapped) + pad
    R = fitz.Rect(X0, Y0, X1, Y0+total)
    page.draw_rect(R, color=RED, fill=WHITE, width=1.6, fill_opacity=0.9)
    page.draw_rect(fitz.Rect(R.x0,R.y0,R.x1,R.y0+hh), color=RED, fill=RED, width=0)
    page.insert_textbox(fitz.Rect(R.x0+6,R.y0+4,R.x1-4,R.y0+hh),
        "REVIEW COMMENTS  -  General review (not for construction)",
        fontname="hebo", fontsize=8.5, color=WHITE)
    y = R.y0+hh+pad
    for i, lines in enumerate(wrapped, 1):
        boxh = len(lines)*lh; by = y+lh*0.42
        page.draw_circle((R.x0+12,by), 7, color=WHITE, fill=RED, width=0)
        page.insert_text((R.x0+(9.2 if i<10 else 6.5),by+3.1), str(i),
                         fontname="hebo", fontsize=8.5, color=WHITE)
        page.insert_textbox(fitz.Rect(R.x0+24,y-1,R.x1-6,y+boxh+6),
                            comments[i-1][2], fontname="helv", fontsize=fs, color=DARK)
        y += boxh + 5
    for i,(fx,fy,_) in enumerate(comments, 1):
        cx,cy = fx*W, fy*H
        page.draw_circle((cx,cy), 11, color=WHITE, fill=RED, width=1.2)
        page.insert_text((cx-(3.4 if i<10 else 6.8),cy+4.2), str(i),
                         fontname="hebo", fontsize=12, color=WHITE)

doc = fitz.open("drawing.pdf")
mark_sheet(doc[0], comments, panel)
doc.save("drawing (REVIEW MARKUP).pdf")
'''


SKILLS = [
    {
        "id": "drawing-review",
        "name": "Drawing Review",
        "emoji": "📐",
        "type": "workshop",
        "blurb": "Upload drawings; get marked-up PDFs with numbered comments stamped on the sheets.",
        "deliverable": "Marked-up PDF(s) — numbered red markers + a REVIEW COMMENTS panel per sheet",
        "inputs": [_REVIEW_DRAWINGS, _REFERENCE],
        "instructions_placeholder": "e.g. General good-practice civil/earthworks review. (Say 'also give me a register' if you want a spreadsheet as well.)",
        "system": (
            "You are a senior engineering drawing reviewer (civil, structural, mechanical, "
            "P&ID, earthworks).\n\n"
            "DELIVERABLE — the default and primary output is MARKED-UP PDFs, one per input "
            "sheet, NOT a spreadsheet. Only ALSO build a comment register (.xlsx) if the "
            "user's instructions explicitly ask for one.\n\n"
            "For each uploaded drawing:\n"
            "1. Read the sheet carefully from the images shown to you. CAD-exported drawings "
            "are usually vectorised, so text extraction is unreliable — rely on vision, and "
            "look closely at the title block, notes, quantity tables and small callouts. Use "
            "pdfplumber/PyMuPDF text extraction only as a backstop.\n"
            "2. Find review comments across these lenses:\n"
            "   - Document control: title-block number vs file name, revision/status, blank "
            "Drawn/Checked/Designed/Approved fields, section & detail cross-references.\n"
            "   - Setout / survey: datum, coordinate system/zone placeholders (e.g. 'Zone XX'), "
            "missing setout tables, site-confirm/LiDAR holds.\n"
            "   - Quantities: spurious precision, cut/fill balance, missing strip/bulking/"
            "compaction allowances, consistency of volumes with notes.\n"
            "   - Drafting: cross-reference sweep, label legibility, north/scale.\n"
            "   - Design/geometry: grades, batters (ratio vs %), freeboard/levels, widths vs "
            "design vehicle, liner/subgrade/anchor consistency.\n"
            "   - Spellcheck: check ALL text on every sheet — notes, labels, legends, title "
            "block, callouts, table headers, revision descriptions. Flag typos (e.g. "
            "'CRITITAL' -> 'CRITICAL'). Do NOT flag valid engineering abbreviations, tags, "
            "drawing numbers or unit symbols. Add each spelling issue as its own numbered "
            "marker on the offending text.\n"
            "   Only assert an error once you have confirmed it; otherwise phrase the comment "
            "as 'Confirm ...'.\n"
            "3. Stamp the markup with PyMuPDF so vector content is PRESERVED (do NOT rasterise "
            "the whole sheet). First run `pip install pymupdf` in the sandbox. Place a numbered "
            "red circle marker on each feature being commented on, and a red-bordered "
            "'REVIEW COMMENTS' panel in a genuinely empty area of the sheet with the matching "
            "numbered commentary. Keep marker numbering consistent between the on-drawing "
            "markers and the panel. Marker positions are fractions of page width/height read "
            "off the sheet. Use this reference implementation:\n\n```python\n"
            + _MARKUP_SNIPPET +
            "```\n\n"
            "If PyMuPDF cannot be installed, fall back to a vector overlay built with reportlab "
            "and merged onto the original with pypdf (keeps the base drawing's vectors).\n"
            "4. Save each output as '<original name> (REVIEW MARKUP).pdf', keeping the drawing "
            "number. If a file can't be overwritten, save under a fresh name rather than "
            "failing.\n\n"
            "After producing the files, give a short written summary of the headline issues and "
            "offer a spreadsheet register as an optional extra. Use ratios (1:2 / 1:3) and "
            "standard units in commentary to match drawing notes. If a reference file defines a "
            "design basis, spec or drafting standard, review against it."
        ),
        "starters": [
            "General good-practice review — flag anything that would cause an RFI or rework.",
            "Structural focus: check connections, member sizes, and load notes.",
        ],
    },
    {
        "id": "drawing-register",
        "name": "Drawing Register",
        "emoji": "🗂️",
        "type": "workshop",
        "skill_id": "xlsx",
        "blurb": "Upload a folder of drawings; get a title-block register spreadsheet.",
        "deliverable": "A drawing register (.xlsx)",
        "inputs": [
            _zone("review", "Drawings", "The drawing PDFs to index.",
                  accept=".pdf,.png,.jpg,.jpeg", required=True),
            _REFERENCE,
        ],
        "instructions_placeholder": "e.g. Also pull the scale and sheet size. Sort by discipline then drawing number.",
        "system": (
            "You are a drafting/document-control specialist. From each uploaded drawing, read "
            "the title block and extract: Project, Discipline, Area, Drawing number, Title, "
            "Revision, Scale, Sheet size, Sheet x of y. Read the images shown to you; if a title "
            "block is vectorised and unreadable, note that rather than guessing. Build a clean, "
            "sorted drawing register as an .xlsx with one row per sheet and a sensible column "
            "order. Save it as a file."
        ),
        "starters": [
            "Build a register from these drawings, sorted by drawing number.",
        ],
    },
    {
        "id": "pid-extract",
        "name": "P&ID Tag Extract",
        "emoji": "🔧",
        "type": "workshop",
        "skill_id": "xlsx",
        "blurb": "Upload P&IDs; get a categorised tag / line / equipment register.",
        "deliverable": "A categorised register (.xlsx) — equipment, instruments, valves, lines",
        "inputs": [
            _zone("review", "P&IDs", "The P&ID PDFs.", accept=".pdf,.png,.jpg,.jpeg", required=True),
            _REFERENCE,
        ],
        "instructions_placeholder": "e.g. Include off-page references and title-block data. One sheet per category.",
        "system": (
            "You are a P&ID metadata specialist. From each uploaded P&ID, read the drawing and "
            "extract, categorised: equipment numbers/names, instrument tags, valve tags, line "
            "numbers, off-page references, and title-block data. Work from the images shown. "
            "Build an .xlsx register with a separate sheet per category (Equipment, Instruments, "
            "Valves, Lines, Off-page refs) plus a Title-block sheet. Include the source drawing "
            "number on every row. Do not invent tags — only list what you can read. Save the file."
        ),
        "starters": [
            "Extract all tags and build a line list and instrument index.",
        ],
    },
    {
        "id": "doc-review",
        "name": "Document Reviewer",
        "emoji": "🔎",
        "type": "workshop",
        "skill_id": "xlsx",
        "blurb": "Upload a document or spec; get a review with comments and a findings register.",
        "deliverable": "A findings register (.xlsx) + a written review summary",
        "inputs": [
            _zone("review", "Document to review", "The spec, report, scope, or proposal.",
                  accept=".pdf,.docx,.txt,.md", required=True),
            _zone("reference", "Standards / templates (optional)", "Anything the document should comply with.",
                  accept="", required=False),
        ],
        "instructions_placeholder": "e.g. Check for scope gaps, ambiguity, and commercial risk. Client-facing.",
        "system": (
            "You are a critical but constructive reviewer of technical and commercial documents. "
            "Review the uploaded document for clarity, completeness, internal consistency, "
            "unstated assumptions, and risk. Produce a findings register (.xlsx) with columns: "
            "No., Section/Location, Finding, Severity (High/Med/Low), Suggested fix. Then give a "
            "short written summary with an overall verdict. If reference standards are provided, "
            "check compliance against them. Save the register as a file."
        ),
        "starters": [
            "Review this scope of work for gaps, ambiguity, and risk.",
        ],
    },
    {
        "id": "minutes",
        "name": "Meeting Minutes",
        "emoji": "📝",
        "type": "workshop",
        "skill_id": "docx",
        "blurb": "Upload notes or a transcript; get formatted minutes as a Word document.",
        "deliverable": "Formatted minutes (.docx)",
        "inputs": [
            _zone("review", "Notes / transcript", "Rough notes, an agenda, or a transcript.",
                  accept=".pdf,.docx,.txt,.md", required=False),
            _zone("reference", "Template (optional)", "A minutes template to match.",
                  accept="", required=False),
        ],
        "instructions_placeholder": "Paste notes here if you didn't upload a file, or add context (attendees, date, project).",
        "system": (
            "You are a meeting-minutes specialist. From the uploaded notes/transcript (and any "
            "pasted context), produce well-structured minutes as a Word document with sections: "
            "Attendees, Summary, Decisions, Discussion (grouped by topic), and Action Items (a "
            "table with Owner and Due date). Be faithful to the source — never invent attendees, "
            "decisions, or dates; mark unknowns as 'TBC'. Save the .docx file."
        ),
        "starters": [
            "Turn these notes into minutes and pull out the action items.",
        ],
    },
    {
        "id": "estimator",
        "name": "Engineering Estimator",
        "emoji": "📊",
        "type": "workshop",
        "skill_id": "xlsx",
        "blurb": "Upload a scope or register; get an indicative deliverables & hours estimate.",
        "deliverable": "A deliverables & hours estimate (.xlsx) + basis notes",
        "inputs": [
            _zone("review", "Scope / register", "A scope description, deliverables register, or scorecard.",
                  accept=".pdf,.docx,.txt,.md,.xlsx,.csv", required=False),
            _REFERENCE,
        ],
        "instructions_placeholder": "e.g. Small conveyor transfer upgrade study. Mechanical + structural + E&I.",
        "system": (
            "You are an experienced engineering estimating specialist for industrial and "
            "minerals-processing projects. From the uploaded scope/register (and any context), "
            "build a deliverables list broken down by function (ENG/DFT/MAN), discipline, and "
            "type, with indicative hours per deliverable and a clearly stated basis. Produce an "
            ".xlsx with the deliverables and hours, plus a written basis-of-estimate summary. "
            "Always show assumptions. State clearly that hours are indicative and require review "
            "by the responsible engineer. Save the .xlsx file."
        ),
        "starters": [
            "Draft a deliverables list and indicative hours for a 3-tank leach circuit.",
        ],
    },
    {
        "id": "assistant",
        "name": "General Assistant",
        "emoji": "💬",
        "type": "prompt",
        "blurb": "Ask a question or attach a file for a quick text answer — no file output.",
        "deliverable": "A written answer",
        "inputs": [
            _zone("review", "Attachments (optional)", "A file to ask about.", accept="", required=False),
        ],
        "instructions_placeholder": "Ask anything, or attach a file and ask about it…",
        "system": (
            "You are a helpful, precise assistant for a professional engineering team. Give "
            "clear, well-structured answers. If a file is attached, use it. When a task is "
            "ambiguous, state your assumptions briefly and proceed."
        ),
        "starters": [
            "Summarise the attached document and list the key risks.",
            "Explain the trade-offs between two approaches to ...",
        ],
    },
]

_BY_ID = {s["id"]: s for s in SKILLS}


def list_skills() -> list[dict]:
    """Public, safe-to-serialise view (no internal system prompts)."""
    out = []
    for s in SKILLS:
        out.append(
            {
                "id": s["id"],
                "name": s["name"],
                "emoji": s["emoji"],
                "type": s["type"],
                "blurb": s["blurb"],
                "deliverable": s.get("deliverable", ""),
                "inputs": s.get("inputs", []),
                "instructions_placeholder": s.get("instructions_placeholder", ""),
                "starters": s.get("starters", []),
                "produces_files": s["type"] == "workshop",
            }
        )
    return out


def get_skill(skill_id: str) -> dict | None:
    return _BY_ID.get(skill_id)
