# Team Skills

A web front end that lets your wider team run file-based Claude workflows from a
browser — upload the files to review, add some reference material and
instructions, hit **Run**, and get files back (a marked-up PDF, a review
register, an extracted spreadsheet) plus a short summary.

No API keys, terminal, or setup for the people using it — they just open the
page and drop in files.

## The tools

| Tool | Upload | You get back |
|------|--------|--------------|
| Drawing Review | drawings (+ optional reference/standards) | marked-up PDF(s) + a review register `.xlsx` |
| Drawing Register | a set of drawings | a title-block register `.xlsx` |
| P&ID Tag Extract | P&IDs | a categorised tag/line/equipment register `.xlsx` |
| Document Reviewer | a spec/report (+ optional standards) | a findings register `.xlsx` + written review |
| Meeting Minutes | notes/transcript (or pasted text) | formatted minutes `.docx` |
| Engineering Estimator | a scope/register (+ context) | indicative deliverables & hours `.xlsx` |
| General Assistant | optional attachment | a written answer |

Each tool is a card with its own upload zones and instructions. Runs stack below
so you can compare or refine.

## How it works

Everything runs on **Claude Opus 4.8** over the Claude API:

1. Uploaded files go to the Anthropic **Files API**.
2. Claude is shown the files it can view (vision over PDFs and images) **and**
   given them inside a **code-execution sandbox** (which has `pypdf`,
   `reportlab`, `openpyxl`, `python-docx`, `pdfplumber`, `matplotlib`, `pillow`).
3. Claude does the work and saves deliverables as files.
4. Those files are captured and offered back as downloads.

Follow-up runs reuse the same sandbox, so "also flag the electrical clashes"
keeps your uploaded files in context.

### A note on the engineering skills

The deep engineering skills you may already use in Claude Code
(`drawing-review`, `pid-metadata-extract`, `drawing-titleblock`, …) are packaged
skills that run *inside Claude Code*, not on the API. This app reproduces their
**deliverables** via the code-execution approach above, driven by a tailored
instruction set per tool (see `prompts.py`). When you want to run your exact
packaged skills instead, upload them once via the Anthropic **Skills API** and
set `skill_id` on the matching entry in `prompts.py` — the rest of the app is
already wired for it (the two Anthropic document skills `xlsx`/`docx` are
attached this way today).

## Architecture

```
index.html   → single-page UI (upload zones, run cards, no build step)
main.py      → FastAPI server: serves the UI + a multipart run API (SSE stream)
runner.py    → Anthropic SDK: uploads, sandbox run, output-file capture
prompts.py   → the skill registry — the one file you edit to add/curate tools
```

No database and no login by design — run it behind your own network, VPN, or SSO
(an internal load balancer, Cloudflare Access, an authenticating proxy). Runs
and uploads are held in memory per browser session and cleared on restart.

## Run it locally

You need an Anthropic API key: https://console.anthropic.com/

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...
python main.py            # or: uvicorn main:app --reload --port 8000
```

Open http://localhost:8000 — without a key the page loads and shows a banner but
can't run.

## Run it with Docker

```bash
docker build -t team-skills .
docker run -e ANTHROPIC_API_KEY=sk-ant-... -p 8000:8000 team-skills
```

## Deploy

Standard ASGI app — runs on any VM (`uvicorn`/`gunicorn`), container platform
(Cloud Run, ECS, Fly.io, Render), or behind nginx. Two must-dos:

1. Set `ANTHROPIC_API_KEY`.
2. **Put an auth layer in front** before exposing it — the app is unauthenticated.

For higher traffic, run multiple workers. In-memory sandbox/run state is
per-worker, so use sticky sessions (or a single worker) if you rely on follow-up
runs reusing an earlier upload; each fresh run works fine without stickiness.

Upload limits (`MAX_FILES`, `MAX_TOTAL_BYTES`) are constants at the top of
`main.py`.

## Add or change a tool

Open `prompts.py` and add an entry to `SKILLS`. A file-based tool:

```python
{
    "id": "hazop-review",
    "name": "HAZOP Reviewer",
    "emoji": "🦺",
    "type": "workshop",
    "skill_id": "xlsx",                      # optional Anthropic hosted skill
    "blurb": "Upload a P&ID + node list; get a HAZOP worksheet.",
    "deliverable": "A HAZOP worksheet (.xlsx)",
    "inputs": [
        {"key": "review", "label": "P&IDs", "help": "The drawings.",
         "accept": ".pdf", "required": True, "multiple": True},
        {"key": "reference", "label": "Node list (optional)", "help": "",
         "accept": "", "required": False, "multiple": True},
    ],
    "instructions_placeholder": "e.g. Use guidewords No/More/Less on each node.",
    "system": "You are a HAZOP facilitator. From the uploaded P&IDs ...",
    "starters": ["Run a HAZOP on nodes 1–3 ..."],
}
```

Only `review` and `reference` are valid input keys (the two upload zones the
backend accepts). Nothing else needs to change — the UI and API pick it up.

## Cost & tuning

Model, token limits, and beta flags live at the top of `runner.py`. File
workflows use more tokens than a plain question because Claude reads the files
and writes/runs code to build the deliverable. Adjust `MODEL` / `MAX_TOKENS`
there for a cheaper or faster configuration.
