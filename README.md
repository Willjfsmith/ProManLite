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
| Drawing Review | drawings (+ optional reference/standards) | marked-up PDF(s) — numbered red markers + a REVIEW COMMENTS panel per sheet (register on request) |
| Drawing Register | a set of drawings | a title-block register `.xlsx` |
| P&ID Tag Extract | P&IDs | a categorised tag/line/equipment register `.xlsx` |
| Document Reviewer | a spec/report (+ optional standards) | a findings register `.xlsx` + written review |
| Meeting Minutes | notes/transcript (or pasted text) | formatted minutes `.docx` |
| Engineering Estimator | a scope/register (+ context) | indicative deliverables & hours `.xlsx` |
| General Assistant | optional attachment | a written answer |

Each tool is a card with its own upload zones and instructions. Runs stack below
so you can compare or refine.

## How it works

Runs go over the Claude API. You pick the model per run from a dropdown —
**Opus 4.8** (default, recommended for dense/vectorised P&IDs), **Sonnet 5**
(cheaper & faster), or **Haiku 4.5** (cheapest). The badge next to each option
and on every run card tells you what you used.

1. Uploaded files go to the Anthropic **Files API**.
2. Claude is shown the files it can view (vision over PDFs and images) **and**
   given them inside a **code-execution sandbox** (which has `pypdf`,
   `reportlab`, `openpyxl`, `python-docx`, `pdfplumber`, `matplotlib`, `pillow`).
3. Claude does the work and saves deliverables as files.
4. Those files are captured and offered back as downloads.

Follow-up runs reuse the same sandbox, so "also flag the electrical clashes"
keeps your uploaded files in context.

### Runs survive the tab closing

A run executes on a **background thread on the server**, not inside your browser
connection. So you can close the tab, switch tools, or lose wifi mid-run and it
keeps going. When you come back, the tool's **run history** reloads with every
past run and its **download links** intact, and any run still working reconnects
and keeps streaming. A **Stop** button cancels a run in flight. History is held
in memory, so it survives page reloads but not a server restart/redeploy.

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
index.html   → single-page UI (upload zones, model picker, run history, no build step)
main.py      → FastAPI server: serves the UI + the run API (start / list / stream / stop)
runner.py    → Anthropic SDK + the server-side run store: uploads, sandbox run, output-file capture
prompts.py   → loads the skills/ folder into the catalogue
skills/       → one folder per tool, each a SKILL.md (this is what you edit)
api/index.py → Vercel entry point (re-exports the app)
vercel.json  → Vercel config (routing + function timeout)
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

## Deploy on Vercel

The repo is Vercel-ready (`api/index.py` + `vercel.json`):

1. Push the repo to GitHub and **Import Project** in Vercel.
2. In **Settings → Environment Variables**, add `ANTHROPIC_API_KEY` (use the key
   from the **work** Anthropic org so runs bill there — nobody on the team needs
   their own account).
3. Deploy. Adding a skill later = commit a new `skills/<name>/SKILL.md` and let
   Vercel redeploy; the new page appears.

**Vercel-specific caveats** (worth knowing before you rely on it):

- **Use the Pro plan.** Vercel functions time out at 60s on Hobby and up to 300s
  on Pro (`maxDuration` in `vercel.json`). A drawing review with markup can run a
  few minutes, so Hobby will cut it off; even on Pro a large batch may be tight.
- **No live token streaming.** Vercel's Python runtime buffers the response, so
  the UI shows a spinner and then the finished result (files + summary) rather
  than streaming text as it's produced. Everything still works; it just isn't
  incremental.
- **No run history, reconnect, or Stop, and follow-ups don't reuse context.**
  Serverless invocations don't share memory or keep work alive past the response,
  so the app **auto-detects Vercel and falls back** to running each job inside its
  request: the run completes and returns its files, but you must keep the tab open
  until it finishes, and each run is independent (re-upload files for a follow-up).
  The model picker still works.

If those bite, the **Dockerfile** below runs the exact same app on a persistent
host (Railway, Render, Fly.io, Cloud Run, a VM) where live streaming, long runs,
follow-up context, **run history that survives closing the tab, reconnecting to a
run in progress, and the Stop button** all work — no code changes.

## Run it with Docker

```bash
docker build -t team-skills .
docker run -e ANTHROPIC_API_KEY=sk-ant-... -p 8000:8000 team-skills
```

## Deploy anywhere else

Standard ASGI app — runs on any VM (`uvicorn`/`gunicorn`) or container platform.
Two must-dos everywhere:

1. Set `ANTHROPIC_API_KEY`.
2. **Put an auth layer in front** before exposing it — the app is unauthenticated.

For higher traffic, run multiple workers. In-memory sandbox/run state is
per-worker, so use sticky sessions (or a single worker) if you rely on follow-up
runs reusing an earlier upload; each fresh run works fine without stickiness.

Upload limits (`MAX_FILES`, `MAX_TOTAL_BYTES`) are constants at the top of
`main.py`.

## Add or change a tool

Each tool is a folder under `skills/` containing a single `SKILL.md` — a short
YAML header plus the instructions. Create the folder (locally or straight in the
GitHub web editor), commit, and the new page appears on redeploy. No Python, no
UI code, and a malformed file just skips that one skill instead of breaking the
app.

```
skills/
  hazop/
    SKILL.md
```

```markdown
---
name: HAZOP Reviewer
emoji: 🦺
order: 8
skill_id: xlsx                 # optional: also attach an Anthropic hosted skill (xlsx/docx/pptx/pdf)
blurb: "Upload a P&ID + node list; get a HAZOP worksheet."
deliverable: "A HAZOP worksheet (.xlsx)"
instructions_placeholder: "e.g. Use guidewords No/More/Less on each node."
inputs:
  - key: review
    label: "P&IDs"
    help: "The drawings."
    accept: .pdf
    required: true
  - key: reference
    label: "Node list (optional)"
starters:
  - "Run a HAZOP on nodes 1–3 ..."
---

You are a HAZOP facilitator. From the uploaded P&IDs and node list, work
through each node using the guidewords No/More/Less/... and produce an .xlsx
worksheet with columns Node, Deviation, Cause, Consequence, Safeguard, Action.
Save the file.
```

**Header fields:** `name`, `emoji`, `blurb` (nav/header text), `deliverable`
(the "You'll get…" line), `order` (sort position), `type` (`workshop` for
files-in/files-out, the default, or `prompt` for a text answer), `skill_id`
(optional hosted skill), `instructions_placeholder`, `inputs`, and `starters`.
The body below the `---` is the instructions Claude follows.

**Input zones:** only `review` and `reference` are valid keys (the two upload
zones the backend accepts). Each takes `label`, `help`, `accept`
(comma-separated extensions, or omit for any), `required` (default false), and
`multiple` (default true).

On a persistent host, the **Reload** button in the sidebar re-scans `skills/`
without a restart. On Vercel, a redeploy picks up new skills.

### Bringing in your real packaged skills

If you already have a Claude Code / packaged `SKILL.md`, this format is close to
a copy-paste: move the front-matter fields across (or add the few this app
uses), and drop the instructions in as the body. For a skill that truly needs to
run *as a packaged skill* (bundled scripts, resources), upload it once to the
work org via the Anthropic **Skills API** and set `skill_id:` to its id.

## Cost & tuning

The model is chosen per run in the UI; the catalogue (and which one is the
default) lives in the `MODELS` / `DEFAULT_MODEL` constants at the top of
`runner.py` — edit those to add, remove, or re-label options. Token limits and
beta flags live alongside them. File workflows use more tokens than a plain
question because Claude reads the files and writes/runs code to build the
deliverable, so Sonnet or Haiku can be a big saving on simpler jobs; adjust
`MAX_TOKENS` there too for a cheaper or faster configuration.
