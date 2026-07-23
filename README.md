# Team Skills

A lightweight web front end that lets your wider team interact with specific
Claude skills — without needing the API, the terminal, or any setup of their own.

Someone opens the page, picks a skill from the sidebar, and chats. Document
skills (PowerPoint, Excel, Word, PDF) hand back real, downloadable files.

## What's in the box

| Skill | Type | What it does |
|-------|------|--------------|
| PowerPoint Builder | document | Generates `.pptx` decks |
| Excel Builder | document | Generates `.xlsx` workbooks with formulas/charts |
| Word Document Writer | document | Generates `.docx` reports, letters, memos |
| PDF Toolkit | document | Generates and manipulates PDFs |
| General Assistant | prompt | A capable everyday assistant |
| Meeting Minutes | prompt | Rough notes → structured minutes + actions |
| Engineering Estimator | prompt | Deliverables lists & indicative hours |
| Document Reviewer | prompt | Structured review of a doc, spec, or email |

Everything runs on **Claude Opus 4.8**. Document skills use Anthropic's hosted
skills running in a code-execution container, so the files are produced by
Claude and streamed back for download.

## Architecture

```
index.html   → single-page UI (no build step, vanilla JS)
main.py      → FastAPI server: serves the UI + a streaming chat API (SSE)
runner.py    → wraps the Anthropic SDK: streaming, skill containers, file capture
prompts.py   → the skill registry — the one file you edit to add/curate skills
```

No database and no login by design — it's meant to run behind your own network,
VPN, or SSO (e.g. an internal load balancer, Cloudflare Access, or an
authenticating reverse proxy). Conversations are held in memory per browser
session and are lost on restart.

## Run it locally

You need an Anthropic API key: https://console.anthropic.com/

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...
python main.py            # or: uvicorn main:app --reload --port 8000
```

Open http://localhost:8000

If the key isn't set, the app still loads and shows a banner — it just can't chat
until a key is present.

## Run it with Docker

```bash
docker build -t team-skills .
docker run -e ANTHROPIC_API_KEY=sk-ant-... -p 8000:8000 team-skills
```

## Deploy

It's a standard ASGI app, so it runs anywhere Python does — a VM with
`uvicorn`/`gunicorn`, a container platform (Cloud Run, ECS, Fly.io, Render), or
behind nginx. Two things to remember:

1. Set `ANTHROPIC_API_KEY` in the environment.
2. **Put an auth layer in front of it** before exposing it to the team, since the
   app itself is unauthenticated.

For higher traffic, run multiple workers
(`uvicorn main:app --workers 4`). Note that in-memory conversation state is
per-worker, so use sticky sessions or a single worker if continuous multi-turn
threads matter; each turn still works fine without stickiness.

## Add or change a skill

Open `prompts.py` and add an entry to `SKILLS`.

**A prompt specialist** (text only) — just write a good system prompt:

```python
{
    "id": "safety",
    "name": "Safety Reviewer",
    "emoji": "🦺",
    "type": "prompt",
    "blurb": "Reviews method statements for HSE gaps.",
    "system": "You are an HSE specialist. Review the provided ...",
    "starters": ["Review this JSA for missing controls: ..."],
}
```

**A document skill** (produces files) — point at an Anthropic skill id:

```python
{
    "id": "xlsx",
    "skill_id": "xlsx",
    "name": "Excel Builder",
    "emoji": "📈",
    "type": "document",
    "blurb": "Builds spreadsheets and models.",
    "starters": ["Build a budget tracker ..."],
}
```

No other file needs to change — the UI and API pick it up automatically.

## Cost & tuning

The model, token limits, and beta flags live at the top of `runner.py`. Document
skills use more tokens than prompt skills because Claude writes and runs code to
build the file. Adjust `MODEL` / `MAX_TOKENS` there if you want a cheaper or
faster configuration for high-volume use.
