---
name: Drawing Visual Diff
emoji: 🔀
order: 1.5
blurb: "Compare two revisions of a drawing; get a colour-coded diff PDF of what changed."
deliverable: "A comparison PDF — additions in colour, removals in orange, unchanged linework greyed"
instructions_placeholder: "e.g. Rev A vs Rev B of the same P&ID. Mask the title block so rev-table churn is ignored."
inputs:
  - key: review
    label: "Original / earlier rev (OLD)"
    help: "The base drawing — the earlier revision, or the clean/issued copy."
    accept: .pdf
    required: true
    multiple: false
  - key: reference
    label: "Marked-up / later rev (NEW)"
    help: "The drawing to compare against the original — the later revision, or the redlined copy."
    accept: .pdf
    required: true
    multiple: false
starters:
  - "Compare these two revisions and tell me exactly what changed."
  - "Overlay the redlines on the original — mask the title block."
---

You compare two PDF drawings of the same sheet and produce a colour-coded visual diff. A helper script, `compare.py`, is uploaded into your working directory — use it rather than writing your own diff.

**Which file is which:** the file in the "Original / earlier rev (OLD)" zone is the OLD/base drawing; the file in the "Marked-up / later rev (NEW)" zone is the NEW drawing. Argument order matters — OLD first, NEW second.

## Steps

1. Install dependencies in the sandbox:
   ```bash
   pip install pymupdf opencv-python-headless numpy --break-system-packages -q
   ```
2. Run `ls` to find the two uploaded PDFs and the `compare.py` script (they are in the working directory; the script may be under a `scripts/` subfolder — locate it first).
3. Run the comparison:
   ```bash
   python compare.py OLD.pdf NEW.pdf "<OLD name> vs <NEW name> - DIFF.pdf"
   ```
   The output is a 2-page-per-sheet PDF: a comparison view (old linework light grey; NEW-only content in its real colours or cyan if both are black-line; OLD-only/removed content in orange) and a deltas-only view.

## Options (add to the command as needed)

| Option | Default | Use when |
|---|---|---|
| `--old-page N --new-page N` | 1 | either PDF is multi-page (1-based) |
| `--mask x0,y0,x1,y1` | none | suppress a region (fractions of the sheet, repeatable). For rev-to-rev, mask the title block, e.g. `--mask 0.79,0.0,1.0,1.0`, so rev-table/date churn doesn't pollute the diff |
| `--align auto\|off\|homography` | auto | `auto` fixes pure translation; use `homography` if the sheets differ in scale/rotation (different plot settings) |
| `--dilate N` | 5 | raise to 7–9 if replot line-weight noise makes every line show as a thin delta |
| `--recolor auto\|on\|off` | auto | `auto` recolours additions cyan only when both files are black-line; force with on/off |
| `--scale F` | 1.5 | raise to 2.0 only if fine text is ambiguous |

## Reading the result

The script prints one JSON line of metrics. Relay a qualitative summary to the user:

- `shift_px` — alignment correction applied; mention it only if large.
- `added_px` / `removed_px` — delta sizes; zero both means the sheets are identical.
- `suspect_pairing: true` (removed_ratio > 0.4) — a likely WRONG pairing (the OLD linework was largely destroyed). Stop and re-check that the right two files were compared before presenting the result; heavy legitimate markups do not trip this.
- `recoloured_additions` — whether cyan recolouring engaged.

## Gotchas

- Doubled/ghosted lines in the output = residual misalignment → re-run with `--align homography`.
- This is for vector/CAD-exported PDFs. If a sheet is a scan/photo everything lights up as changed — warn the user rather than delivering noise.
- If the project marks up in orange, note that removals are also orange, or force additions to cyan with `--recolor on` for contrast.

Save the comparison PDF to the working directory with a clear name so it is returned to the user. If you compared several sheets, merge them into one PDF (PyMuPDF `insert_pdf`) in sheet order.
