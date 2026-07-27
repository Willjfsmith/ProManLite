#!/usr/bin/env python3
"""
drawing-visual-diff: compare any two PDF drawings of the same sheet visually.

Output: 2-page PDF per sheet
  page 1 - base drawing greyed; ADDED content overlaid (original colours, or
           recoloured cyan if monochrome); REMOVED content in orange
  page 2 - deltas only on white

Features over v1:
  - auto-alignment (translation via phase correlation; optional homography via ORB)
  - region masking (e.g. title block / rev table) to suppress known-noise areas
  - monochrome recolouring: if added content is essentially black-on-black,
    additions are recoloured CYAN so they are visible against the grey base
  - quality metrics printed as one JSON line for batch reporting

Usage:
  python compare.py OLD.pdf NEW.pdf OUT.pdf [options]

Options:
  --old-page N / --new-page N   1-based page selection (default 1)
  --scale F                     render scale (default 1.5)
  --mask x0,y0,x1,y1            fractional rect(s) to ignore, repeatable.
                                e.g. --mask 0.79,0.0,1.0,1.0  (right title strip)
  --align {auto,off,homography} default auto: translation fix if shift detected;
                                homography adds rotation/scale correction (ORB)
  --dilate N                    match tolerance in px (default 5; raise for
                                replot line-weight noise)
  --recolor {auto,on,off}       recolour additions cyan (auto: only if additions
                                are monochrome/black)

Requires: pymupdf, opencv-python(-headless), numpy
"""
import argparse
import json
import sys

import cv2
import fitz
import numpy as np

INK_THRESH = 225
MIN_SPECK_AREA = 6
ORANGE = (255, 140, 0)
CYAN = (0, 160, 200)
GREY = (205, 205, 205)


def render(path, page_no, scale, target_size=None):
    doc = fitz.open(path)
    if page_no < 1 or page_no > len(doc):
        sys.exit(f"{path} has {len(doc)} page(s); requested page {page_no}")
    pix = doc[page_no - 1].get_pixmap(matrix=fitz.Matrix(scale, scale), annots=True)
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)[:, :, :3].copy()
    rect = doc[page_no - 1].rect
    doc.close()
    if target_size is not None and (img.shape[1], img.shape[0]) != target_size:
        img = cv2.resize(img, target_size, interpolation=cv2.INTER_AREA)
    return img, (rect.width, rect.height)


def ink(img):
    return img.min(axis=2) < INK_THRESH


def clean(mask):
    n, lab, stats, _ = cv2.connectedComponentsWithStats(mask.astype(np.uint8), 8)
    keep = np.zeros(n, bool)
    if n > 1:
        keep[1:] = stats[1:, cv2.CC_STAT_AREA] >= MIN_SPECK_AREA
    return keep[lab]


def align(old_img, new_img, mode):
    """Return new_img warped onto old_img, plus metrics dict."""
    m = {"align_mode": mode, "shift_px": [0.0, 0.0], "homography": False}
    if mode == "off":
        return new_img, m
    g_old = cv2.cvtColor(old_img, cv2.COLOR_RGB2GRAY).astype(np.float32)
    g_new = cv2.cvtColor(new_img, cv2.COLOR_RGB2GRAY).astype(np.float32)
    (dx, dy), _ = cv2.phaseCorrelate(g_new, g_old)
    m["shift_px"] = [round(dx, 2), round(dy, 2)]
    if mode == "homography":
        orb = cv2.ORB_create(4000)
        k1, d1 = orb.detectAndCompute(cv2.convertScaleAbs(g_new), None)
        k2, d2 = orb.detectAndCompute(cv2.convertScaleAbs(g_old), None)
        if d1 is not None and d2 is not None and len(k1) > 50 and len(k2) > 50:
            matches = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True).match(d1, d2)
            matches = sorted(matches, key=lambda x: x.distance)[:500]
            if len(matches) >= 20:
                src = np.float32([k1[x.queryIdx].pt for x in matches]).reshape(-1, 1, 2)
                dst = np.float32([k2[x.trainIdx].pt for x in matches]).reshape(-1, 1, 2)
                H, inliers = cv2.findHomography(src, dst, cv2.RANSAC, 5.0)
                if H is not None and inliers is not None and inliers.sum() >= 15:
                    m["homography"] = True
                    warped = cv2.warpPerspective(new_img, H, (old_img.shape[1], old_img.shape[0]),
                                                 borderValue=(255, 255, 255))
                    return warped, m
        # fall through to translation
    if abs(dx) >= 0.5 or abs(dy) >= 0.5:
        M = np.float32([[1, 0, dx], [0, 1, dy]])
        new_img = cv2.warpAffine(new_img, M, (old_img.shape[1], old_img.shape[0]),
                                 borderValue=(255, 255, 255))
    return new_img, m


def parse_masks(mask_args, h, w):
    rects = []
    for s in mask_args or []:
        x0, y0, x1, y1 = [float(v) for v in s.split(",")]
        rects.append((int(y0 * h), int(y1 * h), int(x0 * w), int(x1 * w)))
    return rects


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("old"); ap.add_argument("new"); ap.add_argument("output")
    ap.add_argument("--old-page", type=int, default=1)
    ap.add_argument("--new-page", type=int, default=1)
    ap.add_argument("--scale", type=float, default=1.5)
    ap.add_argument("--mask", action="append")
    ap.add_argument("--align", choices=["auto", "off", "homography"], default="auto")
    ap.add_argument("--dilate", type=int, default=5)
    ap.add_argument("--recolor", choices=["auto", "on", "off"], default="auto")
    a = ap.parse_args()

    old_img, page_wh = render(a.old, a.old_page, a.scale)
    new_img, _ = render(a.new, a.new_page, a.scale, target_size=(old_img.shape[1], old_img.shape[0]))
    new_img, metrics = align(old_img, new_img, a.align)

    mo, mn = ink(old_img), ink(new_img)
    h, w = mo.shape
    mask_rects = parse_masks(a.mask, h, w)
    ignore = np.zeros((h, w), bool)
    for (r0, r1, c0, c1) in mask_rects:
        ignore[r0:r1, c0:c1] = True

    k = np.ones((a.dilate, a.dilate), np.uint8)
    mo_d = cv2.dilate(mo.astype(np.uint8), k) > 0
    mn_d = cv2.dilate(mn.astype(np.uint8), k) > 0

    added = clean(mn & ~mo_d & ~ignore)
    removed = clean(mo & ~mn_d & ~ignore)

    # metrics
    metrics.update({
        "added_px": int(added.sum()),
        "removed_px": int(removed.sum()),
        "old_ink_px": int(mo.sum()),
        "diff_ratio": round(float(added.sum() + removed.sum()) / max(mo.sum(), 1), 3),
        "removed_ratio": round(float(removed.sum()) / max(mo.sum(), 1), 3),
    })
    # wrong pairing looks like most of the old sheet "removed" AND lots "added";
    # heavy but legitimate markups only add, so key off removals
    metrics["suspect_pairing"] = bool(metrics["removed_ratio"] > 0.4)

    # recolour decision: are additions essentially monochrome (dark grey/black)?
    recolor = a.recolor == "on"
    if a.recolor == "auto" and added.any():
        px = new_img[added].astype(int)
        sat = (px.max(axis=1) - px.min(axis=1))
        dark = px.max(axis=1) < 120
        recolor = bool(((sat < 30) & dark).mean() > 0.85)
    metrics["recoloured_additions"] = recolor

    p1 = np.full((h, w, 3), 255, np.uint8)
    p1[mo] = GREY
    if recolor:
        p1[added] = CYAN
    else:
        p1[added] = new_img[added]
    p1[removed] = ORANGE

    p2 = np.full((h, w, 3), 255, np.uint8)
    p2[added] = CYAN if recolor else new_img[added]
    p2[removed] = ORANGE

    # visualise masked regions faintly on page 1
    for (r0, r1, c0, c1) in mask_rects:
        cv2.rectangle(p1, (c0, r0), (c1 - 1, r1 - 1), (150, 150, 220), 2)

    add_lbl = "ADDED in cyan" if recolor else "ADDED in original colours"
    doc = fitz.open()
    labels = [
        f"VISUAL DIFF: base greyed | {add_lbl} | REMOVED in orange"
        + (" | masked regions outlined" if mask_rects else ""),
        "DELTAS ONLY (added" + (" in cyan" if recolor else "") + ", removed in orange)",
    ]
    for img, label in zip((p1, p2), labels):
        ok, buf = cv2.imencode(".png", cv2.cvtColor(img, cv2.COLOR_RGB2BGR))
        page = doc.new_page(width=page_wh[0], height=page_wh[1])
        page.insert_image(page.rect, stream=buf.tobytes())
        page.insert_textbox(fitz.Rect(15, 4, page_wh[0] - 15, 40), label,
                            fontsize=max(10, page_wh[0] / 150), color=(0.3, 0, 0.5))
    doc.save(a.output, deflate=True)
    print(json.dumps(metrics))
    if metrics["suspect_pairing"]:
        print("WARNING: diff_ratio > 0.6 - these are probably not the same sheet/revision. "
              "Check pairing before trusting this output.", file=sys.stderr)


if __name__ == "__main__":
    main()
