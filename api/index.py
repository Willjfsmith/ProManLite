"""
Vercel entry point.

Vercel serves the ASGI `app` exported from this file. The real app lives at the
repo root; we add the root to sys.path and re-export it. Path resolution for
index.html and skills/ happens relative to each module's own __file__ (repo
root), so it works whether launched here or via `python main.py` locally.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app  # noqa: E402,F401
