from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Optional

import requests


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:24]


def download_to_cache(url: str, cache_dir: str, *, timeout_s: int = 60) -> str:
    """
    Download a URL to a deterministic cache filename.
    Returns local file path.
    """
    ensure_dir(cache_dir)
    ext = os.path.splitext(url.split("?")[0])[1] or ".bin"
    fp = os.path.join(cache_dir, f"{_hash(url)}{ext}")

    if os.path.exists(fp) and os.path.getsize(fp) > 0:
        return fp

    headers = {"User-Agent": "Mozilla/5.0 (LCA App; +https://streamlit.io)"}
    r = requests.get(url, headers=headers, timeout=timeout_s)
    r.raise_for_status()

    with open(fp, "wb") as f:
        f.write(r.content)

    return fp


def try_download(urls: list[str], cache_dir: str) -> Optional[str]:
    for u in urls:
        try:
            return download_to_cache(u, cache_dir)
        except Exception:
            continue
    return None
