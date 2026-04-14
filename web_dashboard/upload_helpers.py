from __future__ import annotations

import re
from pathlib import Path


UPLOAD_TARGET_FOLDERS = {
    "sales": "All orders",
    "samples": "Samples",
    "replacements": "Replacements",
    "statements": "Finance Tab",
}


def sanitize_upload_filename(filename: str) -> str:
    name = Path(str(filename or "")).name.strip()
    if not name:
        raise ValueError("Missing filename")
    cleaned = re.sub(r'[<>:"/\\\\|?*]+', "-", name)
    cleaned = cleaned.strip(". ")
    if not cleaned:
        raise ValueError("Invalid filename")
    return cleaned


def upload_directory_for_kind(base_dir: Path, upload_kind: str) -> Path:
    folder_name = UPLOAD_TARGET_FOLDERS.get(str(upload_kind or "").strip().lower())
    if not folder_name:
        raise ValueError("Unsupported upload type")
    target = base_dir / folder_name
    target.mkdir(parents=True, exist_ok=True)
    return target
