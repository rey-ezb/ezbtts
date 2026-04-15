from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from deployment.supabase_api import insert_rows, upload_storage_bytes
from web_dashboard.upload_helpers import sanitize_upload_filename


def hosted_uploads_enabled() -> bool:
    return all(
        (
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
            os.getenv("SUPABASE_UPLOAD_BUCKET"),
        )
    )


def upload_bucket() -> str:
    return (os.getenv("SUPABASE_UPLOAD_BUCKET") or "").strip()


def upload_prefix() -> str:
    return (os.getenv("SUPABASE_UPLOAD_PREFIX") or "uploads").strip().strip("/")


def timestamp_token() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_storage_object_path(upload_kind: str, filename: str) -> str:
    cleaned_name = sanitize_upload_filename(filename)
    prefix = upload_prefix()
    normalized_kind = str(upload_kind or "").strip().lower()
    return "/".join(part for part in [prefix, normalized_kind, f"{timestamp_token()}__{cleaned_name}"] if part)


def upload_hosted_file(upload_kind: str, filename: str, payload: bytes, *, notes: str | None = None) -> dict[str, Any]:
    object_path = build_storage_object_path(upload_kind, filename)
    upload_storage_bytes(payload, upload_bucket(), object_path)
    record = {
        "upload_type": upload_kind,
        "original_filename": sanitize_upload_filename(filename),
        "stored_filename": object_path.rsplit("/", 1)[-1],
        "storage_path": object_path,
        "file_size_bytes": len(payload),
        "notes": notes or "",
    }
    inserted = insert_rows("upload_batches", [record], return_columns="id,upload_type,original_filename,stored_filename,storage_path,uploaded_at,file_size_bytes")
    return inserted[0] if inserted else record
