from __future__ import annotations

import os
from pathlib import Path

from deployment.supabase_api import delete_rows, download_storage_file, fetch_rows, insert_rows, update_rows
from deployment.upload_coverage import determine_active_uploads, infer_upload_coverage
from web_dashboard.upload_helpers import UPLOAD_TARGET_FOLDERS


def normalize_prefix(prefix: str) -> str:
    return prefix.strip().strip("/")


def upload_bucket() -> str:
    return (os.getenv("SUPABASE_UPLOAD_BUCKET") or "").strip()


def upload_prefix() -> str:
    return normalize_prefix(os.getenv("SUPABASE_UPLOAD_PREFIX") or "uploads")


def remote_uploads_enabled() -> bool:
    return bool(upload_bucket())


def sync_upload_coverage(target_root: Path) -> list[dict[str, str]]:
    batches = fetch_rows(
        "upload_batches",
        query={
            "select": "id,upload_type,original_filename,storage_path,uploaded_at",
            "order": "uploaded_at.desc",
        },
    )
    coverage_rows: list[dict[str, str]] = []
    for batch in batches:
        storage_path = str(batch.get("storage_path") or "").strip("/")
        upload_type = str(batch.get("upload_type") or "").strip().lower()
        if not storage_path or upload_type not in UPLOAD_TARGET_FOLDERS:
            continue
        local_file = target_root / "_coverage_scan" / Path(storage_path).name
        download_storage_file(upload_bucket(), storage_path, local_file)
        coverage = infer_upload_coverage(local_file, upload_type)
        coverage_rows.append(
            {
                "upload_batch_id": batch["id"],
                "upload_type": upload_type,
                "uploaded_at": batch.get("uploaded_at"),
                "storage_path": storage_path,
                "source_label": upload_type,
                "start_date": coverage.start_date,
                "end_date": coverage.end_date,
            }
        )

    resolved = determine_active_uploads(coverage_rows)
    existing = fetch_rows("upload_coverage", query={"select": "id,upload_batch_id"})
    existing_ids = [row["id"] for row in existing if row.get("id")]
    for coverage_id in existing_ids:
        delete_rows("upload_coverage", query={"id": f"eq.{coverage_id}"})

    insert_rows(
        "upload_coverage",
        [
            {
                "upload_batch_id": row["upload_batch_id"],
                "source_label": row["source_label"],
                "start_date": row["start_date"],
                "end_date": row["end_date"],
                "replaced_by_batch_id": row["replaced_by_batch_id"],
                "is_active": row["is_active"],
            }
            for row in resolved
        ],
    )

    for row in resolved:
        update_rows(
            "upload_batches",
            {"notes": f"coverage:{row['start_date'] or 'unknown'}:{row['end_date'] or 'unknown'} active={row['is_active']}"},
            query={"id": f"eq.{row['upload_batch_id']}"},
        )

    return resolved


def materialize_supabase_uploads(target_root: Path) -> Path | None:
    bucket = upload_bucket()
    if not bucket:
        return None

    active_uploads = sync_upload_coverage(target_root)
    active_by_type = {
        row["upload_batch_id"]: row
        for row in active_uploads
        if row.get("is_active") and row.get("upload_type") in UPLOAD_TARGET_FOLDERS and row.get("storage_path")
    }
    wrote_any = False
    for row in active_by_type.values():
        upload_kind = row["upload_type"]
        folder_name = UPLOAD_TARGET_FOLDERS[upload_kind]
        target_dir = target_root / folder_name
        target_dir.mkdir(parents=True, exist_ok=True)
        object_name = str(row["storage_path"]).strip("/")
        destination = target_dir / Path(object_name).name
        download_storage_file(bucket, object_name, destination)
        wrote_any = True

    return target_root if wrote_any else None
