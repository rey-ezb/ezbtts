from __future__ import annotations

import os
from pathlib import Path

from deployment.export_dashboard_snapshot import SNAPSHOT_DIR, export_snapshot
from deployment.supabase_api import upload_storage_file


def upload_snapshot_tree(target_dir: Path, bucket: str, prefix: str) -> None:
    for path in target_dir.rglob("*.json"):
        object_path = "/".join(part for part in [prefix.strip("/"), path.relative_to(target_dir).as_posix()] if part)
        upload_storage_file(path, bucket, object_path)


def main() -> None:
    bucket = os.getenv("SUPABASE_STORAGE_BUCKET", "dashboard-snapshots")
    prefix = os.getenv("SUPABASE_STORAGE_PREFIX", "latest")
    export_snapshot(SNAPSHOT_DIR)
    upload_snapshot_tree(SNAPSHOT_DIR, bucket, prefix)
    print(f"Uploaded snapshot files to supabase bucket '{bucket}' with prefix '{prefix}'.")


if __name__ == "__main__":
    main()
