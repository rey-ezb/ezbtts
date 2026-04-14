from __future__ import annotations

import os

from deployment.export_dashboard_snapshot import SNAPSHOT_DIR, export_snapshot
from deployment.supabase_api import upload_storage_file


def main() -> None:
    bucket = os.getenv("SUPABASE_STORAGE_BUCKET", "dashboard-snapshots")
    prefix = os.getenv("SUPABASE_STORAGE_PREFIX", "latest")
    meta_path, dashboard_path = export_snapshot(SNAPSHOT_DIR)
    upload_storage_file(meta_path, bucket, f"{prefix}/meta.json")
    upload_storage_file(dashboard_path, bucket, f"{prefix}/dashboard.json")
    print(f"Uploaded snapshot files to supabase bucket '{bucket}' with prefix '{prefix}'.")


if __name__ == "__main__":
    main()
