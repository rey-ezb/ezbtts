from __future__ import annotations

import mimetypes
import os
from pathlib import Path
from urllib.request import Request, urlopen

from deployment.export_dashboard_snapshot import SNAPSHOT_DIR, export_snapshot


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value.rstrip("/")


def upload_file(file_path: Path, bucket: str, object_path: str) -> None:
    supabase_url = require_env("SUPABASE_URL")
    service_role_key = require_env("SUPABASE_SERVICE_ROLE_KEY")
    storage_url = f"{supabase_url}/storage/v1/object/{bucket}/{object_path.lstrip('/')}"
    content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    request = Request(
        storage_url,
        data=file_path.read_bytes(),
        method="POST",
        headers={
            "Authorization": f"Bearer {service_role_key}",
            "apikey": service_role_key,
            "Content-Type": content_type,
            "x-upsert": "true",
        },
    )
    with urlopen(request) as response:
        response.read()


def main() -> None:
    bucket = os.getenv("SUPABASE_STORAGE_BUCKET", "dashboard-snapshots")
    prefix = os.getenv("SUPABASE_STORAGE_PREFIX", "latest")
    meta_path, dashboard_path = export_snapshot(SNAPSHOT_DIR)
    upload_file(meta_path, bucket, f"{prefix}/meta.json")
    upload_file(dashboard_path, bucket, f"{prefix}/dashboard.json")
    print(f"Uploaded snapshot files to supabase bucket '{bucket}' with prefix '{prefix}'.")


if __name__ == "__main__":
    main()
