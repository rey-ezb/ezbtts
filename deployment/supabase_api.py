from __future__ import annotations

import json
import mimetypes
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value.rstrip("/")


def service_role_key() -> str:
    return require_env("SUPABASE_SERVICE_ROLE_KEY")


def supabase_url() -> str:
    return require_env("SUPABASE_URL")


def rest_request(method: str, path: str, payload: Any | None = None, query: dict[str, str] | None = None) -> Any:
    url = f"{supabase_url()}/rest/v1/{path.lstrip('/')}"
    if query:
        url = f"{url}?{urlencode(query)}"
    body = None
    headers = {
        "Authorization": f"Bearer {service_role_key()}",
        "apikey": service_role_key(),
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=body, method=method.upper(), headers=headers)
    with urlopen(request) as response:
        raw = response.read()
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))


def insert_rows(table: str, rows: list[dict[str, Any]], *, return_columns: str | None = None) -> Any:
    if not rows:
        return []
    headers = {
        "Prefer": "return=representation",
    }
    query = {"select": return_columns} if return_columns else None
    url = f"{supabase_url()}/rest/v1/{table}"
    if query:
        url = f"{url}?{urlencode(query)}"
    request = Request(
        url,
        data=json.dumps(rows).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {service_role_key()}",
            "apikey": service_role_key(),
            "Content-Type": "application/json",
            **headers,
        },
    )
    with urlopen(request) as response:
        raw = response.read()
        return json.loads(raw.decode("utf-8")) if raw else []


def fetch_rows(table: str, *, query: dict[str, str] | None = None) -> list[dict[str, Any]]:
    result = rest_request("GET", table, query=query)
    return result or []


def update_rows(table: str, payload: dict[str, Any], *, query: dict[str, str]) -> list[dict[str, Any]]:
    url = f"{supabase_url()}/rest/v1/{table}"
    if query:
        url = f"{url}?{urlencode(query)}"
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="PATCH",
        headers={
            "Authorization": f"Bearer {service_role_key()}",
            "apikey": service_role_key(),
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        },
    )
    with urlopen(request) as response:
        raw = response.read()
        return json.loads(raw.decode("utf-8")) if raw else []


def delete_rows(table: str, *, query: dict[str, str]) -> None:
    url = f"{supabase_url()}/rest/v1/{table}"
    if query:
        url = f"{url}?{urlencode(query)}"
    request = Request(
        url,
        method="DELETE",
        headers={
            "Authorization": f"Bearer {service_role_key()}",
            "apikey": service_role_key(),
            "Prefer": "return=minimal",
        },
    )
    with urlopen(request) as response:
        response.read()


def upload_storage_file(file_path: Path, bucket: str, object_path: str) -> None:
    storage_url = f"{supabase_url()}/storage/v1/object/{bucket}/{object_path.lstrip('/')}"
    content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    request = Request(
        storage_url,
        data=file_path.read_bytes(),
        method="POST",
        headers={
            "Authorization": f"Bearer {service_role_key()}",
            "apikey": service_role_key(),
            "Content-Type": content_type,
            "x-upsert": "true",
        },
    )
    with urlopen(request) as response:
        response.read()


def upload_storage_bytes(content: bytes, bucket: str, object_path: str, *, content_type: str = "application/octet-stream") -> None:
    storage_url = f"{supabase_url()}/storage/v1/object/{bucket}/{object_path.lstrip('/')}"
    request = Request(
        storage_url,
        data=content,
        method="POST",
        headers={
            "Authorization": f"Bearer {service_role_key()}",
            "apikey": service_role_key(),
            "Content-Type": content_type,
            "x-upsert": "true",
        },
    )
    with urlopen(request) as response:
        response.read()


def list_storage_objects(bucket: str, prefix: str = "") -> list[dict[str, Any]]:
    normalized = prefix.strip().strip("/")
    request = Request(
        f"{supabase_url()}/storage/v1/object/list/{bucket}",
        data=json.dumps({"prefix": normalized}).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {service_role_key()}",
            "apikey": service_role_key(),
            "Content-Type": "application/json",
        },
    )
    with urlopen(request) as response:
        raw = response.read()
        rows = json.loads(raw.decode("utf-8")) if raw else []
    results: list[dict[str, Any]] = []
    for row in rows:
        name = str(row.get("name") or "")
        if not name:
            continue
        object_name = "/".join(part for part in [normalized, name.strip("/")] if part)
        results.append({**row, "name": object_name})
    return results


def download_storage_file(bucket: str, object_path: str, destination: Path) -> Path:
    download_url = f"{supabase_url()}/storage/v1/object/{bucket}/{object_path.lstrip('/')}"
    request = Request(
        download_url,
        method="GET",
        headers={
            "Authorization": f"Bearer {service_role_key()}",
            "apikey": service_role_key(),
        },
    )
    with urlopen(request) as response:
        payload = response.read()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)
    return destination
