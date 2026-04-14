from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from web_dashboard.server import dashboard_payload, detect_order_source_dirs, detect_statement_sources, meta_payload


WEB_DIR = ROOT_DIR / "web_dashboard"
SNAPSHOT_DIR = WEB_DIR / "data" / "snapshot"


def has_local_snapshot_inputs() -> bool:
    if detect_order_source_dirs():
        return True
    if detect_statement_sources():
        return True
    return False


def build_dashboard_params(meta: dict[str, Any]) -> dict[str, list[str]]:
    params: dict[str, list[str]] = {}
    default_output = meta.get("defaultOutputDir")
    if default_output:
        params["output"] = [str(default_output)]

    env_map = {
        "DASHBOARD_TARGET_ZIP": "target_zip",
        "DASHBOARD_RADIUS_MILES": "radius_miles",
        "DASHBOARD_TARGET_CITY": "target_city",
        "DASHBOARD_TARGET_STATE": "target_state",
        "DASHBOARD_DATE_BASIS": "date_basis",
        "DASHBOARD_ORDER_BUCKET_MODE": "order_bucket_mode",
    }
    for env_name, param_name in env_map.items():
        value = os.getenv(env_name)
        if value:
            params[param_name] = [value]

    return params


def build_static_meta(meta: dict[str, Any], generated_at: str) -> dict[str, Any]:
    default_output = meta.get("defaultOutputDir")
    return {
        **meta,
        "outputDirs": [default_output] if default_output else [],
        "deploymentMode": "static",
        "snapshotGeneratedAt": generated_at,
    }


def build_static_payload(payload: dict[str, Any], generated_at: str) -> dict[str, Any]:
    summary = dict(payload.get("summary") or {})
    summary["deployment_mode"] = "static"
    return {
        **payload,
        "summary": summary,
        "snapshotGeneratedAt": generated_at,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def export_snapshot(target_dir: Path | None = None) -> tuple[Path, Path]:
    generated_at = datetime.now(timezone.utc).isoformat()
    destination = target_dir or SNAPSHOT_DIR
    meta_path = destination / "meta.json"
    dashboard_path = destination / "dashboard.json"

    if not has_local_snapshot_inputs() and meta_path.exists() and dashboard_path.exists():
        return meta_path, dashboard_path

    meta = meta_payload()
    params = build_dashboard_params(meta)
    payload = dashboard_payload(params)
    static_meta = build_static_meta(meta, generated_at)
    static_payload = build_static_payload(payload, generated_at)
    write_json(meta_path, static_meta)
    write_json(dashboard_path, static_payload)
    return meta_path, dashboard_path


if __name__ == "__main__":
    meta_path, dashboard_path = export_snapshot()
    print(f"Wrote snapshot meta to {meta_path}")
    print(f"Wrote snapshot dashboard to {dashboard_path}")
