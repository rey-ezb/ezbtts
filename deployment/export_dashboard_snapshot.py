from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from deployment.materialize_supabase_uploads import materialize_supabase_uploads
from web_dashboard.server import dashboard_payload, detect_order_source_dirs, detect_statement_sources, meta_payload


WEB_DIR = ROOT_DIR / "web_dashboard"
SNAPSHOT_DIR = WEB_DIR / "data" / "snapshot"
RUNTIME_CONFIG_PATH = WEB_DIR / "runtime-config.js"
DATA_ROOT_ENV = "DASHBOARD_DATA_ROOT"
CHUNKED_KEYS = {
    "productDailyRows": "reporting_date",
    "orderLevelRowsAll": "reporting_date",
    "customerFirstOrderAllRows": "first_order_date",
    "statementRowsAll": "statement_date",
    "rawProductNameAllRows": "reporting_date",
}
MAX_CHUNK_BYTES = 20_000_000


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
    static_payload = {
        **payload,
        "summary": summary,
        "snapshotGeneratedAt": generated_at,
    }
    for key in [
        "rawProductNameRows",
        "reconciliationRows",
        "unmatchedStatementRows",
        "unmatchedOrderRows",
        "cityRows",
        "zipRows",
        "radiusRows",
        "targetCityRows",
        "cohortSummaryRows",
        "cohortHeatmap",
    ]:
        static_payload.pop(key, None)
    return static_payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def month_key(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if len(normalized) < 7:
        return None
    month = normalized[:7]
    if len(month) == 7 and month[4] == "-":
        return month
    return None


def chunk_relative_path(key: str, month: str, part_index: int) -> str:
    return f"chunks/{key}/{month}-{part_index}.json"


def split_rows_by_size(rows: list[dict[str, Any]], max_bytes: int = MAX_CHUNK_BYTES) -> list[list[dict[str, Any]]]:
    parts: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_bytes = 2
    for row in rows:
        row_bytes = len(json.dumps(row, separators=(",", ":")).encode("utf-8")) + 1
        if current and current_bytes + row_bytes > max_bytes:
            parts.append(current)
            current = []
            current_bytes = 2
        current.append(row)
        current_bytes += row_bytes
    if current:
        parts.append(current)
    return parts


def split_payload_into_chunks(payload: dict[str, Any], destination: Path) -> dict[str, Any]:
    chunk_manifest: dict[str, Any] = {}
    static_payload = dict(payload)
    for key, date_key in CHUNKED_KEYS.items():
        rows = static_payload.pop(key, None) or []
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            month = month_key((row or {}).get(date_key))
            if not month:
                continue
            grouped.setdefault(month, []).append(row)
        months = sorted(grouped.keys())
        files_by_month: dict[str, list[str]] = {}
        for month in months:
            month_parts = split_rows_by_size(grouped[month])
            files_by_month[month] = []
            for index, part_rows in enumerate(month_parts, start=1):
                relative_path = chunk_relative_path(key, month, index)
                write_json(destination / relative_path, part_rows)
                files_by_month[month].append(relative_path)
        chunk_manifest[key] = {
            "dateKey": date_key,
            "months": months,
            "filesByMonth": files_by_month,
        }
    static_payload["chunkManifest"] = chunk_manifest
    return static_payload


def normalize_storage_prefix(prefix: str) -> str:
    return prefix.strip().strip("/")


def build_runtime_config() -> dict[str, str]:
    supabase_url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    storage_bucket = (os.getenv("SUPABASE_STORAGE_BUCKET") or "").strip()
    storage_prefix = normalize_storage_prefix(os.getenv("SUPABASE_STORAGE_PREFIX") or "latest")

    if supabase_url and storage_bucket:
        public_base = f"{supabase_url}/storage/v1/object/public/{storage_bucket}"
        if storage_prefix:
            public_base = f"{public_base}/{storage_prefix}"
        return {
            "mode": "static",
            "staticMetaUrl": f"{public_base}/meta.json",
            "staticDashboardUrl": f"{public_base}/dashboard.json",
        }

    return {
        "mode": "auto",
        "staticMetaUrl": "./data/snapshot/meta.json",
        "staticDashboardUrl": "./data/snapshot/dashboard.json",
    }


def write_runtime_config(path: Path | None = None) -> Path:
    destination = path or RUNTIME_CONFIG_PATH
    config = build_runtime_config()
    destination.write_text(
        "window.DASHBOARD_CONFIG = "
        + json.dumps(config, indent=2)
        + ";\n",
        encoding="utf-8",
    )
    return destination


def export_snapshot(target_dir: Path | None = None) -> tuple[Path, Path]:
    generated_at = datetime.now(timezone.utc).isoformat()
    destination = target_dir or SNAPSHOT_DIR
    meta_path = destination / "meta.json"
    dashboard_path = destination / "dashboard.json"

    temp_input_dir: tempfile.TemporaryDirectory[str] | None = None
    previous_data_root = os.getenv(DATA_ROOT_ENV)
    has_inputs = has_local_snapshot_inputs()
    if not has_inputs:
        temp_input_dir = tempfile.TemporaryDirectory()
        staged_root = materialize_supabase_uploads(Path(temp_input_dir.name))
        if staged_root is not None:
            os.environ[DATA_ROOT_ENV] = str(staged_root)
            has_inputs = has_local_snapshot_inputs()
        else:
            temp_input_dir.cleanup()
            temp_input_dir = None

    if not has_inputs and meta_path.exists() and dashboard_path.exists():
        write_runtime_config()
        return meta_path, dashboard_path

    try:
        meta = meta_payload()
        params = build_dashboard_params(meta)
        payload = dashboard_payload(params)
        static_meta = build_static_meta(meta, generated_at)
        shutil.rmtree(destination / "chunks", ignore_errors=True)
        static_payload = split_payload_into_chunks(build_static_payload(payload, generated_at), destination)
        write_json(meta_path, static_meta)
        write_json(dashboard_path, static_payload)
        write_runtime_config()
        return meta_path, dashboard_path
    finally:
        if previous_data_root is None:
            os.environ.pop(DATA_ROOT_ENV, None)
        else:
            os.environ[DATA_ROOT_ENV] = previous_data_root
        if temp_input_dir is not None:
            temp_input_dir.cleanup()


if __name__ == "__main__":
    meta_path, dashboard_path = export_snapshot()
    print(f"Wrote snapshot meta to {meta_path}")
    print(f"Wrote snapshot dashboard to {dashboard_path}")
