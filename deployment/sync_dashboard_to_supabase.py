from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from deployment.export_dashboard_snapshot import SNAPSHOT_DIR, export_snapshot
from deployment.supabase_api import insert_rows, upload_storage_file


def build_snapshot_record(meta: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload.get("summary") or {}
    return {
        "snapshot_label": os.getenv("SUPABASE_SNAPSHOT_LABEL", "latest"),
        "selected_output_dir": payload.get("selectedOutputDir"),
        "generated_at": payload.get("snapshotGeneratedAt") or meta.get("snapshotGeneratedAt"),
        "meta": meta,
        "summary": summary,
        "planning_config": payload.get("planningConfig") or {},
        "order_summary": payload.get("orderSummary") or {},
        "statement_summary": payload.get("statementSummary") or {},
        "reconciliation_summary": payload.get("reconciliationSummary") or {},
        "data_quality_summary": payload.get("dataQualitySummary") or {},
        "report_markdown": payload.get("reportMarkdown") or "",
    }


def build_inventory_planning_rows(snapshot_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in payload.get("inventoryPlanningRows") or []:
        rows.append(
            {
                "snapshot_id": snapshot_id,
                "product": row.get("product"),
                "inventory_product": row.get("inventory_product"),
                "snapshot_date": row.get("snapshot_date"),
                "baseline_label": row.get("baseline_label"),
                "baseline_start": row.get("baseline_start"),
                "baseline_end": row.get("baseline_end"),
                "on_hand": row.get("on_hand"),
                "in_transit": row.get("in_transit"),
                "counted_in_transit": row.get("counted_in_transit"),
                "effective_total_supply": row.get("effective_total_supply"),
                "units_sold_in_window": row.get("units_sold_in_window"),
                "avg_daily_demand": row.get("avg_daily_demand"),
                "forecast_uplift_pct": row.get("forecast_uplift_pct"),
                "forecast_daily_demand": row.get("forecast_daily_demand"),
                "forecast_units_in_horizon": row.get("forecast_units_in_horizon"),
                "safety_stock_weeks": row.get("safety_stock_weeks"),
                "safety_stock_units": row.get("safety_stock_units"),
                "projected_in_transit_arrival_date": row.get("projected_in_transit_arrival_date"),
                "days_on_hand": row.get("days_on_hand"),
                "days_total_supply": row.get("days_total_supply"),
                "weeks_on_hand": row.get("weeks_on_hand"),
                "weeks_total_supply": row.get("weeks_total_supply"),
                "projected_stockout_date": row.get("projected_stockout_date"),
                "reorder_date": row.get("reorder_date"),
                "reorder_quantity": row.get("reorder_quantity"),
                "status": row.get("status"),
            }
        )
    return rows


def load_snapshot_files(target_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    meta = json.loads((target_dir / "meta.json").read_text(encoding="utf-8"))
    payload = json.loads((target_dir / "dashboard.json").read_text(encoding="utf-8"))
    return meta, payload


def main() -> None:
    target_dir = SNAPSHOT_DIR
    bucket = os.getenv("SUPABASE_STORAGE_BUCKET", "dashboard-snapshots")
    prefix = os.getenv("SUPABASE_STORAGE_PREFIX", "latest")
    meta_path, dashboard_path = export_snapshot(target_dir)
    meta, payload = load_snapshot_files(target_dir)

    upload_storage_file(meta_path, bucket, f"{prefix}/meta.json")
    upload_storage_file(dashboard_path, bucket, f"{prefix}/dashboard.json")

    snapshot_record = build_snapshot_record(meta, payload)
    inserted = insert_rows("dashboard_snapshots", [snapshot_record], return_columns="id,snapshot_label,generated_at")
    snapshot_id = inserted[0]["id"]
    planning_rows = build_inventory_planning_rows(snapshot_id, payload)
    insert_rows("inventory_planning_snapshots", planning_rows)

    print(f"Synced dashboard snapshot '{snapshot_record['snapshot_label']}' to Supabase.")
    print(f"Snapshot id: {snapshot_id}")
    print(f"Planning rows synced: {len(planning_rows)}")


if __name__ == "__main__":
    main()
