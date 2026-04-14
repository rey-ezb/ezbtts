from __future__ import annotations

import unittest

from deployment.sync_dashboard_to_supabase import build_inventory_planning_rows, build_snapshot_record


class SyncDashboardToSupabaseTests(unittest.TestCase):
    def test_build_snapshot_record_keeps_core_sections(self) -> None:
        meta = {"snapshotGeneratedAt": "2026-04-14T12:00:00+00:00"}
        payload = {
            "summary": {"deployment_mode": "static"},
            "planningConfig": {"baselineLabel": "Last Full Month"},
            "orderSummary": {"orders_paid_orders": 10},
            "statementSummary": {},
            "reconciliationSummary": {},
            "dataQualitySummary": {},
            "reportMarkdown": "report",
            "selectedOutputDir": "analysis_output",
        }
        record = build_snapshot_record(meta, payload)
        self.assertEqual(record["selected_output_dir"], "analysis_output")
        self.assertEqual(record["planning_config"]["baselineLabel"], "Last Full Month")
        self.assertEqual(record["order_summary"]["orders_paid_orders"], 10)

    def test_build_inventory_planning_rows_attaches_snapshot_id(self) -> None:
        payload = {
            "inventoryPlanningRows": [
                {"product": "Birria Bomb 2-Pack", "forecast_uplift_pct": 35.0, "reorder_quantity": 1000.0}
            ]
        }
        rows = build_inventory_planning_rows("snap-1", payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["snapshot_id"], "snap-1")
        self.assertEqual(rows[0]["product"], "Birria Bomb 2-Pack")
        self.assertEqual(rows[0]["forecast_uplift_pct"], 35.0)


if __name__ == "__main__":
    unittest.main()
