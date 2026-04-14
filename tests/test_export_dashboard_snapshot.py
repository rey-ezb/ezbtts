from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from deployment.export_dashboard_snapshot import build_dashboard_params, build_static_meta, build_static_payload, export_snapshot


class ExportDashboardSnapshotTests(unittest.TestCase):
    def test_build_static_meta_limits_output_dirs_to_default(self) -> None:
        meta = {
            "outputDirs": ["analysis_output", "analysis_output_test"],
            "defaultOutputDir": "analysis_output",
        }

        static_meta = build_static_meta(meta, "2026-04-14T12:00:00+00:00")

        self.assertEqual(static_meta["outputDirs"], ["analysis_output"])
        self.assertEqual(static_meta["deploymentMode"], "static")
        self.assertEqual(static_meta["snapshotGeneratedAt"], "2026-04-14T12:00:00+00:00")

    def test_build_static_payload_marks_summary_as_static(self) -> None:
        payload = {"summary": {"selected_sources": ["Sales"]}, "orderSummary": {"orders_paid_orders": 10}}

        static_payload = build_static_payload(payload, "2026-04-14T12:00:00+00:00")

        self.assertEqual(static_payload["summary"]["deployment_mode"], "static")
        self.assertEqual(static_payload["snapshotGeneratedAt"], "2026-04-14T12:00:00+00:00")
        self.assertEqual(static_payload["orderSummary"]["orders_paid_orders"], 10)

    def test_build_dashboard_params_uses_default_output_only_when_no_env_is_present(self) -> None:
        meta = {"defaultOutputDir": "analysis_output"}

        params = build_dashboard_params(meta)

        self.assertEqual(params, {"output": ["analysis_output"]})

    def test_export_snapshot_reuses_existing_files_when_no_local_inputs_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target_dir = Path(tmp_dir)
            meta_path = target_dir / "meta.json"
            dashboard_path = target_dir / "dashboard.json"
            meta_path.write_text('{"existing": true}', encoding="utf-8")
            dashboard_path.write_text('{"existing": true}', encoding="utf-8")

            with (
                patch("deployment.export_dashboard_snapshot.detect_order_source_dirs", return_value=[]),
                patch("deployment.export_dashboard_snapshot.detect_statement_sources", return_value=[]),
            ):
                result_meta, result_dashboard = export_snapshot(target_dir)

            self.assertEqual(result_meta, meta_path)
            self.assertEqual(result_dashboard, dashboard_path)
            self.assertEqual(meta_path.read_text(encoding="utf-8"), '{"existing": true}')
            self.assertEqual(dashboard_path.read_text(encoding="utf-8"), '{"existing": true}')


if __name__ == "__main__":
    unittest.main()
