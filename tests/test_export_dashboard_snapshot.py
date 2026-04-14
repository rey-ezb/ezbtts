from __future__ import annotations

import unittest

from deployment.export_dashboard_snapshot import build_dashboard_params, build_static_meta, build_static_payload


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


if __name__ == "__main__":
    unittest.main()
