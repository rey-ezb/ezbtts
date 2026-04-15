from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from deployment.export_dashboard_snapshot import (
    build_dashboard_params,
    build_runtime_config,
    build_static_meta,
    build_static_payload,
    export_snapshot,
)


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
                patch("deployment.export_dashboard_snapshot.write_runtime_config"),
            ):
                result_meta, result_dashboard = export_snapshot(target_dir)

            self.assertEqual(result_meta, meta_path)
            self.assertEqual(result_dashboard, dashboard_path)
            self.assertEqual(meta_path.read_text(encoding="utf-8"), '{"existing": true}')
            self.assertEqual(dashboard_path.read_text(encoding="utf-8"), '{"existing": true}')

    def test_export_snapshot_uses_materialized_remote_inputs_before_reusing_existing_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target_dir = Path(tmp_dir)
            meta_path = target_dir / "meta.json"
            dashboard_path = target_dir / "dashboard.json"
            meta_path.write_text('{"existing": true}', encoding="utf-8")
            dashboard_path.write_text('{"existing": true}', encoding="utf-8")

            with tempfile.TemporaryDirectory() as staged_dir:
                with (
                    patch("deployment.export_dashboard_snapshot.has_local_snapshot_inputs", side_effect=[False, True]),
                    patch("deployment.export_dashboard_snapshot.materialize_supabase_uploads", return_value=Path(staged_dir)),
                    patch("deployment.export_dashboard_snapshot.meta_payload", return_value={"defaultOutputDir": "analysis_output"}),
                    patch("deployment.export_dashboard_snapshot.dashboard_payload", return_value={"summary": {}}),
                    patch("deployment.export_dashboard_snapshot.write_runtime_config"),
                ):
                    result_meta, result_dashboard = export_snapshot(target_dir)

            self.assertEqual(result_meta, meta_path)
            self.assertEqual(result_dashboard, dashboard_path)
            self.assertNotEqual(meta_path.read_text(encoding="utf-8"), '{"existing": true}')
            self.assertNotEqual(dashboard_path.read_text(encoding="utf-8"), '{"existing": true}')

    def test_build_runtime_config_defaults_to_local_snapshot_files(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            config = build_runtime_config()

        self.assertEqual(
            config,
            {
                "mode": "auto",
                "staticMetaUrl": "./data/snapshot/meta.json",
                "staticDashboardUrl": "./data/snapshot/dashboard.json",
            },
        )

    def test_build_runtime_config_uses_public_supabase_storage_urls(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SUPABASE_URL": "https://smfmrieskhqcvzzgqqbe.supabase.co/",
                "SUPABASE_STORAGE_BUCKET": "dashboard-snapshots",
                "SUPABASE_STORAGE_PREFIX": "/latest/",
            },
            clear=True,
        ):
            config = build_runtime_config()

        self.assertEqual(config["mode"], "static")
        self.assertEqual(
            config["staticMetaUrl"],
            "https://smfmrieskhqcvzzgqqbe.supabase.co/storage/v1/object/public/dashboard-snapshots/latest/meta.json",
        )
        self.assertEqual(
            config["staticDashboardUrl"],
            "https://smfmrieskhqcvzzgqqbe.supabase.co/storage/v1/object/public/dashboard-snapshots/latest/dashboard.json",
        )


if __name__ == "__main__":
    unittest.main()
