from __future__ import annotations

import unittest
from unittest.mock import patch

from deployment.hosted_uploads import build_storage_object_path, hosted_uploads_enabled
from deployment.supabase_api import build_storage_object_url


class HostedUploadsTests(unittest.TestCase):
    def test_hosted_uploads_enabled_requires_core_env(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertFalse(hosted_uploads_enabled())

        with patch.dict(
            "os.environ",
            {
                "SUPABASE_URL": "https://example.supabase.co",
                "SUPABASE_SERVICE_ROLE_KEY": "sb_secret_test",
                "SUPABASE_UPLOAD_BUCKET": "dashboard-uploads",
            },
            clear=True,
        ):
            self.assertTrue(hosted_uploads_enabled())

    def test_build_storage_object_path_keeps_kind_and_sanitized_filename(self) -> None:
        with patch("deployment.hosted_uploads.timestamp_token", return_value="20260414T182233Z"):
            path = build_storage_object_path("sales", 'March 2026 Orders?.csv')

        self.assertEqual(path, "uploads/sales/20260414T182233Z__March 2026 Orders-.csv")

    def test_build_storage_object_url_encodes_spaces_in_path(self) -> None:
        with patch.dict("os.environ", {"SUPABASE_URL": "https://example.supabase.co"}, clear=True):
            url = build_storage_object_url(
                "dashboard-uploads",
                "uploads/sales/20260415T143837Z__All order-2026-04-15-10_14.csv",
            )

        self.assertEqual(
            url,
            "https://example.supabase.co/storage/v1/object/dashboard-uploads/uploads/sales/20260415T143837Z__All%20order-2026-04-15-10_14.csv",
        )


if __name__ == "__main__":
    unittest.main()
