from __future__ import annotations

import unittest

from deployment.upload_coverage import determine_active_uploads


class UploadCoverageTests(unittest.TestCase):
    def test_newer_full_overlap_deactivates_older_upload(self) -> None:
        resolved = determine_active_uploads(
            [
                {
                    "upload_batch_id": "old",
                    "upload_type": "sales",
                    "uploaded_at": "2026-04-01T10:00:00+00:00",
                    "start_date": "2026-03-01",
                    "end_date": "2026-03-31",
                },
                {
                    "upload_batch_id": "new",
                    "upload_type": "sales",
                    "uploaded_at": "2026-04-14T10:00:00+00:00",
                    "start_date": "2026-03-01",
                    "end_date": "2026-04-30",
                },
            ]
        )
        by_id = {row["upload_batch_id"]: row for row in resolved}
        self.assertTrue(by_id["new"]["is_active"])
        self.assertFalse(by_id["old"]["is_active"])
        self.assertEqual(by_id["old"]["replaced_by_batch_id"], "new")

    def test_partial_overlap_keeps_older_upload_active(self) -> None:
        resolved = determine_active_uploads(
            [
                {
                    "upload_batch_id": "old",
                    "upload_type": "sales",
                    "uploaded_at": "2026-03-31T10:00:00+00:00",
                    "start_date": "2026-01-01",
                    "end_date": "2026-03-31",
                },
                {
                    "upload_batch_id": "new",
                    "upload_type": "sales",
                    "uploaded_at": "2026-04-14T10:00:00+00:00",
                    "start_date": "2026-03-01",
                    "end_date": "2026-04-30",
                },
            ]
        )
        by_id = {row["upload_batch_id"]: row for row in resolved}
        self.assertTrue(by_id["new"]["is_active"])
        self.assertTrue(by_id["old"]["is_active"])
        self.assertIsNone(by_id["old"]["replaced_by_batch_id"])


if __name__ == "__main__":
    unittest.main()
