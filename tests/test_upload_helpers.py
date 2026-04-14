from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from web_dashboard.upload_helpers import sanitize_upload_filename, upload_directory_for_kind


class UploadHelpersTests(unittest.TestCase):
    def test_sanitize_upload_filename_removes_path_and_invalid_chars(self) -> None:
        cleaned = sanitize_upload_filename("..\\Jan:2026?.csv")
        self.assertEqual(cleaned, "Jan-2026-.csv")

    def test_upload_directory_for_kind_returns_expected_folder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            result = upload_directory_for_kind(Path(temp_dir), "sales")
            self.assertEqual(result.name, "All orders")
            self.assertTrue(result.exists())


if __name__ == "__main__":
    unittest.main()
