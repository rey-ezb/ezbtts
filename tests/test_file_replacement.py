from __future__ import annotations

import unittest

import pandas as pd

from web_dashboard.file_replacement import keep_latest_file_rows_by_date


class FileReplacementTests(unittest.TestCase):
    def test_keeps_latest_sales_rows_for_same_reporting_date(self) -> None:
        frame = pd.DataFrame(
            [
                {"source_type": "Sales", "reporting_date": pd.Timestamp("2026-01-05"), "source_file": "jan-old.csv", "source_file_mtime": 100.0, "order_id": "1"},
                {"source_type": "Sales", "reporting_date": pd.Timestamp("2026-01-05"), "source_file": "jan-new.csv", "source_file_mtime": 200.0, "order_id": "2"},
                {"source_type": "Sales", "reporting_date": pd.Timestamp("2026-01-06"), "source_file": "jan-old.csv", "source_file_mtime": 100.0, "order_id": "3"},
            ]
        )

        result = keep_latest_file_rows_by_date(
            frame,
            date_column="reporting_date",
            file_column="source_file",
            mtime_column="source_file_mtime",
            partition_columns=["source_type"],
        )

        self.assertEqual(sorted(result["order_id"].tolist()), ["2", "3"])

    def test_keeps_latest_statement_rows_for_same_statement_date(self) -> None:
        frame = pd.DataFrame(
            [
                {"statement_date": pd.Timestamp("2026-03-31"), "statement_source_file": "q1-old.xlsx", "statement_source_mtime": 100.0, "amount": 10},
                {"statement_date": pd.Timestamp("2026-03-31"), "statement_source_file": "q1-new.xlsx", "statement_source_mtime": 200.0, "amount": 20},
                {"statement_date": pd.Timestamp("2026-04-01"), "statement_source_file": "apr.xlsx", "statement_source_mtime": 150.0, "amount": 30},
            ]
        )

        result = keep_latest_file_rows_by_date(
            frame,
            date_column="statement_date",
            file_column="statement_source_file",
            mtime_column="statement_source_mtime",
        )

        self.assertEqual(result["amount"].tolist(), [20, 30])


if __name__ == "__main__":
    unittest.main()
