from __future__ import annotations

import unittest

import pandas as pd

from web_dashboard.server import build_reconciliation_view, build_statement_daily_table, empty_statement_rows


class ReconciliationTests(unittest.TestCase):
    def test_order_basis_handles_empty_statement_rollup(self) -> None:
        order_level_df = pd.DataFrame(
            [
                {
                    "order_id": "A-100",
                    "reporting_date": pd.Timestamp("2026-04-10"),
                    "units_sold": 2,
                    "order_sales": 20.0,
                    "gross_sales": 22.0,
                }
            ]
        )

        matched, unmatched_statement, unmatched_orders, summary = build_reconciliation_view(
            order_level_df=order_level_df,
            statement_rows=pd.DataFrame(),
            date_basis="order",
            start_date=pd.Timestamp("2026-04-01"),
            end_date=pd.Timestamp("2026-04-30"),
        )

        self.assertEqual(len(matched), 1)
        self.assertEqual(len(unmatched_statement), 0)
        self.assertEqual(len(unmatched_orders), 1)
        self.assertEqual(summary["matched_orders"], 1)
        self.assertEqual(summary["unmatched_orders"], 1)
        self.assertEqual(float(matched.loc[0, "statement_amount_total"]), 0.0)

    def test_statement_daily_table_keeps_reporting_date_column_when_empty(self) -> None:
        daily = build_statement_daily_table(pd.DataFrame())

        self.assertIn("reporting_date", daily.columns)
        self.assertEqual(len(daily), 0)

    def test_empty_statement_rows_keep_statement_date_column(self) -> None:
        rows = empty_statement_rows()

        self.assertIn("statement_date", rows.columns)
        self.assertEqual(len(rows), 0)


if __name__ == "__main__":
    unittest.main()
