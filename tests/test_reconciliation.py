from __future__ import annotations

import unittest

import pandas as pd

from web_dashboard.server import build_reconciliation_view


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


if __name__ == "__main__":
    unittest.main()
