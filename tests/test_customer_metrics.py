from __future__ import annotations

import unittest

import pandas as pd

from web_dashboard.server import build_customer_metrics


class CustomerMetricsTests(unittest.TestCase):
    def test_customer_metrics_match_tiktok_new_and_repeat_definitions(self) -> None:
        full_history = pd.DataFrame(
            [
                {
                    "reporting_date": "2026-01-10",
                    "customer_id": "alice",
                    "is_canceled": False,
                    "Order ID": "A-1",
                    "source_type": "Sales",
                    "customer_id_source": "Buyer Username",
                },
                {
                    "reporting_date": "2026-02-05",
                    "customer_id": "alice",
                    "is_canceled": False,
                    "Order ID": "A-2",
                    "source_type": "Sales",
                    "customer_id_source": "Buyer Username",
                },
                {
                    "reporting_date": "2026-02-06",
                    "customer_id": "bob",
                    "is_canceled": False,
                    "Order ID": "B-1",
                    "source_type": "Sales",
                    "customer_id_source": "Buyer Nickname",
                },
                {
                    "reporting_date": "2026-02-12",
                    "customer_id": "carol",
                    "is_canceled": False,
                    "Order ID": "C-1",
                    "source_type": "Sales",
                    "customer_id_source": "Recipient",
                },
                {
                    "reporting_date": "2026-02-20",
                    "customer_id": "carol",
                    "is_canceled": False,
                    "Order ID": "C-2",
                    "source_type": "Sales",
                    "customer_id_source": "Recipient",
                },
                {
                    "reporting_date": "2026-01-15",
                    "customer_id": "david",
                    "is_canceled": False,
                    "Order ID": "D-1",
                    "source_type": "Sales",
                    "customer_id_source": "Buyer Username",
                },
                {
                    "reporting_date": "2026-02-21",
                    "customer_id": "david",
                    "is_canceled": False,
                    "Order ID": "D-2",
                    "source_type": "Sales",
                    "customer_id_source": "Buyer Username",
                },
            ]
        )
        full_history["reporting_date"] = pd.to_datetime(full_history["reporting_date"])
        selected_slice = full_history[full_history["reporting_date"].between("2026-02-01", "2026-02-28")].copy()

        metrics = build_customer_metrics(selected_slice, full_history)

        self.assertEqual(metrics["selected_unique_customers"], 4)
        self.assertEqual(metrics["selected_first_time_buyers"], 2)
        self.assertEqual(metrics["selected_repeat_customers"], 2)
        self.assertEqual(metrics["selected_returning_customers"], 2)
        self.assertAlmostEqual(metrics["selected_repeat_customer_rate"], 2 / 4)
        self.assertAlmostEqual(metrics["selected_first_time_buyer_rate"], 2 / 4)
        self.assertEqual(metrics["customer_proxy_username_count"], 2)
        self.assertEqual(metrics["customer_proxy_nickname_count"], 1)
        self.assertEqual(metrics["customer_proxy_recipient_count"], 1)
        self.assertAlmostEqual(metrics["customer_proxy_username_pct"], 0.5)
        self.assertAlmostEqual(metrics["customer_proxy_nickname_pct"], 0.25)
        self.assertAlmostEqual(metrics["customer_proxy_recipient_pct"], 0.25)


if __name__ == "__main__":
    unittest.main()
