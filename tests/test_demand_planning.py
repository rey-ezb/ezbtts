from __future__ import annotations

import unittest

import pandas as pd

from web_dashboard.demand_planning import (
    calculate_planning_row,
    choose_baseline_window,
    planning_defaults,
    resolve_planning_horizon,
    safety_stock_weeks_for_date,
)


class DemandPlanningTests(unittest.TestCase):
    def test_planning_defaults_use_last_30_days(self) -> None:
        defaults = planning_defaults()
        self.assertEqual(defaults["baseline"], "last_30_days")

    def test_choose_baseline_window_last_full_month(self) -> None:
        start, end, label = choose_baseline_window(pd.Timestamp("2026-04-14"), "last_full_month", pd.Timestamp("2026-05-01"), pd.Timestamp("2026-05-31"))
        self.assertEqual((str(start.date()), str(end.date()), label), ("2026-03-01", "2026-03-31", "Last Full Month"))

    def test_choose_baseline_window_custom_range_uses_explicit_dates(self) -> None:
        start, end, label = choose_baseline_window(
            pd.Timestamp("2026-04-14"),
            "custom_range",
            pd.Timestamp("2026-05-01"),
            pd.Timestamp("2026-05-31"),
            pd.Timestamp("2026-02-01"),
            pd.Timestamp("2026-02-28"),
        )
        self.assertEqual((str(start.date()), str(end.date()), label), ("2026-02-01", "2026-02-28", "Custom Range"))

    def test_resolve_planning_horizon_uses_explicit_future_range(self) -> None:
        start, end = resolve_planning_horizon(
            pd.Timestamp("2026-03-17"),
            pd.Timestamp("2026-04-15"),
            pd.Timestamp("2026-05-01"),
            pd.Timestamp("2026-05-31"),
        )
        self.assertEqual((str(start.date()), str(end.date())), ("2026-05-01", "2026-05-31"))

    def test_resolve_planning_horizon_swaps_inverted_range(self) -> None:
        start, end = resolve_planning_horizon(
            pd.Timestamp("2026-03-17"),
            pd.Timestamp("2026-04-15"),
            pd.Timestamp("2026-08-31"),
            pd.Timestamp("2026-08-01"),
        )
        self.assertEqual((str(start.date()), str(end.date())), ("2026-08-01", "2026-08-31"))

    def test_safety_stock_weeks_changes_by_quarter(self) -> None:
        self.assertEqual(safety_stock_weeks_for_date(pd.Timestamp("2026-05-10")), 3)
        self.assertEqual(safety_stock_weeks_for_date(pd.Timestamp("2026-07-10")), 5)

    def test_calculate_planning_row_counts_in_transit_before_stockout(self) -> None:
        row = calculate_planning_row(
            dashboard_product="Birria Bomb 2-Pack",
            inventory_product="Birria",
            inventory_snapshot={"snapshot_date": "2026-04-12", "products": {"Birria": {"on_hand": 1000, "in_transit": 2000}}},
            units_sold_in_baseline=3100,
            baseline_start=pd.Timestamp("2026-03-01"),
            baseline_end=pd.Timestamp("2026-03-31"),
            horizon_start=pd.Timestamp("2026-05-01"),
            horizon_end=pd.Timestamp("2026-05-31"),
            uplift_pct=35.0,
        )
        self.assertEqual(row["counted_in_transit"], 2000)
        self.assertGreater(row["reorder_quantity"], 0)


if __name__ == "__main__":
    unittest.main()
