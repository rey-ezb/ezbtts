from __future__ import annotations

import math
from typing import Any

import pandas as pd


DEFAULT_FORECAST_UPLIFT_PCT = 35.0
INBOUND_TO_FBT_LEAD_DAYS = 5
REORDER_LEAD_DAYS = 8
PLANNING_BASELINE_OPTIONS = [
    {"value": "last_full_month", "label": "Last Full Month"},
    {"value": "last_30_days", "label": "Last 30 Days"},
    {"value": "last_90_days", "label": "Last 90 Days"},
    {"value": "custom_range", "label": "Custom Range"},
]
FORECAST_PRODUCT_PARAM_MAP = {
    "Birria Bomb 2-Pack": "forecast_birria",
    "Pozole Bomb 2-Pack": "forecast_pozole",
    "Tinga Bomb 2-Pack": "forecast_tinga",
    "Pozole Verde Bomb 2-Pack": "forecast_pozole_verde",
    "Brine Bomb": "forecast_brine",
    "Variety Pack": "forecast_variety_pack",
}


def planning_defaults() -> dict[str, Any]:
    return {
        "baseline": "last_full_month",
        "default_uplift_pct": DEFAULT_FORECAST_UPLIFT_PCT,
        "baselineOptions": PLANNING_BASELINE_OPTIONS,
        "productOverrides": [
            {"product": product, "param": param, "default_uplift_pct": DEFAULT_FORECAST_UPLIFT_PCT}
            for product, param in FORECAST_PRODUCT_PARAM_MAP.items()
        ],
    }


def choose_baseline_window(
    max_date: pd.Timestamp | None,
    baseline_mode: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    custom_start_date: pd.Timestamp | None = None,
    custom_end_date: pd.Timestamp | None = None,
) -> tuple[pd.Timestamp, pd.Timestamp, str]:
    latest = pd.Timestamp(max_date or end_date).normalize()
    if baseline_mode == "last_30_days":
        baseline_end = latest
        baseline_start = latest - pd.Timedelta(days=29)
        return baseline_start, baseline_end, "Last 30 Days"
    if baseline_mode == "last_90_days":
        baseline_end = latest
        baseline_start = latest - pd.Timedelta(days=89)
        return baseline_start, baseline_end, "Last 90 Days"
    if baseline_mode == "custom_range":
        custom_start = pd.Timestamp(custom_start_date or start_date).normalize()
        custom_end = pd.Timestamp(custom_end_date or end_date).normalize()
        if custom_start > custom_end:
            custom_start, custom_end = custom_end, custom_start
        return custom_start, custom_end, "Custom Range"

    current_month_start = latest.replace(day=1)
    baseline_end = current_month_start - pd.Timedelta(days=1)
    baseline_start = baseline_end.replace(day=1)
    return baseline_start.normalize(), baseline_end.normalize(), "Last Full Month"


def safety_stock_weeks_for_date(target_date: pd.Timestamp | None) -> int:
    if target_date is None:
        return 3
    return 3 if int(pd.Timestamp(target_date).quarter) in (1, 2) else 5


def calculate_planning_row(
    *,
    dashboard_product: str,
    inventory_product: str,
    inventory_snapshot: dict[str, Any],
    units_sold_in_baseline: float,
    baseline_start: pd.Timestamp,
    baseline_end: pd.Timestamp,
    horizon_start: pd.Timestamp,
    horizon_end: pd.Timestamp,
    uplift_pct: float,
) -> dict[str, Any]:
    snapshot_products = inventory_snapshot.get("products", {}) if isinstance(inventory_snapshot, dict) else {}
    inv = snapshot_products.get(inventory_product, {})
    snapshot_date = pd.Timestamp(inventory_snapshot.get("snapshot_date")) if inventory_snapshot.get("snapshot_date") else horizon_start
    baseline_days = max((baseline_end - baseline_start).days + 1, 1)
    horizon_days = max((horizon_end - horizon_start).days + 1, 1)
    on_hand = float(inv.get("on_hand", 0) or 0)
    in_transit = float(inv.get("in_transit", 0) or 0)
    baseline_avg_daily = units_sold_in_baseline / baseline_days if baseline_days else 0.0
    forecast_multiplier = 1 + (uplift_pct / 100.0)
    forecast_daily_demand = baseline_avg_daily * forecast_multiplier
    on_hand_days = (on_hand / forecast_daily_demand) if forecast_daily_demand > 0 else None
    on_hand_stockout_ts = snapshot_date + pd.to_timedelta(math.floor(on_hand_days), unit="D") if on_hand_days is not None else None
    projected_in_transit_arrival_ts = snapshot_date + pd.to_timedelta(INBOUND_TO_FBT_LEAD_DAYS, unit="D") if in_transit > 0 else None
    counted_in_transit = in_transit if (projected_in_transit_arrival_ts is not None and (on_hand_stockout_ts is None or projected_in_transit_arrival_ts <= on_hand_stockout_ts)) else 0.0
    effective_total_supply = on_hand + counted_in_transit
    total_days = (effective_total_supply / forecast_daily_demand) if forecast_daily_demand > 0 else None
    projected_stockout_ts = snapshot_date + pd.to_timedelta(math.floor(total_days), unit="D") if total_days is not None else None
    safety_weeks = safety_stock_weeks_for_date(horizon_start)
    safety_stock_units = forecast_daily_demand * safety_weeks * 7 if forecast_daily_demand > 0 else 0.0
    horizon_forecast_units = forecast_daily_demand * horizon_days if forecast_daily_demand > 0 else 0.0
    reorder_quantity = max(0.0, horizon_forecast_units + safety_stock_units - effective_total_supply)
    reorder_date_ts = None
    if projected_stockout_ts is not None and forecast_daily_demand > 0:
        reorder_date_ts = projected_stockout_ts - pd.to_timedelta((safety_weeks * 7) + REORDER_LEAD_DAYS, unit="D")

    if forecast_daily_demand <= 0:
        status = "No demand in baseline"
    elif reorder_quantity <= 0:
        status = "Covered"
    elif reorder_date_ts is not None and reorder_date_ts <= snapshot_date:
        status = "Urgent"
    elif reorder_date_ts is not None and reorder_date_ts <= snapshot_date + pd.Timedelta(days=7):
        status = "Watch"
    else:
        status = "Healthy"

    return {
        "product": dashboard_product,
        "inventory_product": inventory_product,
        "snapshot_date": snapshot_date,
        "baseline_start": baseline_start,
        "baseline_end": baseline_end,
        "baseline_label": "Custom Range" if baseline_start != baseline_end and baseline_start == horizon_start and baseline_end == horizon_end else None,
        "on_hand": on_hand,
        "in_transit": in_transit,
        "counted_in_transit": counted_in_transit,
        "effective_total_supply": effective_total_supply,
        "units_sold_in_window": units_sold_in_baseline,
        "avg_daily_demand": baseline_avg_daily if baseline_avg_daily > 0 else None,
        "forecast_uplift_pct": uplift_pct,
        "forecast_daily_demand": forecast_daily_demand if forecast_daily_demand > 0 else None,
        "forecast_units_in_horizon": horizon_forecast_units if horizon_forecast_units > 0 else None,
        "safety_stock_weeks": safety_weeks,
        "safety_stock_units": safety_stock_units if safety_stock_units > 0 else None,
        "projected_in_transit_arrival_date": projected_in_transit_arrival_ts,
        "days_on_hand": on_hand_days,
        "days_total_supply": total_days,
        "weeks_on_hand": (on_hand_days / 7) if on_hand_days is not None else None,
        "weeks_total_supply": (total_days / 7) if total_days is not None else None,
        "projected_stockout_date": projected_stockout_ts,
        "reorder_date": reorder_date_ts,
        "reorder_quantity": reorder_quantity if reorder_quantity > 0 else 0.0,
        "status": status,
    }
