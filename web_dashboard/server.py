from __future__ import annotations

import cgi
import json
import math
import os
import re
from dataclasses import dataclass
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd

from deployment.hosted_uploads import hosted_uploads_enabled, upload_hosted_file
from web_dashboard.demand_planning import (
    DEFAULT_FORECAST_UPLIFT_PCT,
    FORECAST_PRODUCT_PARAM_MAP,
    calculate_planning_row,
    choose_baseline_window,
    planning_defaults,
    resolve_planning_horizon,
)
from web_dashboard.dashboard_response_cache import DashboardResponseCache
from web_dashboard.file_replacement import keep_latest_file_rows_by_date
from web_dashboard.upload_helpers import UPLOAD_TARGET_FOLDERS, sanitize_upload_filename, upload_directory_for_kind


BASE_DIR = Path(__file__).resolve().parent.parent
WEB_DIR = Path(__file__).resolve().parent
DATA_ROOT_ENV = "DASHBOARD_DATA_ROOT"
DEFAULT_OUTPUT_DIR = BASE_DIR / "analysis_output"
CACHE_FILE = WEB_DIR / ".dashboard_cache.pkl"
CACHE_SCHEMA_VERSION = 2
STATEMENT_CACHE_FILE = WEB_DIR / ".statement_cache.pkl"
STATEMENT_CACHE_SCHEMA_VERSION = 2
INVENTORY_CACHE_FILE = WEB_DIR / ".inventory_sheet_cache.pkl"
INVENTORY_CACHE_SCHEMA_VERSION = 1
ZIP_REFERENCE_FILE = WEB_DIR / "data" / "us_zip_centroids.csv"
INVENTORY_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/1hAjW1gbDd-UJgTfS4Bb2QyOwGo9F53nQebjRBHJz9K8/export?format=csv&gid=536853877"
ORDER_SOURCE_FOLDERS = {
    "All orders": "Sales",
    "Samples": "Samples",
    "Replacements": "Replacements",
}
STATEMENT_SOURCE_NAMES = [
    "Statements",
    "Statement",
    "Finance",
    "Finance Tab",
    "Finance statements",
    "Statements data",
    "TikTok Statements",
]
COGS_MAP = {
    "Birria Bomb 2-Pack": {"list_price": 19.99, "cogs": 3.10},
    "Pozole Bomb 2-Pack": {"list_price": 19.99, "cogs": 3.05},
    "Tinga Bomb 2-Pack": {"list_price": 19.99, "cogs": 3.15},
    "Pozole Verde Bomb 2-Pack": {"list_price": 19.99, "cogs": 3.75},
    "Brine Bomb": {"list_price": 19.99, "cogs": 4.20},
    "Variety Pack": {"list_price": 49.99, "cogs": 13.35},
}
STATE_ABBREVIATIONS = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
    "DISTRICT OF COLUMBIA": "DC",
}
STATEMENT_USECOLS = {
    "Statement date",
    "Type",
    "Order/adjustment ID",
    "Related order ID",
    "Related order ID  ",
    "Amount",
    "Statement Amount",
    "Net Amount",
    "Posting Amount",
    "Total settlement amount",
    "Net sales",
    "Gross sales",
    "Gross sales refund",
    "Seller discount",
    "Seller discount refund",
    "Shipping",
    "TikTok Shop shipping fee",
    "Fulfilled by TikTok Shop shipping fee",
    "Customer-paid shipping fee",
    "Customer-paid shipping fee refund",
    "TikTok Shop shipping incentive",
    "TikTok Shop shipping incentive refund",
    "Shipping fee subsidy",
    "Return shipping fee",
    "FBT fulfillment fee",
    "Customer shipping fee offset",
    "Shipping fee discount",
    "Return shipping label fee",
    "FBT fulfillment fee reimbursement",
    "Fees",
    "Transaction fee",
    "Referral fee",
    "Refund administration fee",
    "Affiliate Commission",
    "Affiliate partner commission",
    "Affiliate Shop Ads commission",
    "Affiliate Partner shop ads commission",
    "TikTok Shop Partner commission",
    "Co-funded promotion (seller-funded)",
    "Campaign service fee",
    "Smart Promotion fee",
    "Marketing benefits package fee",
    "Adjustment amount",
    "Logistics reimbursement",
    "GMV deduction for FBT warehouse service fee",
    "TikTok Shop reimbursement",
    "Adjustment reason",
    "Customer payment",
    "Customer refund",
    "Seller co-funded voucher discount",
    "Seller co-funded voucher discount refund",
    "Platform discounts",
    "Platform discounts refund",
    "Sales tax payment",
    "Sales tax refund",
}
MONTH_NAME_TO_NUMBER = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

STORE: "DataStore | None" = None
DASHBOARD_RESPONSE_CACHE = DashboardResponseCache()
TIKTOK_INVENTORY_FIELDS = {
    "Birria": ("birria_in_transit", "birria_on_hand"),
    "Pozole": ("pozole_in_transit", "pozole_on_hand"),
    "Tinga": ("tinga_in_transit", "tinga_on_hand"),
    "Brine": ("brine_in_transit", "brine_on_hand"),
    "Variety Pack": ("variety_pack_in_transit", "variety_pack_on_hand"),
    "Pozole Verde": ("pozole_verde_in_transit", "pozole_verde_on_hand"),
}
INVENTORY_PRODUCT_KEY_MAP = {
    "Birria Bomb 2-Pack": "Birria",
    "Pozole Bomb 2-Pack": "Pozole",
    "Tinga Bomb 2-Pack": "Tinga",
    "Brine Bomb": "Brine",
    "Variety Pack": "Variety Pack",
    "Pozole Verde Bomb 2-Pack": "Pozole Verde",
}


def source_base_dir() -> Path:
    override = os.getenv(DATA_ROOT_ENV)
    if override:
        return Path(override).resolve()
    return BASE_DIR


def json_safe(value: Any) -> Any:
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)
    return value


def records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    output: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        output.append({key: json_safe(value) for key, value in row.items()})
    return output


def normalize_zip(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(text) >= 5:
        return text[:5]
    return ""


def normalize_state(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if len(text) == 2:
        return text
    return STATE_ABBREVIATIONS.get(text, text)


def infer_month_start_from_filename(filename: str) -> pd.Timestamp | pd.NaT:
    text = Path(filename).stem.strip().lower()
    match = re.search(r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})", text)
    if not match:
        return pd.NaT
    month = MONTH_NAME_TO_NUMBER.get(match.group(1))
    year = int(match.group(2))
    if not month:
        return pd.NaT
    return pd.Timestamp(year=year, month=month, day=1)


def normalized_column_name(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else " " for ch in str(value)).split()


def normalized_column_text(value: str) -> str:
    return " ".join(normalized_column_name(value))


def parse_numeric(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    cleaned = text.replace(",", "").replace("$", "").replace("(", "-").replace(")", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def pick_column(columns: list[str], exact_candidates: list[str], keyword_groups: list[tuple[str, ...]]) -> str | None:
    normalized_map = {column: normalized_column_text(column) for column in columns}
    normalized_exact = {normalized_column_text(candidate) for candidate in exact_candidates}
    for column, normalized in normalized_map.items():
        if normalized in normalized_exact:
            return column
    for keywords in keyword_groups:
        for column, normalized in normalized_map.items():
            if all(keyword in normalized for keyword in keywords):
                return column
    return None


def build_tiktok_inventory_history_from_sheet(raw_sheet: pd.DataFrame) -> pd.DataFrame:
    if raw_sheet is None or raw_sheet.empty or len(raw_sheet.index) < 4:
        return pd.DataFrame()

    header_channel = raw_sheet.iloc[0].ffill()
    header_product = raw_sheet.iloc[1].ffill()
    header_metric = raw_sheet.iloc[2]
    field_map: dict[int, str] = {}
    for idx in range(1, len(raw_sheet.columns)):
        channel = str(header_channel.iloc[idx] or "").strip()
        product = str(header_product.iloc[idx] or "").strip()
        metric = str(header_metric.iloc[idx] or "").strip()
        if channel != "TikTok":
            continue
        if product not in TIKTOK_INVENTORY_FIELDS:
            continue
        if metric == "In Transit":
            field_map[idx] = TIKTOK_INVENTORY_FIELDS[product][0]
        elif metric == "On Hand":
            field_map[idx] = TIKTOK_INVENTORY_FIELDS[product][1]

    records_out: list[dict[str, Any]] = []
    for row_idx in range(3, len(raw_sheet.index)):
        date_value = pd.to_datetime(raw_sheet.iat[row_idx, 0], errors="coerce")
        if pd.isna(date_value):
            continue
        record: dict[str, Any] = {"date": pd.Timestamp(date_value).normalize()}
        valid_metric_count = 0
        for col_idx, field_name in field_map.items():
            numeric = parse_numeric(raw_sheet.iat[row_idx, col_idx])
            record[field_name] = numeric
            if numeric is not None:
                valid_metric_count += 1
        record["valid_metric_count"] = valid_metric_count
        records_out.append(record)

    if not records_out:
        return pd.DataFrame()
    history = pd.DataFrame(records_out).sort_values("date").reset_index(drop=True)
    for field_pair in TIKTOK_INVENTORY_FIELDS.values():
        for field_name in field_pair:
            if field_name not in history.columns:
                history[field_name] = None
    return history[
        ["date", *[field for pair in TIKTOK_INVENTORY_FIELDS.values() for field in pair], "valid_metric_count"]
    ].copy()


def latest_tiktok_inventory_snapshot(history: pd.DataFrame) -> dict[str, Any]:
    if history is None or history.empty:
        return {"snapshot_date": None, "valid_metric_count": 0, "products": {}}
    valid_rows = history.loc[history["valid_metric_count"].fillna(0) > 0].sort_values("date")
    if valid_rows.empty:
        return {"snapshot_date": None, "valid_metric_count": 0, "products": {}}
    latest = valid_rows.iloc[-1]
    products: dict[str, dict[str, float | None]] = {}
    for product_name, (in_transit_field, on_hand_field) in TIKTOK_INVENTORY_FIELDS.items():
        in_transit = latest.get(in_transit_field)
        on_hand = latest.get(on_hand_field)
        total_supply = (in_transit or 0) + (on_hand or 0) if (in_transit is not None or on_hand is not None) else None
        products[product_name] = {
            "in_transit": float(in_transit) if in_transit is not None else None,
            "on_hand": float(on_hand) if on_hand is not None else None,
            "total_supply": float(total_supply) if total_supply is not None else None,
        }
    return {
        "snapshot_date": pd.Timestamp(latest["date"]),
        "valid_metric_count": int(latest.get("valid_metric_count") or 0),
        "products": products,
    }


def load_tiktok_inventory_history() -> pd.DataFrame:
    cache_max_age_seconds = 900
    if INVENTORY_CACHE_FILE.exists():
        cache_age = pd.Timestamp.now().timestamp() - INVENTORY_CACHE_FILE.stat().st_mtime
        if cache_age <= cache_max_age_seconds:
            cached = pd.read_pickle(INVENTORY_CACHE_FILE)
            if isinstance(cached, dict) and cached.get("cache_schema_version") == INVENTORY_CACHE_SCHEMA_VERSION:
                return cached.get("history", pd.DataFrame())
    try:
        raw_sheet = pd.read_csv(INVENTORY_SHEET_CSV_URL, header=None)
        history = build_tiktok_inventory_history_from_sheet(raw_sheet)
        pd.to_pickle({"cache_schema_version": INVENTORY_CACHE_SCHEMA_VERSION, "history": history}, INVENTORY_CACHE_FILE)
        return history
    except Exception:
        if INVENTORY_CACHE_FILE.exists():
            cached = pd.read_pickle(INVENTORY_CACHE_FILE)
            if isinstance(cached, dict):
                return cached.get("history", pd.DataFrame())
        return pd.DataFrame()


def build_inventory_planning_rows(
    component_cogs_df: pd.DataFrame,
    inventory_snapshot: dict[str, Any],
    horizon_start: pd.Timestamp,
    horizon_end: pd.Timestamp,
    baseline_start: pd.Timestamp,
    baseline_end: pd.Timestamp,
    uplift_overrides: dict[str, float],
    baseline_label: str,
) -> pd.DataFrame:
    demand_lookup: dict[str, float] = {}
    if component_cogs_df is not None and not component_cogs_df.empty:
        for _, row in component_cogs_df.iterrows():
            mapped_name = INVENTORY_PRODUCT_KEY_MAP.get(str(row.get("product", "")))
            if mapped_name:
                demand_lookup[mapped_name] = float(row.get("component_units_sold", 0) or 0)

    rows: list[dict[str, Any]] = []
    for dashboard_product, inventory_name in INVENTORY_PRODUCT_KEY_MAP.items():
        units_sold = float(demand_lookup.get(inventory_name, 0) or 0)
        uplift_pct = float(uplift_overrides.get(dashboard_product, DEFAULT_FORECAST_UPLIFT_PCT))
        planning_row = calculate_planning_row(
            dashboard_product=dashboard_product,
            inventory_product=inventory_name,
            inventory_snapshot=inventory_snapshot,
            units_sold_in_baseline=units_sold,
            baseline_start=baseline_start,
            baseline_end=baseline_end,
            horizon_start=horizon_start,
            horizon_end=horizon_end,
            uplift_pct=uplift_pct,
        )
        planning_row["baseline_label"] = baseline_label
        rows.append(planning_row)
    return pd.DataFrame(rows).sort_values(["reorder_quantity", "weeks_on_hand", "product"], ascending=[False, True, True], na_position="last").reset_index(drop=True)


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 3958.8
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def detect_product_components(product_name: str) -> tuple[list[tuple[str, int]], str]:
    name = (product_name or "").lower()
    if not name:
        return [], "No product name"
    if "4-flavor variety pack" in name or ("variety pack" in name and "pozole verde" in name):
        return [
            ("Birria Bomb 2-Pack", 1),
            ("Pozole Bomb 2-Pack", 1),
            ("Pozole Verde Bomb 2-Pack", 1),
            ("Tinga Bomb 2-Pack", 1),
        ], "4-flavor variety pack mapped to one of each named 2-pack"
    if "variety pack" in name:
        return [("Variety Pack", 1)], "Mapped as fixed Variety Pack COGS"

    flavors: list[str] = []
    has_pozole_verde = "pozole verde" in name
    has_regular_pozole = (
        "pozole verde and pozole" in name
        or "pozole verde + pozole" in name
        or ("pozole" in name and "pozole verde" not in name)
    )
    if has_pozole_verde:
        flavors.append("Pozole Verde Bomb 2-Pack")
    if "birria" in name:
        flavors.append("Birria Bomb 2-Pack")
    if "tinga" in name:
        flavors.append("Tinga Bomb 2-Pack")
    if "brine" in name:
        flavors.append("Brine Bomb")
    if has_regular_pozole:
        flavors.append("Pozole Bomb 2-Pack")

    seen: set[str] = set()
    normalized: list[str] = []
    for flavor in flavors:
        if flavor not in seen:
            normalized.append(flavor)
            seen.add(flavor)

    if not normalized:
        return [], "No COGS mapping matched product title"
    if "bundle" in name:
        if len(normalized) == 1:
            return [(normalized[0], 2)], "Single-flavor bundle assumed to contain 2 of the same 2-pack"
        return [(flavor, 1) for flavor in normalized], "Mixed bundle assumed to contain 1 of each named 2-pack"
    return [(normalized[0], 1)], "Standard listing mapped to one 2-pack"


def canonical_item_name(product_name: str) -> str:
    name = (product_name or "").lower()
    if "variety pack" in name and "pozole verde" in name:
        return "4-Flavor Variety Pack"
    if "variety pack" in name:
        return "Variety Pack"
    if "pozole verde" in name:
        if "bundle" in name:
            if "birria" in name:
                return "Pozole Verde + Birria Bundle"
            if "tinga" in name:
                return "Pozole Verde + Tinga Bundle"
            if "pozole verde and pozole" in name or "pozole verde + pozole" in name:
                return "Pozole Verde + Pozole Bundle"
            return "Pozole Verde Bundle"
        return "Pozole Verde Bomb 2-Pack"
    if "brine" in name:
        return "Brine Bomb"
    has_birria = "birria" in name
    has_pozole = "pozole" in name
    has_tinga = "tinga" in name
    if "bundle" in name:
        parts = []
        if has_birria:
            parts.append("Birria")
        if has_pozole:
            parts.append("Pozole")
        if has_tinga:
            parts.append("Tinga")
        if len(parts) == 1:
            return f"{parts[0]} Bundle"
        if parts:
            return " + ".join(parts) + " Bundle"
    if has_birria:
        return "Birria Bomb 2-Pack"
    if has_pozole:
        return "Pozole Bomb 2-Pack"
    if has_tinga:
        return "Tinga Bomb 2-Pack"
    return product_name or "Unmapped Item"


def build_cogs_views(product_df: pd.DataFrame | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    if product_df is None or product_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    listing_rows: list[dict[str, Any]] = []
    component_rows: list[dict[str, Any]] = []
    for _, row in product_df.iterrows():
        product_name = str(row.get("product_name", ""))
        units_sold = float(row.get("units_sold", 0) or 0)
        net_sales = float(row.get("net_merchandise_sales", 0) or 0)
        components, assumption = detect_product_components(product_name)
        estimated_cogs = 0.0
        mapped_component_units = 0.0
        for component_name, qty in components:
            component_units = units_sold * qty
            component_cogs = COGS_MAP[component_name]["cogs"] * component_units
            estimated_cogs += component_cogs
            mapped_component_units += component_units
            component_rows.append(
                {
                    "product": component_name,
                    "component_units_sold": component_units,
                    "list_price": COGS_MAP[component_name]["list_price"],
                    "unit_cogs": COGS_MAP[component_name]["cogs"],
                    "estimated_cogs": component_cogs,
                }
            )
        listing_rows.append(
            {
                "listing": product_name,
                "listing_sku": row.get("seller_sku_resolved"),
                "units_sold": units_sold,
                "mapped_component_units": mapped_component_units,
                "net_merchandise_sales": net_sales,
                "estimated_cogs": estimated_cogs if components else None,
                "estimated_gross_profit": (net_sales - estimated_cogs) if components else None,
                "cogs_assumption": assumption,
            }
        )

    listing_df = pd.DataFrame(listing_rows).sort_values("net_merchandise_sales", ascending=False)
    component_df = pd.DataFrame(component_rows)
    if not component_df.empty:
        component_df = (
            component_df.groupby("product", as_index=False)
            .agg(
                component_units_sold=("component_units_sold", "sum"),
                list_price=("list_price", "max"),
                unit_cogs=("unit_cogs", "max"),
                estimated_cogs=("estimated_cogs", "sum"),
            )
            .sort_values("estimated_cogs", ascending=False)
        )
    return listing_df, component_df


def build_filtered_product_view(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    working = raw_df.copy()
    working["canonical_item_name"] = working["Product Name"].map(canonical_item_name)
    working["bundle_units_sold"] = working["Quantity"].where(working["is_virtual_bundle_listing"], 0)
    if "seller_sku_resolved" not in working.columns:
        working["seller_sku_resolved"] = working.get("Seller SKU", pd.Series("", index=working.index))
    grouped = (
        working.groupby(["canonical_item_name", "seller_sku_resolved"], dropna=False, as_index=False)
        .agg(
            order_count=("Order ID", pd.Series.nunique),
            units_sold=("Quantity", "sum"),
            returned_units=("Sku Quantity of return", "sum"),
            gross_merchandise_sales=("SKU Subtotal Before Discount", "sum"),
            seller_discount=("SKU Seller Discount", "sum"),
            platform_discount=("SKU Platform Discount", "sum"),
            net_merchandise_sales=("SKU Subtotal After Discount", "sum"),
            virtual_bundle_units=("is_virtual_bundle_listing", "sum"),
            bundle_units_sold=("bundle_units_sold", "sum"),
        )
        .rename(columns={"canonical_item_name": "product_name"})
        .sort_values("net_merchandise_sales", ascending=False)
        .reset_index(drop=True)
    )
    grouped["return_rate_by_units"] = grouped.apply(
        lambda row: row["returned_units"] / row["units_sold"] if row["units_sold"] else None,
        axis=1,
    )
    units_by_source = (
        working.groupby(["canonical_item_name", "seller_sku_resolved", "source_type"], dropna=False, as_index=False)
        .agg(source_units=("Quantity", "sum"))
        .pivot(index=["canonical_item_name", "seller_sku_resolved"], columns="source_type", values="source_units")
        .fillna(0)
        .reset_index()
        .rename(
            columns={
                "canonical_item_name": "product_name",
                "Sales": "sales_units",
                "Samples": "sample_units",
                "Replacements": "replacement_units",
            }
        )
    )
    for col in ["sales_units", "sample_units", "replacement_units"]:
        if col not in units_by_source.columns:
            units_by_source[col] = 0.0
    grouped = grouped.merge(
        units_by_source[["product_name", "seller_sku_resolved", "sales_units", "sample_units", "replacement_units"]],
        on=["product_name", "seller_sku_resolved"],
        how="left",
    )
    grouped[["sales_units", "sample_units", "replacement_units"]] = grouped[
        ["sales_units", "sample_units", "replacement_units"]
    ].fillna(0)
    sales_only = working.loc[working["source_type"].eq("Sales")].copy()
    if not sales_only.empty:
        solo_order_candidates = (
            sales_only.groupby("Order ID", as_index=False)
            .agg(
                order_product_count=("seller_sku_resolved", pd.Series.nunique),
                order_total_units=("Quantity", "sum"),
                shipping_fee_after_discount=("Shipping Fee After Discount", "max"),
            )
        )
        eligible_order_ids = solo_order_candidates.loc[
            solo_order_candidates["order_product_count"].eq(1) & solo_order_candidates["order_total_units"].eq(1),
            "Order ID",
        ]
        solo_shipping = (
            sales_only.loc[sales_only["Order ID"].isin(eligible_order_ids)]
            .groupby(["canonical_item_name", "seller_sku_resolved"], dropna=False, as_index=False)
            .agg(
                solo_shipping_orders=("Order ID", pd.Series.nunique),
                solo_shipping_total=("Shipping Fee After Discount", "sum"),
                solo_units_sold=("Quantity", "sum"),
            )
            .rename(columns={"canonical_item_name": "product_name"})
        )
        solo_shipping["solo_avg_shipping_per_unit"] = solo_shipping.apply(
            lambda row: row["solo_shipping_total"] / row["solo_units_sold"] if row["solo_units_sold"] else None,
            axis=1,
        )
        solo_shipping["solo_avg_shipping_per_sku"] = solo_shipping.apply(
            lambda row: row["solo_shipping_total"] / row["solo_shipping_orders"] if row["solo_shipping_orders"] else None,
            axis=1,
        )
        grouped = grouped.merge(
            solo_shipping[
                [
                    "product_name",
                    "seller_sku_resolved",
                    "solo_shipping_orders",
                    "solo_avg_shipping_per_unit",
                    "solo_avg_shipping_per_sku",
                ]
            ],
            on=["product_name", "seller_sku_resolved"],
            how="left",
        )
    else:
        grouped["solo_shipping_orders"] = 0.0
        grouped["solo_avg_shipping_per_unit"] = None
        grouped["solo_avg_shipping_per_sku"] = None
    if "solo_shipping_orders" not in grouped.columns:
        grouped["solo_shipping_orders"] = 0.0
    grouped["solo_shipping_orders"] = grouped["solo_shipping_orders"].fillna(0.0)
    return grouped


def build_product_daily_view(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    working = raw_df.copy()
    working["canonical_item_name"] = working["Product Name"].map(canonical_item_name)
    working["bundle_units_sold"] = working["Quantity"].where(working["is_virtual_bundle_listing"], 0)
    if "seller_sku_resolved" not in working.columns:
        working["seller_sku_resolved"] = working.get("Seller SKU", pd.Series("", index=working.index))
    grouped = (
        working.groupby(["reporting_date", "canonical_item_name", "seller_sku_resolved"], dropna=False, as_index=False)
        .agg(
            order_count=("Order ID", pd.Series.nunique),
            units_sold=("Quantity", "sum"),
            returned_units=("Sku Quantity of return", "sum"),
            gross_merchandise_sales=("SKU Subtotal Before Discount", "sum"),
            seller_discount=("SKU Seller Discount", "sum"),
            platform_discount=("SKU Platform Discount", "sum"),
            net_merchandise_sales=("SKU Subtotal After Discount", "sum"),
            virtual_bundle_units=("is_virtual_bundle_listing", "sum"),
            bundle_units_sold=("bundle_units_sold", "sum"),
        )
        .rename(columns={"canonical_item_name": "product_name"})
        .sort_values(["reporting_date", "net_merchandise_sales"], ascending=[False, False])
        .reset_index(drop=True)
    )
    grouped["return_rate_by_units"] = grouped.apply(
        lambda row: row["returned_units"] / row["units_sold"] if row["units_sold"] else None,
        axis=1,
    )
    units_by_source = (
        working.groupby(["reporting_date", "canonical_item_name", "seller_sku_resolved", "source_type"], dropna=False, as_index=False)
        .agg(source_units=("Quantity", "sum"))
        .pivot(index=["reporting_date", "canonical_item_name", "seller_sku_resolved"], columns="source_type", values="source_units")
        .fillna(0)
        .reset_index()
        .rename(
            columns={
                "canonical_item_name": "product_name",
                "Sales": "sales_units",
                "Samples": "sample_units",
                "Replacements": "replacement_units",
            }
        )
    )
    for col in ["sales_units", "sample_units", "replacement_units"]:
        if col not in units_by_source.columns:
            units_by_source[col] = 0.0
    grouped = grouped.merge(
        units_by_source[["reporting_date", "product_name", "seller_sku_resolved", "sales_units", "sample_units", "replacement_units"]],
        on=["reporting_date", "product_name", "seller_sku_resolved"],
        how="left",
    )
    grouped[["sales_units", "sample_units", "replacement_units"]] = grouped[
        ["sales_units", "sample_units", "replacement_units"]
    ].fillna(0)
    return grouped


def build_order_level_geo_view(order_level_df: pd.DataFrame, zip_reference: pd.DataFrame) -> pd.DataFrame:
    if order_level_df is None or order_level_df.empty:
        return pd.DataFrame()
    working = order_level_df.copy()
    if zip_reference is not None and not zip_reference.empty:
        working = working.merge(
            zip_reference[["zip", "latitude", "longitude"]],
            left_on="zipcode",
            right_on="zip",
            how="left",
        ).drop(columns=["zip"], errors="ignore")
    return working


def build_raw_product_name_view(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    grouped = (
        raw_df.groupby("Product Name", dropna=False, as_index=False)
        .agg(
            order_count=("Order ID", pd.Series.nunique),
            units_sold=("Quantity", "sum"),
        )
        .rename(columns={"Product Name": "product_name"})
        .sort_values("units_sold", ascending=False)
        .reset_index(drop=True)
    )
    return grouped


def build_raw_product_name_rows(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    working = raw_df.copy()
    working["reporting_date"] = pd.to_datetime(working.get("reporting_date"), errors="coerce")
    working["product_name"] = working.get("Product Name", pd.Series("", index=working.index)).astype("string").fillna("").str.strip()
    working["order_id"] = working.get("Order ID", pd.Series("", index=working.index)).astype("string").fillna("").str.strip()
    working["source_type"] = working.get("source_type", pd.Series("", index=working.index)).astype("string").fillna("").str.strip()
    working["units_sold"] = pd.to_numeric(working.get("Quantity", pd.Series(0, index=working.index)), errors="coerce").fillna(0.0)
    working = working.loc[working["product_name"].ne("")].copy()
    return working[["reporting_date", "product_name", "order_id", "source_type", "units_sold"]].reset_index(drop=True)


def build_math_audit(filtered_operational_df: pd.DataFrame, filtered_product_df: pd.DataFrame, component_cogs_df: pd.DataFrame) -> list[dict[str, Any]]:
    raw_listing_units = float(filtered_operational_df["Quantity"].sum()) if filtered_operational_df is not None and not filtered_operational_df.empty else 0.0
    canonical_listing_units = float(filtered_product_df["units_sold"].sum()) if filtered_product_df is not None and not filtered_product_df.empty else 0.0
    bundle_listing_units = float(filtered_product_df["bundle_units_sold"].sum()) if filtered_product_df is not None and not filtered_product_df.empty and "bundle_units_sold" in filtered_product_df.columns else 0.0
    expanded_component_units = float(component_cogs_df["component_units_sold"].sum()) if component_cogs_df is not None and not component_cogs_df.empty else 0.0
    inferred_paid_time_units = (
        float(filtered_operational_df.loc[filtered_operational_df["paid_time_inferred_from_file_month"].fillna(False), "Quantity"].sum())
        if filtered_operational_df is not None and not filtered_operational_df.empty and "paid_time_inferred_from_file_month" in filtered_operational_df.columns
        else 0.0
    )
    return [
        {"metric": "Raw listing units", "value": raw_listing_units, "note": "SUM(Quantity) from filtered raw order lines using original product rows"},
        {"metric": "Canonical listing units", "value": canonical_listing_units, "note": "SUM(Quantity) after title normalization; should match raw listing units"},
        {"metric": "Units from bundle listings", "value": bundle_listing_units, "note": "Subset of listing units where Virtual Bundle Seller SKU was present"},
        {"metric": "Expanded physical product units", "value": expanded_component_units, "note": "Units after expanding virtual bundles into actual products sent to TikTok"},
        {"metric": "Units inferred from file month", "value": inferred_paid_time_units, "note": "Rows missing Paid Time but included using the monthly source filename"},
    ]


def apply_order_bucket_mode(raw_df: pd.DataFrame, order_bucket_mode: str) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame() if raw_df is None else raw_df.copy()
    working = raw_df.copy()
    paid_time_date = pd.to_datetime(working.get("paid_time_date"), errors="coerce")
    source_file_month = pd.to_datetime(working.get("source_file_month"), errors="coerce")
    working["paid_time_date"] = paid_time_date
    working["source_file_month"] = source_file_month
    working["paid_time_inferred_from_file_month"] = paid_time_date.isna() & source_file_month.notna()
    working["outside_source_file_month"] = (
        paid_time_date.notna()
        & source_file_month.notna()
        & (paid_time_date.dt.to_period("M") != source_file_month.dt.to_period("M"))
    )
    if order_bucket_mode == "file_month":
        working["reporting_date"] = source_file_month.fillna(paid_time_date)
    else:
        working["reporting_date"] = paid_time_date.fillna(source_file_month)
    return working.loc[working["reporting_date"].notna()].copy()


def build_order_export_finance_view(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    line_daily = (
        raw_df.groupby(["reporting_date", "source_type"], as_index=False)
        .agg(
            gross_product_sales=("SKU Subtotal Before Discount", "sum"),
            seller_discount=("SKU Seller Discount", "sum"),
            platform_discount=("SKU Platform Discount", "sum"),
            product_sales_after_all_discounts=("SKU Subtotal After Discount", "sum"),
        )
        .sort_values("reporting_date")
    )
    line_daily["product_sales_after_seller_discount"] = line_daily["gross_product_sales"] - line_daily["seller_discount"]
    order_daily = (
        raw_df.groupby(["Order ID", "source_type"], as_index=False)
        .agg(
            reporting_date=("reporting_date", "min"),
            collected_order_amount=("Order Amount", "max"),
            order_refund_amount=("Order Refund Amount", "max"),
            taxes=("Taxes", "max"),
            shipping_fee_after_discount=("Shipping Fee After Discount", "max"),
        )
        .groupby(["reporting_date", "source_type"], as_index=False)
        .agg(
            paid_orders=("Order ID", "count"),
            collected_order_amount=("collected_order_amount", "sum"),
            order_refund_amount=("order_refund_amount", "sum"),
            taxes=("taxes", "sum"),
            shipping_fee_after_discount=("shipping_fee_after_discount", "sum"),
        )
        .sort_values("reporting_date")
    )
    daily = line_daily.merge(order_daily, on=["reporting_date", "source_type"], how="outer").fillna(0)
    daily["approx_product_sales_after_export_refunds"] = daily["product_sales_after_seller_discount"] - daily["order_refund_amount"]
    return daily.sort_values("reporting_date").reset_index(drop=True)


def build_data_quality_summary(raw_df: pd.DataFrame, order_bucket_mode: str) -> dict[str, Any]:
    if raw_df is None or raw_df.empty:
        return {
            "status": "no-data",
            "total_units": 0.0,
            "inferred_units": 0.0,
            "spillover_units": 0.0,
            "spillover_rows": 0,
            "mismatch_units_under_current_mode": 0.0,
            "mismatch_pct_under_current_mode": 0.0,
            "order_bucket_mode": order_bucket_mode,
            "message": "No order rows for the selected filters.",
        }
    total_units = float(raw_df["Quantity"].sum())
    inferred_units = float(raw_df.loc[raw_df["paid_time_inferred_from_file_month"].fillna(False), "Quantity"].sum())
    spillover_units = float(raw_df.loc[raw_df["outside_source_file_month"].fillna(False), "Quantity"].sum())
    spillover_rows = int(raw_df["outside_source_file_month"].fillna(False).sum())
    mismatch_units = spillover_units if order_bucket_mode == "file_month" else inferred_units
    mismatch_pct = (mismatch_units / total_units) if total_units else 0.0
    if mismatch_pct <= 0.0005:
        status = "clean"
    elif mismatch_pct <= 0.005:
        status = "minor"
    else:
        status = "material"
    if order_bucket_mode == "file_month":
        message = "Order rows are bucketed by the source export file month for exact pivot matching."
    else:
        message = "Order rows are bucketed by actual paid time, falling back to file month only when Paid Time is blank."
    return {
        "status": status,
        "total_units": total_units,
        "inferred_units": inferred_units,
        "spillover_units": spillover_units,
        "spillover_rows": spillover_rows,
        "mismatch_units_under_current_mode": mismatch_units,
        "mismatch_pct_under_current_mode": mismatch_pct,
        "order_bucket_mode": order_bucket_mode,
        "message": message,
    }


def build_statement_expense_structure(statement_rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if statement_rows is None or statement_rows.empty:
        empty = pd.DataFrame(columns=["category", "amount"])
        return empty, pd.DataFrame(columns=["category", "line_item", "amount"])

    line_defs = [
        ("Shipping", "Customer-paid shipping fee", "statement_customer_paid_shipping_fee"),
        ("Shipping", "Customer-paid shipping fee refund", "statement_customer_paid_shipping_fee_refund"),
        ("Shipping", "TikTok Shop shipping incentive", "statement_tiktok_shipping_incentive"),
        ("Shipping", "TikTok Shop shipping incentive refund", "statement_tiktok_shipping_incentive_refund"),
        ("Shipping", "TikTok Shop shipping fee", "statement_tiktok_shipping_fee"),
        ("Shipping", "FBT shipping fee", "statement_fbt_shipping_fee"),
        ("Shipping", "Shipping fee subsidy", "statement_shipping_fee_subsidy"),
        ("Shipping", "Return shipping fee", "statement_return_shipping_fee"),
        ("Shipping", "FBT fulfillment fee", "statement_fbt_fulfillment_fee"),
        ("Shipping", "Customer shipping fee offset", "statement_customer_shipping_fee_offset"),
        ("Shipping", "Shipping fee discount", "statement_shipping_fee_discount"),
        ("Shipping", "Return shipping label fee", "statement_return_shipping_label_fee"),
        ("Shipping", "FBT fulfillment fee reimbursement", "statement_fbt_fulfillment_fee_reimbursement"),
        ("Product discount", "Seller discount", "statement_seller_discount"),
        ("Product discount", "Seller discount refund", "statement_seller_discount_refund"),
        ("Product discount", "Seller voucher discount", "statement_seller_voucher_discount"),
        ("Product discount", "Seller voucher discount refund", "statement_seller_voucher_discount_refund"),
        ("Affiliate", "Affiliate Commission", "statement_affiliate_commission"),
        ("Affiliate", "Affiliate partner commission", "statement_affiliate_partner_commission"),
        ("Affiliate", "Affiliate Shop Ads commission", "statement_affiliate_shop_ads_commission"),
        ("Affiliate", "Affiliate Partner shop ads commission", "statement_affiliate_partner_shop_ads_commission"),
        ("Affiliate", "TikTok Shop Partner commission", "statement_tiktok_partner_commission"),
        ("Operation", "Transaction fee", "statement_transaction_fee"),
        ("Operation", "Referral fee", "statement_referral_fee"),
        ("Operation", "Refund administration fee", "statement_refund_admin_fee"),
        ("Marketing", "Campaign service fee", "statement_campaign_service_fee"),
        ("Marketing", "Smart Promotion fee", "statement_smart_promotion_fee"),
        ("Marketing", "Marketing benefits package fee", "statement_marketing_benefits_package_fee"),
        ("Marketing", "Co-funded promotion (seller-funded)", "statement_cofunded_promotion"),
    ]
    detail_rows: list[dict[str, Any]] = []
    for category, label, column in line_defs:
        if column not in statement_rows.columns:
            continue
        amount = statement_rows[column].sum(min_count=1)
        if pd.isna(amount) or abs(float(amount)) < 0.005:
            continue
        detail_rows.append({"category": category, "line_item": label, "amount": float(amount)})
    detail_df = pd.DataFrame(detail_rows)
    if detail_df.empty:
        empty = pd.DataFrame(columns=["category", "amount"])
        return empty, pd.DataFrame(columns=["category", "line_item", "amount"])
    category_df = (
        detail_df.groupby("category", as_index=False)["amount"]
        .sum()
        .sort_values("amount", key=lambda series: series.abs(), ascending=False)
        .reset_index(drop=True)
    )
    detail_df = detail_df.sort_values(["category", "amount"], key=lambda series: series if series.name == "category" else series.abs(), ascending=[True, False]).reset_index(drop=True)
    return category_df, detail_df


def detect_statement_sources() -> list[Path]:
    found: list[Path] = []
    base_dir = source_base_dir()
    for name in STATEMENT_SOURCE_NAMES:
        candidate = base_dir / name
        if candidate.exists():
            if candidate.is_dir():
                found.extend(sorted(candidate.glob("*.csv")))
                found.extend(sorted(candidate.glob("*.xlsx")))
                found.extend(sorted(candidate.glob("*.xls")))
            else:
                found.append(candidate)
    for pattern in ["*statement*.csv", "*statement*.xlsx", "*finance*.csv", "*finance*.xlsx"]:
        found.extend(sorted(base_dir.glob(pattern)))
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in found:
        if path not in seen and path.is_file():
            unique.append(path)
            seen.add(path)
    return unique


def latest_statement_mtime() -> float:
    latest = 0.0
    for path in detect_statement_sources():
        latest = max(latest, path.stat().st_mtime)
    return latest


MONTH_LOOKUP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def infer_statement_file_range(path: Path) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    matches = re.findall(r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s*_?-?\s*(\d{2,4})", path.stem, flags=re.IGNORECASE)
    if not matches:
        return None, None
    months: list[pd.Timestamp] = []
    for month_text, year_text in matches:
        month = MONTH_LOOKUP.get(month_text.lower())
        if not month:
            continue
        year = int(year_text)
        if year < 100:
            year += 2000
        months.append(pd.Timestamp(year=year, month=month, day=1))
    if not months:
        return None, None
    start = min(months)
    end = max(months) + pd.offsets.MonthEnd(1)
    return start, end


def statement_date_bounds() -> tuple[str | None, str | None]:
    starts: list[pd.Timestamp] = []
    ends: list[pd.Timestamp] = []
    for path in detect_statement_sources():
        start, end = infer_statement_file_range(path)
        if start is not None and end is not None:
            starts.append(start)
            ends.append(end)
    if not starts:
        return None, None
    return min(starts).strftime("%Y-%m-%d"), max(ends).strftime("%Y-%m-%d")


def candidate_statement_files(start_date: pd.Timestamp | None = None, end_date: pd.Timestamp | None = None) -> list[Path]:
    candidates: list[Path] = []
    for path in detect_statement_sources():
        file_start, file_end = infer_statement_file_range(path)
        if start_date is None or end_date is None or file_start is None or file_end is None:
            candidates.append(path)
            continue
        if file_end >= start_date and file_start <= end_date:
            candidates.append(path)
    return candidates


def read_table_file(path: Path) -> pd.DataFrame:
    normalized_usecols = {normalized_column_text(column) for column in STATEMENT_USECOLS}
    include = lambda column: normalized_column_text(column) in normalized_usecols
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False, usecols=include)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        workbook = pd.ExcelFile(path)
        detail_sheet = next(
            (sheet for sheet in workbook.sheet_names if normalized_column_text(sheet) in {"order details", "order detail"}),
            workbook.sheet_names[0],
        )
        return pd.read_excel(path, sheet_name=detail_sheet, dtype=str, usecols=include)
    return pd.DataFrame()


def classify_statement_category(statement_type: str, description: str) -> str:
    text = f"{statement_type} {description}".lower()
    if "fulfillment" in text or "fbt" in text:
        return "fulfillment_fee"
    if "shipping" in text:
        return "shipping"
    if "referral" in text:
        return "referral_fee"
    if "affiliate" in text:
        return "affiliate_fee"
    if "ads" in text or "campaign" in text or "promotion" in text:
        return "marketing_fee"
    if "service fee" in text or "administration fee" in text:
        return "service_fee"
    if "refund" in text or "chargeback" in text:
        return "refund"
    if "sale" in text or "product" in text or "order" in text:
        return "sale"
    return "other"


def load_statement_rows_uncached(files: list[Path]) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for path in files:
        frame = read_table_file(path)
        if frame is not None and not frame.empty:
            frame["statement_source_file"] = path.name
            frame["statement_source_mtime"] = path.stat().st_mtime
            frames.append(frame)
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df.columns = [str(column).strip() for column in df.columns]
    available = list(df.columns)

    order_id_col = pick_column(
        available,
        ["Order ID", "order_id", "Order Number", "Reference ID", "Order/adjustment ID"],
        [("order", "id"), ("order", "number"), ("reference", "id"), ("order", "adjustment", "id")],
    )
    related_order_id_col = pick_column(
        available,
        ["Related order ID", "Related order ID  "],
        [("related", "order", "id")],
    )
    statement_date_col = pick_column(
        available,
        ["Statement Date", "Date", "Transaction Date", "Settlement Date", "Book Date"],
        [("statement", "date"), ("transaction", "date"), ("settlement", "date"), ("book", "date")],
    )
    amount_col = pick_column(
        available,
        ["Amount", "Statement Amount", "Net Amount", "Posting Amount"],
        [("amount",), ("net", "amount"), ("posting", "amount")],
    )
    type_col = pick_column(
        available,
        ["Type", "Transaction Type", "Statement Type", "Fee Type"],
        [("type",), ("transaction", "type"), ("statement", "type"), ("fee", "type")],
    )
    adjustment_reason_col = pick_column(
        available,
        ["Adjustment reason", "Reason", "Description"],
        [("adjustment", "reason"), ("description",), ("reason",)],
    )

    def series_for(column: str | None) -> pd.Series:
        if not column or column not in df.columns:
            return pd.Series("", index=df.index, dtype="object")
        return df[column].astype(str).str.replace("\t", "", regex=False).str.strip()

    normalized = pd.DataFrame(
        {
            "order_id": series_for(order_id_col),
            "related_order_id": series_for(related_order_id_col),
            "statement_date": pd.to_datetime(series_for(statement_date_col), errors="coerce"),
            "statement_type": series_for(type_col),
            "adjustment_reason": series_for(adjustment_reason_col),
            "amount": pd.to_numeric(series_for(amount_col).replace({"": None}), errors="coerce").fillna(0.0),
            "statement_source_file": df["statement_source_file"].astype(str),
            "statement_source_mtime": pd.to_numeric(df["statement_source_mtime"], errors="coerce"),
        }
    )
    normalized["order_id"] = normalized["order_id"].where(normalized["order_id"].ne(""), normalized["related_order_id"])
    normalized["category"] = normalized.apply(
        lambda row: classify_statement_category(row["statement_type"], row["adjustment_reason"]),
        axis=1,
    )

    mapping = {
        "Gross sales": "statement_gross_sales",
        "Gross sales refund": "statement_gross_sales_refund",
        "Seller discount": "statement_seller_discount",
        "Seller discount refund": "statement_seller_discount_refund",
        "Net sales": "statement_net_sales",
        "Shipping": "statement_shipping",
        "TikTok Shop shipping fee": "statement_tiktok_shipping_fee",
        "Fulfilled by TikTok Shop shipping fee": "statement_fbt_shipping_fee",
        "Customer-paid shipping fee": "statement_customer_paid_shipping_fee",
        "Customer-paid shipping fee refund": "statement_customer_paid_shipping_fee_refund",
        "TikTok Shop shipping incentive": "statement_tiktok_shipping_incentive",
        "TikTok Shop shipping incentive refund": "statement_tiktok_shipping_incentive_refund",
        "Shipping fee subsidy": "statement_shipping_fee_subsidy",
        "Return shipping fee": "statement_return_shipping_fee",
        "FBT fulfillment fee": "statement_fbt_fulfillment_fee",
        "Customer shipping fee offset": "statement_customer_shipping_fee_offset",
        "Shipping fee discount": "statement_shipping_fee_discount",
        "Return shipping label fee": "statement_return_shipping_label_fee",
        "FBT fulfillment fee reimbursement": "statement_fbt_fulfillment_fee_reimbursement",
        "Fees": "statement_fees",
        "Transaction fee": "statement_transaction_fee",
        "Referral fee": "statement_referral_fee",
        "Refund administration fee": "statement_refund_admin_fee",
        "Affiliate Commission": "statement_affiliate_commission",
        "Affiliate partner commission": "statement_affiliate_partner_commission",
        "Affiliate Shop Ads commission": "statement_affiliate_shop_ads_commission",
        "Affiliate Partner shop ads commission": "statement_affiliate_partner_shop_ads_commission",
        "TikTok Shop Partner commission": "statement_tiktok_partner_commission",
        "Co-funded promotion (seller-funded)": "statement_cofunded_promotion",
        "Campaign service fee": "statement_campaign_service_fee",
        "Smart Promotion fee": "statement_smart_promotion_fee",
        "Marketing benefits package fee": "statement_marketing_benefits_package_fee",
        "Adjustment amount": "statement_adjustment_amount",
        "Logistics reimbursement": "statement_logistics_reimbursement",
        "GMV deduction for FBT warehouse service fee": "statement_gmv_deduction_fbt_warehouse_fee",
        "TikTok Shop reimbursement": "statement_tiktok_shop_reimbursement",
        "Total settlement amount": "total_settlement_amount",
        "Customer payment": "statement_customer_payment",
        "Customer refund": "statement_customer_refund",
        "Seller co-funded voucher discount": "statement_seller_voucher_discount",
        "Seller co-funded voucher discount refund": "statement_seller_voucher_discount_refund",
        "Platform discounts": "statement_platform_discounts",
        "Platform discounts refund": "statement_platform_discounts_refund",
        "Sales tax payment": "statement_sales_tax_payment",
        "Sales tax refund": "statement_sales_tax_refund",
    }
    normalized_lookup = {normalized_column_text(column): column for column in df.columns}
    for source_label, target_column in mapping.items():
        source_column = normalized_lookup.get(normalized_column_text(source_label))
        if source_column:
            normalized[target_column] = pd.to_numeric(series_for(source_column).replace({"": None}), errors="coerce").fillna(0.0)
        else:
            normalized[target_column] = 0.0

    typed_amount = normalized["amount"].fillna(0.0)
    typed_category = normalized["category"].astype(str)
    detail_fee_presence = (
        normalized[
            [
                "statement_shipping",
                "statement_tiktok_shipping_fee",
                "statement_fbt_shipping_fee",
                "statement_fbt_fulfillment_fee",
                "statement_transaction_fee",
                "statement_referral_fee",
                "statement_refund_admin_fee",
                "statement_affiliate_commission",
                "statement_affiliate_partner_commission",
                "statement_affiliate_shop_ads_commission",
                "statement_campaign_service_fee",
            ]
        ]
        .abs()
        .sum(axis=1)
        .eq(0)
    )
    normalized["typed_shipping_fee"] = typed_amount.where(typed_category.eq("shipping") & detail_fee_presence, 0.0)
    normalized["typed_fulfillment_fee"] = typed_amount.where(typed_category.eq("fulfillment_fee") & detail_fee_presence, 0.0)
    normalized["typed_referral_fee"] = typed_amount.where(typed_category.eq("referral_fee") & detail_fee_presence, 0.0)
    normalized["typed_affiliate_fee"] = typed_amount.where(typed_category.eq("affiliate_fee") & detail_fee_presence, 0.0)
    normalized["typed_marketing_fee"] = typed_amount.where(typed_category.eq("marketing_fee") & detail_fee_presence, 0.0)
    normalized["typed_service_fee"] = typed_amount.where(typed_category.eq("service_fee") & detail_fee_presence, 0.0)
    normalized["typed_other_fee"] = typed_amount.where(typed_category.eq("other") & detail_fee_presence, 0.0)
    normalized = normalized.dropna(subset=["statement_date"]).copy()
    normalized = normalized.loc[normalized["order_id"].ne("") | normalized["amount"].ne(0)].copy()
    normalized = keep_latest_file_rows_by_date(
        normalized,
        date_column="statement_date",
        file_column="statement_source_file",
        mtime_column="statement_source_mtime",
    )
    return normalized.reset_index(drop=True)


def load_statement_rows(start_date: pd.Timestamp | None = None, end_date: pd.Timestamp | None = None) -> pd.DataFrame:
    files = candidate_statement_files(start_date, end_date)
    if not files:
        return pd.DataFrame()

    cached: dict[str, Any] = {"cache_schema_version": STATEMENT_CACHE_SCHEMA_VERSION, "files": {}}
    if STATEMENT_CACHE_FILE.exists():
        existing = pd.read_pickle(STATEMENT_CACHE_FILE)
        if isinstance(existing, dict) and existing.get("cache_schema_version") == STATEMENT_CACHE_SCHEMA_VERSION:
            cached = existing

    file_cache = cached.setdefault("files", {})
    frames: list[pd.DataFrame] = []
    dirty = False
    active_keys = {str(path.resolve()) for path in files}

    for path in files:
        key = str(path.resolve())
        mtime = path.stat().st_mtime
        entry = file_cache.get(key)
        if isinstance(entry, dict) and entry.get("mtime") == mtime and isinstance(entry.get("rows"), pd.DataFrame):
            frame = entry["rows"].copy()
        else:
            frame = load_statement_rows_uncached([path])
            file_cache[key] = {"mtime": mtime, "rows": frame}
            dirty = True
        if frame is not None and not frame.empty:
            frame["statement_date"] = pd.to_datetime(frame["statement_date"], errors="coerce")
            frames.append(frame)

    stale_keys = [key for key in list(file_cache.keys()) if key not in active_keys and not Path(key).exists()]
    for key in stale_keys:
        file_cache.pop(key, None)
        dirty = True

    if dirty:
        pd.to_pickle(cached, STATEMENT_CACHE_FILE)

    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if start_date is not None and end_date is not None:
        df = df.loc[df["statement_date"].between(start_date, end_date)].copy()
    return df.reset_index(drop=True)


def load_statement_rows_legacy(start_date: pd.Timestamp | None = None, end_date: pd.Timestamp | None = None) -> pd.DataFrame:
    files = candidate_statement_files(start_date, end_date)
    if not files:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for path in files:
        frame = read_table_file(path)
        if frame is not None and not frame.empty:
            frame["statement_source_file"] = path.name
            frames.append(frame)
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df.columns = [str(column).strip() for column in df.columns]
    available = list(df.columns)

    order_id_col = pick_column(
        available,
        ["Order ID", "order_id", "Order Number", "Reference ID", "Order/adjustment ID"],
        [("order", "id"), ("order", "number"), ("reference", "id"), ("order", "adjustment", "id")],
    )
    related_order_id_col = pick_column(
        available,
        ["Related order ID", "Related order ID  "],
        [("related", "order", "id")],
    )
    statement_date_col = pick_column(
        available,
        ["Statement Date", "Date", "Transaction Date", "Settlement Date", "Book Date"],
        [("statement", "date"), ("transaction", "date"), ("settlement", "date"), ("book", "date")],
    )
    amount_col = pick_column(
        available,
        ["Amount", "Statement Amount", "Net Amount", "Posting Amount"],
        [("amount",), ("net", "amount"), ("posting", "amount")],
    )
    type_col = pick_column(
        available,
        ["Type", "Transaction Type", "Statement Type", "Fee Type"],
        [("transaction", "type"), ("statement", "type"), ("fee", "type"), ("type",)],
    )
    description_col = pick_column(
        available,
        ["Description", "Details", "Fee Description", "Remark", "Notes", "Adjustment reason"],
        [("description",), ("detail",), ("remark",), ("note",), ("adjustment", "reason")],
    )

    working = pd.DataFrame(index=df.index)
    primary_order_id = df[order_id_col].astype(str).str.replace("\t", "", regex=False).str.strip() if order_id_col else ""
    related_order_id = df[related_order_id_col].astype(str).str.replace("\t", "", regex=False).str.strip() if related_order_id_col else ""
    if related_order_id_col:
        related_valid = related_order_id.str.fullmatch(r"\d{8,}")
        working["order_id"] = related_order_id.where(related_valid, primary_order_id)
    else:
        working["order_id"] = primary_order_id
    working["statement_date"] = pd.to_datetime(df[statement_date_col], errors="coerce") if statement_date_col else pd.NaT
    working["statement_type"] = df[type_col].astype(str).str.strip() if type_col else ""
    working["description"] = df[description_col].astype(str).str.strip() if description_col else ""
    working["statement_source_file"] = df["statement_source_file"].astype(str)
    working["category"] = working.apply(lambda row: classify_statement_category(row["statement_type"], row["description"]), axis=1)

    numeric_field_map = {
        "amount": ["Amount", "Statement Amount", "Net Amount", "Posting Amount", "Total settlement amount"],
        "total_settlement_amount": ["Total settlement amount"],
        "statement_net_sales": ["Net sales"],
        "statement_gross_sales": ["Gross sales"],
        "statement_gross_sales_refund": ["Gross sales refund"],
        "statement_seller_discount": ["Seller discount"],
        "statement_seller_discount_refund": ["Seller discount refund"],
        "statement_shipping": ["Shipping"],
        "statement_tiktok_shipping_fee": ["TikTok Shop shipping fee"],
        "statement_fbt_shipping_fee": ["Fulfilled by TikTok Shop shipping fee"],
        "statement_customer_paid_shipping_fee": ["Customer-paid shipping fee"],
        "statement_customer_paid_shipping_fee_refund": ["Customer-paid shipping fee refund"],
        "statement_tiktok_shipping_incentive": ["TikTok Shop shipping incentive"],
        "statement_tiktok_shipping_incentive_refund": ["TikTok Shop shipping incentive refund"],
        "statement_shipping_fee_subsidy": ["Shipping fee subsidy"],
        "statement_return_shipping_fee": ["Return shipping fee"],
        "statement_fbt_fulfillment_fee": ["FBT fulfillment fee"],
        "statement_customer_shipping_fee_offset": ["Customer shipping fee offset"],
        "statement_shipping_fee_discount": ["Shipping fee discount"],
        "statement_return_shipping_label_fee": ["Return shipping label fee"],
        "statement_fbt_fulfillment_fee_reimbursement": ["FBT fulfillment fee reimbursement"],
        "statement_fees": ["Fees"],
        "statement_transaction_fee": ["Transaction fee"],
        "statement_referral_fee": ["Referral fee"],
        "statement_refund_admin_fee": ["Refund administration fee"],
        "statement_affiliate_commission": ["Affiliate Commission"],
        "statement_affiliate_partner_commission": ["Affiliate partner commission"],
        "statement_affiliate_shop_ads_commission": ["Affiliate Shop Ads commission"],
        "statement_affiliate_partner_shop_ads_commission": ["Affiliate Partner shop ads commission"],
        "statement_tiktok_partner_commission": ["TikTok Shop Partner commission"],
        "statement_cofunded_promotion": ["Co-funded promotion (seller-funded)"],
        "statement_campaign_service_fee": ["Campaign service fee"],
        "statement_smart_promotion_fee": ["Smart Promotion fee"],
        "statement_marketing_benefits_package_fee": ["Marketing benefits package fee"],
        "statement_adjustment_amount": ["Adjustment amount"],
        "statement_logistics_reimbursement": ["Logistics reimbursement"],
        "statement_gmv_deduction_fbt_warehouse_fee": ["GMV deduction for FBT warehouse service fee"],
        "statement_tiktok_shop_reimbursement": ["TikTok Shop reimbursement"],
        "statement_customer_payment": ["Customer payment"],
        "statement_customer_refund": ["Customer refund"],
        "statement_seller_voucher_discount": ["Seller co-funded voucher discount"],
        "statement_seller_voucher_discount_refund": ["Seller co-funded voucher discount refund"],
        "statement_platform_discounts": ["Platform discounts"],
        "statement_platform_discounts_refund": ["Platform discounts refund"],
        "statement_sales_tax_payment": ["Sales tax payment"],
        "statement_sales_tax_refund": ["Sales tax refund"],
    }
    for internal_name, candidates in numeric_field_map.items():
        column = pick_column(available, candidates, [])
        if column:
            series = (
                df[column]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.strip()
                .replace({"": None, "--": None, "/": None})
            )
            working[internal_name] = pd.to_numeric(series, errors="coerce").fillna(0.0)
        else:
            working[internal_name] = 0.0

    typed_amount = working["total_settlement_amount"].fillna(0.0)
    typed_category = working["category"].astype(str)
    detail_fee_presence = (
        working[
            [
                "statement_shipping",
                "statement_tiktok_shipping_fee",
                "statement_fbt_shipping_fee",
                "statement_fbt_fulfillment_fee",
                "statement_transaction_fee",
                "statement_referral_fee",
                "statement_refund_admin_fee",
                "statement_affiliate_commission",
                "statement_affiliate_partner_commission",
                "statement_affiliate_shop_ads_commission",
                "statement_campaign_service_fee",
            ]
        ]
        .abs()
        .sum(axis=1)
        .eq(0)
    )
    working["typed_shipping_fee"] = typed_amount.where(typed_category.eq("shipping") & detail_fee_presence, 0.0)
    working["typed_fulfillment_fee"] = typed_amount.where(typed_category.eq("fulfillment_fee") & detail_fee_presence, 0.0)
    working["typed_referral_fee"] = typed_amount.where(typed_category.eq("referral_fee") & detail_fee_presence, 0.0)
    working["typed_affiliate_fee"] = typed_amount.where(typed_category.eq("affiliate_fee") & detail_fee_presence, 0.0)
    working["typed_marketing_fee"] = typed_amount.where(typed_category.eq("marketing_fee") & detail_fee_presence, 0.0)
    working["typed_service_fee"] = typed_amount.where(typed_category.eq("service_fee") & detail_fee_presence, 0.0)
    working["typed_other_fee"] = typed_amount.where(typed_category.eq("other") & detail_fee_presence, 0.0)
    working = working.dropna(subset=["statement_date"]).copy()
    working = working.loc[working["order_id"].ne("") | working["amount"].ne(0)].copy()
    return working.reset_index(drop=True)


def load_zip_reference() -> pd.DataFrame:
    if not ZIP_REFERENCE_FILE.exists():
        return pd.DataFrame(columns=["zip", "city", "state", "latitude", "longitude"])
    zip_df = pd.read_csv(ZIP_REFERENCE_FILE, dtype={"zip": str})
    zip_df["zip"] = zip_df["zip"].map(normalize_zip)
    zip_df["latitude"] = pd.to_numeric(zip_df["latitude"], errors="coerce")
    zip_df["longitude"] = pd.to_numeric(zip_df["longitude"], errors="coerce")
    return zip_df.dropna(subset=["zip", "latitude", "longitude"]).drop_duplicates(subset=["zip"]).reset_index(drop=True)


def build_order_level_view(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    working = raw_df.copy()
    working["zipcode_5"] = working.get("Zipcode", pd.Series("", index=working.index)).map(normalize_zip)
    working["city_clean"] = working.get("City", pd.Series("", index=working.index)).astype("string").fillna("").str.strip()
    working["state_clean"] = working.get("State", pd.Series("", index=working.index)).astype("string").fillna("").str.strip()
    working["state_normalized"] = working["state_clean"].map(normalize_state)
    return (
        working.groupby("Order ID", as_index=False)
        .agg(
            reporting_date=("reporting_date", "min"),
            customer_id=("customer_id", "first"),
            source_type=("source_type", "first"),
            city=("city_clean", "first"),
            state=("state_clean", "first"),
            state_code=("state_normalized", "first"),
            zipcode=("zipcode_5", "first"),
            units_sold=("Quantity", "sum"),
            order_sales=("SKU Subtotal After Discount", "sum"),
            gross_sales=("SKU Subtotal Before Discount", "sum"),
            refund_amount=("Order Refund Amount", "max"),
            is_canceled=("is_canceled", "max"),
            is_refunded=("is_refunded", "max"),
            is_returned=("is_returned", "max"),
        )
        .rename(columns={"Order ID": "order_id"})
    )


STATEMENT_ROLLUP_NUMERIC_COLUMNS = [
    "amount",
    "total_settlement_amount",
    "statement_net_sales",
    "statement_gross_sales",
    "statement_gross_sales_refund",
    "statement_seller_discount",
    "statement_seller_discount_refund",
    "statement_shipping",
    "statement_tiktok_shipping_fee",
    "statement_fbt_shipping_fee",
    "statement_customer_paid_shipping_fee",
    "statement_customer_paid_shipping_fee_refund",
    "statement_tiktok_shipping_incentive",
    "statement_tiktok_shipping_incentive_refund",
    "statement_shipping_fee_subsidy",
    "statement_return_shipping_fee",
    "statement_fbt_fulfillment_fee",
    "statement_customer_shipping_fee_offset",
    "statement_shipping_fee_discount",
    "statement_return_shipping_label_fee",
    "statement_fbt_fulfillment_fee_reimbursement",
    "statement_fees",
    "statement_transaction_fee",
    "statement_referral_fee",
    "statement_refund_admin_fee",
    "statement_affiliate_commission",
    "statement_affiliate_partner_commission",
    "statement_affiliate_shop_ads_commission",
    "statement_affiliate_partner_shop_ads_commission",
    "statement_tiktok_partner_commission",
    "statement_cofunded_promotion",
    "statement_campaign_service_fee",
    "statement_smart_promotion_fee",
    "statement_marketing_benefits_package_fee",
    "statement_adjustment_amount",
    "statement_logistics_reimbursement",
    "statement_gmv_deduction_fbt_warehouse_fee",
    "statement_tiktok_shop_reimbursement",
    "statement_customer_payment",
    "statement_customer_refund",
    "statement_seller_voucher_discount",
    "statement_seller_voucher_discount_refund",
    "statement_platform_discounts",
    "statement_platform_discounts_refund",
    "statement_sales_tax_payment",
    "statement_sales_tax_refund",
    "typed_shipping_fee",
    "typed_fulfillment_fee",
    "typed_referral_fee",
    "typed_affiliate_fee",
    "typed_marketing_fee",
    "typed_service_fee",
    "typed_other_fee",
]

STATEMENT_ROLLUP_COLUMNS = [
    "order_id",
    "first_statement_date",
    "last_statement_date",
    "statement_row_count",
    "statement_amount_total",
    *[column for column in STATEMENT_ROLLUP_NUMERIC_COLUMNS if column != "amount"],
]


def empty_statement_rollup() -> pd.DataFrame:
    return pd.DataFrame(columns=STATEMENT_ROLLUP_COLUMNS)


def build_statement_rollup(statement_rows: pd.DataFrame) -> pd.DataFrame:
    if statement_rows is None or statement_rows.empty:
        return empty_statement_rollup()
    matched = statement_rows.loc[statement_rows["order_id"].astype(str).str.strip().ne("")].copy()
    if matched.empty:
        return empty_statement_rollup()
    base = (
        matched.groupby("order_id", as_index=False)
        .agg(
            first_statement_date=("statement_date", "min"),
            last_statement_date=("statement_date", "max"),
            statement_row_count=("order_id", "count"),
            **{column: (column, "sum") for column in STATEMENT_ROLLUP_NUMERIC_COLUMNS},
        )
    )
    base = base.rename(columns={"amount": "statement_amount_total"})
    return base.reindex(columns=STATEMENT_ROLLUP_COLUMNS).fillna(0)


def build_statement_unmatched(statement_rows: pd.DataFrame, order_level_df: pd.DataFrame) -> pd.DataFrame:
    if statement_rows is None or statement_rows.empty:
        return pd.DataFrame()
    order_ids = set(order_level_df["order_id"].astype(str)) if order_level_df is not None and not order_level_df.empty else set()
    return statement_rows.loc[~statement_rows["order_id"].astype(str).isin(order_ids)].copy()


def build_reconciliation_view(order_level_df: pd.DataFrame, statement_rows: pd.DataFrame, date_basis: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    order_level_df = order_level_df.copy() if order_level_df is not None else pd.DataFrame()
    statement_rows = statement_rows.copy() if statement_rows is not None else pd.DataFrame()
    statement_rollup = build_statement_rollup(statement_rows)

    if date_basis == "statement":
        filtered_statement_rows = statement_rows.loc[statement_rows["statement_date"].between(start_date, end_date)].copy() if not statement_rows.empty else pd.DataFrame()
        filtered_rollup = build_statement_rollup(filtered_statement_rows)
        matched = filtered_rollup.merge(order_level_df, on="order_id", how="inner")
        unmatched_statement = build_statement_unmatched(filtered_statement_rows, order_level_df)
        unmatched_orders = pd.DataFrame()
    else:
        filtered_orders = order_level_df.loc[order_level_df["reporting_date"].between(start_date, end_date)].copy() if not order_level_df.empty else pd.DataFrame()
        matched = filtered_orders.merge(statement_rollup, on="order_id", how="left")
        unmatched_orders = matched.loc[matched["statement_amount_total"].isna()].copy() if not matched.empty else pd.DataFrame()
        unmatched_statement = build_statement_unmatched(statement_rows, filtered_orders)

    if not matched.empty:
        numeric_columns = [
            "statement_amount_total",
            "total_settlement_amount",
            "statement_net_sales",
            "statement_gross_sales",
            "statement_gross_sales_refund",
            "statement_seller_discount",
            "statement_seller_discount_refund",
            "statement_shipping",
            "statement_tiktok_shipping_fee",
            "statement_fbt_shipping_fee",
            "statement_customer_paid_shipping_fee",
            "statement_customer_paid_shipping_fee_refund",
            "statement_tiktok_shipping_incentive",
            "statement_tiktok_shipping_incentive_refund",
            "statement_fbt_fulfillment_fee",
            "statement_customer_shipping_fee_offset",
            "statement_transaction_fee",
            "statement_referral_fee",
            "statement_refund_admin_fee",
            "statement_affiliate_commission",
            "statement_affiliate_partner_commission",
            "statement_affiliate_shop_ads_commission",
            "statement_affiliate_partner_shop_ads_commission",
            "statement_tiktok_partner_commission",
            "statement_cofunded_promotion",
            "statement_campaign_service_fee",
            "statement_smart_promotion_fee",
            "statement_marketing_benefits_package_fee",
            "statement_adjustment_amount",
            "statement_logistics_reimbursement",
            "statement_gmv_deduction_fbt_warehouse_fee",
            "statement_tiktok_shop_reimbursement",
            "statement_platform_discounts",
            "statement_platform_discounts_refund",
            "typed_shipping_fee",
            "typed_fulfillment_fee",
            "typed_referral_fee",
            "typed_affiliate_fee",
            "typed_marketing_fee",
            "typed_service_fee",
            "typed_other_fee",
        ]
        for column in numeric_columns:
            if column not in matched.columns:
                matched[column] = 0.0
            matched[column] = pd.to_numeric(matched[column], errors="coerce").fillna(0)
        matched["shipping_fee_total"] = matched[
            [
                "statement_shipping",
                "statement_tiktok_shipping_fee",
                "statement_fbt_shipping_fee",
                "statement_customer_paid_shipping_fee",
                "statement_customer_paid_shipping_fee_refund",
                "statement_tiktok_shipping_incentive",
                "statement_tiktok_shipping_incentive_refund",
                "statement_customer_shipping_fee_offset",
                "typed_shipping_fee",
            ]
        ].sum(axis=1)
        matched["fulfillment_fee_total"] = matched[
            ["statement_fbt_fulfillment_fee", "typed_fulfillment_fee"]
        ].sum(axis=1)
        matched["affiliate_fee_total"] = matched[
            [
                "statement_affiliate_commission",
                "statement_affiliate_partner_commission",
                "statement_affiliate_shop_ads_commission",
                "statement_affiliate_partner_shop_ads_commission",
                "statement_tiktok_partner_commission",
                "typed_affiliate_fee",
            ]
        ].sum(axis=1)
        matched["marketing_fee_total"] = matched[
            [
                "statement_cofunded_promotion",
                "statement_campaign_service_fee",
                "statement_smart_promotion_fee",
                "statement_marketing_benefits_package_fee",
                "typed_marketing_fee",
            ]
        ].sum(axis=1)
        matched["service_fee_total"] = matched[
            ["statement_transaction_fee", "statement_referral_fee", "statement_refund_admin_fee", "typed_referral_fee", "typed_service_fee", "typed_other_fee"]
        ].sum(axis=1)
        matched["actual_fee_total"] = matched[
            ["shipping_fee_total", "fulfillment_fee_total", "affiliate_fee_total", "marketing_fee_total", "service_fee_total"]
        ].sum(axis=1)
        matched["statement_net_after_fees"] = matched["statement_net_sales"] + matched["actual_fee_total"] + matched["statement_adjustment_amount"]

    summary = {
        "date_basis": date_basis,
        "matched_orders": int(matched["order_id"].nunique()) if not matched.empty else 0,
        "unmatched_statement_rows": int(len(unmatched_statement)),
        "unmatched_orders": int(unmatched_orders["order_id"].nunique()) if not unmatched_orders.empty else 0,
        "statement_amount_total": float(matched["statement_amount_total"].sum()) if not matched.empty else 0.0,
        "actual_fee_total": float(matched["actual_fee_total"].sum()) if not matched.empty else 0.0,
        "fulfillment_fee_total": float(matched["fulfillment_fee_total"].sum()) if not matched.empty else 0.0,
    }
    return matched, unmatched_statement, unmatched_orders, summary


def build_location_views(
    order_level_df: pd.DataFrame,
    zip_reference: pd.DataFrame,
    target_zip: str,
    radius_miles: int,
    target_city: str,
    target_state: str,
    city_radius_miles: int | None = None,
) -> dict[str, pd.DataFrame | dict[str, Any]]:
    if order_level_df is None or order_level_df.empty:
        empty = pd.DataFrame()
        return {
            "cities": empty,
            "zips": empty,
            "radius": empty,
            "target_city_rows": empty,
            "radius_summary": {"target_zip": target_zip, "radius_miles": radius_miles, "customers_within_radius": 0, "orders_within_radius": 0, "matched_zip_rows": 0},
            "target_city_summary": {"target_city": target_city, "target_state": target_state, "city_radius_miles": city_radius_miles or radius_miles, "customers_in_city": 0, "orders_in_city": 0, "zip_rows_in_city": 0},
        }

    valid_customers = order_level_df.loc[order_level_df["customer_id"].astype("string").str.strip().ne("")].copy()
    cities = (
        valid_customers.groupby(["city", "state"], dropna=False, as_index=False)
        .agg(unique_customers=("customer_id", "nunique"), orders=("order_id", "nunique"))
        .sort_values(["unique_customers", "orders"], ascending=False)
    )
    zips = (
        valid_customers.loc[valid_customers["zipcode"].astype(str).str.strip().ne("")]
        .groupby("zipcode", as_index=False)
        .agg(unique_customers=("customer_id", "nunique"), orders=("order_id", "nunique"))
        .sort_values(["unique_customers", "orders"], ascending=False)
    )

    target_city_clean = str(target_city or "").strip().lower()
    target_state_clean = normalize_state(target_state)
    city_target_rows = pd.DataFrame()
    city_target_summary = {
        "target_city": target_city.strip() if isinstance(target_city, str) else "",
        "target_state": target_state.strip().upper() if isinstance(target_state, str) else "",
        "city_radius_miles": city_radius_miles or radius_miles,
        "customers_in_city": 0,
        "orders_in_city": 0,
        "zip_rows_in_city": 0,
    }
    if target_city_clean:
        city_filtered = valid_customers.loc[
            valid_customers["city"].astype(str).str.strip().str.lower().eq(target_city_clean)
        ].copy()
        if target_state_clean:
            city_filtered = city_filtered.loc[
                city_filtered["state_code"].astype(str).str.strip().str.upper().eq(target_state_clean)
            ].copy()
        if not city_filtered.empty:
            city_zip_rows = (
                city_filtered.loc[city_filtered["zipcode"].astype(str).str.strip().ne("")]
                .groupby(["zipcode", "city", "state", "state_code"], dropna=False, as_index=False)
                .agg(unique_customers=("customer_id", "nunique"), orders=("order_id", "nunique"))
            )
            city_target_rows = city_zip_rows.copy()
            city_radius = int(city_radius_miles or radius_miles)
            if not city_zip_rows.empty and not zip_reference.empty:
                city_zip_centroids = city_zip_rows.merge(zip_reference, left_on="zipcode", right_on="zip", how="left").drop(columns=["zip"], errors="ignore")
                city_zip_centroids = city_zip_centroids.dropna(subset=["latitude", "longitude"]).copy()
                if not city_zip_centroids.empty:
                    city_lat = float(city_zip_centroids["latitude"].mean())
                    city_lon = float(city_zip_centroids["longitude"].mean())
                    customer_zips = zips.merge(zip_reference, left_on="zipcode", right_on="zip", how="left").drop(columns=["zip"], errors="ignore")
                    customer_zips = customer_zips.dropna(subset=["latitude", "longitude"]).copy()
                    customer_zips["distance_miles"] = customer_zips.apply(
                        lambda row: haversine_miles(city_lat, city_lon, float(row["latitude"]), float(row["longitude"])),
                        axis=1,
                    )
                    city_target_rows = customer_zips.loc[customer_zips["distance_miles"].le(city_radius)].sort_values("distance_miles")
                    city_target_rows["city"] = city_target_rows["zipcode"].map(
                        city_filtered.groupby("zipcode")["city"].first().to_dict()
                    ).fillna(city_filtered["city"].iloc[0])
                    city_target_rows["state"] = city_target_rows["zipcode"].map(
                        city_filtered.groupby("zipcode")["state"].first().to_dict()
                    ).fillna(city_filtered["state"].iloc[0] if "state" in city_filtered.columns else "")
                    city_target_rows["state_code"] = city_target_rows["zipcode"].map(
                        city_filtered.groupby("zipcode")["state_code"].first().to_dict()
                    ).fillna(city_filtered["state_code"].iloc[0] if "state_code" in city_filtered.columns else "")
            city_target_summary = {
                "target_city": city_filtered["city"].iloc[0],
                "target_state": city_filtered["state_code"].iloc[0] if city_filtered["state_code"].astype(str).str.strip().ne("").any() else normalize_state(target_state),
                "city_radius_miles": city_radius,
                "customers_in_city": int(city_target_rows["unique_customers"].sum()) if not city_target_rows.empty else 0,
                "orders_in_city": int(city_target_rows["orders"].sum()) if not city_target_rows.empty else 0,
                "zip_rows_in_city": int(len(city_target_rows)),
            }

    radius_rows = pd.DataFrame()
    radius_summary = {"target_zip": target_zip, "radius_miles": radius_miles, "customers_within_radius": 0, "orders_within_radius": 0, "matched_zip_rows": 0}
    target_zip = normalize_zip(target_zip)
    if target_zip and not zip_reference.empty:
        target = zip_reference.loc[zip_reference["zip"].eq(target_zip)]
        customer_zips = zips.merge(zip_reference, left_on="zipcode", right_on="zip", how="left")
        if not target.empty:
            target_lat = float(target.iloc[0]["latitude"])
            target_lon = float(target.iloc[0]["longitude"])
            customer_zips = customer_zips.dropna(subset=["latitude", "longitude"]).copy()
            customer_zips["distance_miles"] = customer_zips.apply(
                lambda row: haversine_miles(target_lat, target_lon, float(row["latitude"]), float(row["longitude"])),
                axis=1,
            )
            radius_rows = customer_zips.loc[customer_zips["distance_miles"].le(radius_miles)].sort_values("distance_miles")
            radius_summary = {
                "target_zip": target_zip,
                "radius_miles": radius_miles,
                "customers_within_radius": int(radius_rows["unique_customers"].sum()) if not radius_rows.empty else 0,
                "orders_within_radius": int(radius_rows["orders"].sum()) if not radius_rows.empty else 0,
                "matched_zip_rows": int(len(radius_rows)),
            }

    return {
        "cities": cities.head(100),
        "zips": zips.head(100),
        "radius": radius_rows.head(100),
        "target_city_rows": city_target_rows.head(100),
        "radius_summary": radius_summary,
        "target_city_summary": city_target_summary,
    }


def build_filtered_status_view(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    order_level = (
        raw_df.groupby("Order ID", as_index=False)
        .agg(
            is_canceled=("is_canceled", "max"),
            is_refunded=("is_refunded", "max"),
            is_returned=("is_returned", "max"),
            is_delivered=("is_delivered", "max"),
            is_shipped=("is_shipped", "max"),
        )
    )
    order_level["status"] = "Placed"
    order_level.loc[order_level["is_shipped"], "status"] = "Shipped"
    order_level.loc[order_level["is_delivered"], "status"] = "Delivered"
    order_level.loc[order_level["is_returned"], "status"] = "Returned"
    order_level.loc[order_level["is_refunded"], "status"] = "Refunded"
    order_level.loc[order_level["is_canceled"], "status"] = "Canceled"
    return (
        order_level.groupby("status", as_index=False)
        .agg(order_count=("Order ID", "count"))
        .sort_values("order_count", ascending=False)
    )


def filter_daily(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, selected_sources: list[str] | None = None) -> pd.DataFrame:
    filtered = df.loc[df["reporting_date"].between(start, end)].copy()
    if selected_sources and "source_type" in filtered.columns:
        filtered = filtered.loc[filtered["source_type"].isin(selected_sources)].copy()
    return filtered


def summarize_order_period(filtered_finance: pd.DataFrame) -> dict[str, Any]:
    if filtered_finance.empty:
        return {
            "orders_gross_product_sales": None,
            "orders_net_product_sales": None,
            "product_sales_after_seller_discount": None,
            "product_sales_after_all_discounts": None,
            "collected_order_amount": None,
            "orders_seller_discount": None,
            "orders_platform_discount": None,
            "orders_refund_amount": None,
            "orders_paid_orders": None,
            "orders_aov": None,
        }
    values = filtered_finance[
        [
            "gross_product_sales",
            "product_sales_after_seller_discount",
            "product_sales_after_all_discounts",
            "collected_order_amount",
            "seller_discount",
            "platform_discount",
            "order_refund_amount",
            "paid_orders",
        ]
    ].sum()
    result = {
        "orders_gross_product_sales": json_safe(values.get("gross_product_sales")),
        "product_sales_after_seller_discount": json_safe(values.get("product_sales_after_seller_discount")),
        "product_sales_after_all_discounts": json_safe(values.get("product_sales_after_all_discounts")),
        "collected_order_amount": json_safe(values.get("collected_order_amount")),
        "orders_seller_discount": json_safe(values.get("seller_discount")),
        "orders_platform_discount": json_safe(values.get("platform_discount")),
        "orders_refund_amount": json_safe(values.get("order_refund_amount")),
        "orders_paid_orders": json_safe(values.get("paid_orders")),
    }
    if result["orders_gross_product_sales"] is not None and result["orders_seller_discount"] is not None and result["orders_refund_amount"] is not None:
        result["orders_net_product_sales"] = result["orders_gross_product_sales"] - result["orders_seller_discount"] - result["orders_refund_amount"]
    else:
        result["orders_net_product_sales"] = None
    if result["orders_gross_product_sales"] is not None and result["orders_paid_orders"] not in (None, 0):
        result["orders_aov"] = result["orders_gross_product_sales"] / result["orders_paid_orders"]
    else:
        result["orders_aov"] = None
    return result


def summarize_statement_period(statement_rows: pd.DataFrame) -> dict[str, Any]:
    if statement_rows is None or statement_rows.empty:
        return {
            "finance_gross_sales": None,
            "finance_gross_sales_refund": None,
            "finance_seller_discount": None,
            "finance_seller_discount_refund": None,
            "finance_net_sales": None,
            "finance_shipping_total": None,
            "finance_fees_total": None,
            "finance_adjustments_total": None,
            "finance_payout_amount": None,
        }
    values = statement_rows[
        [
            "statement_gross_sales",
            "statement_gross_sales_refund",
            "statement_seller_discount",
            "statement_seller_discount_refund",
            "statement_net_sales",
            "statement_shipping",
            "statement_fees",
            "statement_adjustment_amount",
            "total_settlement_amount",
        ]
    ].sum()
    return {
        "finance_gross_sales": json_safe(values.get("statement_gross_sales")),
        "finance_gross_sales_refund": json_safe(values.get("statement_gross_sales_refund")),
        "finance_seller_discount": json_safe(values.get("statement_seller_discount")),
        "finance_seller_discount_refund": json_safe(values.get("statement_seller_discount_refund")),
        "finance_net_sales": json_safe(values.get("statement_net_sales")),
        "finance_shipping_total": json_safe(values.get("statement_shipping")),
        "finance_fees_total": json_safe(values.get("statement_fees")),
        "finance_adjustments_total": json_safe(values.get("statement_adjustment_amount")),
        "finance_payout_amount": json_safe(values.get("total_settlement_amount")),
    }


def build_statement_fee_breakdown(statement_rows: pd.DataFrame) -> pd.DataFrame:
    if statement_rows is None or statement_rows.empty:
        return pd.DataFrame(columns=["fee_bucket", "category", "amount"])
    bucket_defs = [
        ("Transaction fee", "Operations", "statement_transaction_fee"),
        ("Referral fee", "Operations", "statement_referral_fee"),
        ("Refund administration fee", "Operations", "statement_refund_admin_fee"),
        ("Affiliate commission", "Affiliate", "statement_affiliate_commission"),
        ("Affiliate partner commission", "Affiliate", "statement_affiliate_partner_commission"),
        ("Affiliate Shop Ads commission", "Affiliate", "statement_affiliate_shop_ads_commission"),
        ("Affiliate Partner shop ads commission", "Affiliate", "statement_affiliate_partner_shop_ads_commission"),
        ("TikTok Shop Partner commission", "Affiliate", "statement_tiktok_partner_commission"),
        ("Campaign service fee", "Marketing", "statement_campaign_service_fee"),
        ("Smart Promotion fee", "Marketing", "statement_smart_promotion_fee"),
        ("Marketing benefits package fee", "Marketing", "statement_marketing_benefits_package_fee"),
        ("Other fee (typed fallback)", "Other", "typed_other_fee"),
        ("Service fee (typed fallback)", "Other", "typed_service_fee"),
        ("Referral fee (typed fallback)", "Other", "typed_referral_fee"),
    ]
    rows: list[dict[str, Any]] = []
    for label, category, column in bucket_defs:
        if column not in statement_rows.columns:
            continue
        amount = statement_rows[column].sum(min_count=1)
        if pd.isna(amount) or abs(float(amount)) < 0.005:
            continue
        rows.append({"fee_bucket": label, "category": category, "amount": float(amount)})
    if not rows:
        return pd.DataFrame(columns=["fee_bucket", "category", "amount"])
    breakdown = pd.DataFrame(rows)
    return breakdown.sort_values("amount", key=lambda series: series.abs(), ascending=False).reset_index(drop=True)


def summarize_source_units(raw_df: pd.DataFrame) -> dict[str, float]:
    if raw_df is None or raw_df.empty or "source_type" not in raw_df.columns:
        return {
            "operational_units": 0.0,
            "sales_units": 0.0,
            "sample_units": 0.0,
            "replacement_units": 0.0,
        }
    units_by_source = raw_df.groupby("source_type")["Quantity"].sum(min_count=1)
    return {
        "operational_units": float(units_by_source.sum()),
        "sales_units": float(units_by_source.get("Sales", 0.0) or 0.0),
        "sample_units": float(units_by_source.get("Samples", 0.0) or 0.0),
        "replacement_units": float(units_by_source.get("Replacements", 0.0) or 0.0),
    }


def build_order_health_metrics(order_level_df: pd.DataFrame) -> dict[str, Any]:
    if order_level_df is None or order_level_df.empty:
        return {
            "valid_orders": None,
            "canceled_orders": None,
            "refunded_orders": None,
            "returned_orders": None,
            "delivered_orders": None,
            "shipped_orders": None,
            "units_per_paid_order": None,
            "cancellation_rate": None,
            "refund_rate": None,
            "return_rate": None,
            "delivery_rate": None,
        }
    total_orders = int(order_level_df["order_id"].nunique())
    canceled_orders = int(order_level_df["is_canceled"].fillna(False).sum()) if "is_canceled" in order_level_df.columns else 0
    refunded_orders = int(order_level_df["is_refunded"].fillna(False).sum()) if "is_refunded" in order_level_df.columns else 0
    returned_orders = int(order_level_df["is_returned"].fillna(False).sum()) if "is_returned" in order_level_df.columns else 0
    delivered_orders = int(order_level_df["is_delivered"].fillna(False).sum()) if "is_delivered" in order_level_df.columns else 0
    shipped_orders = int(order_level_df["is_shipped"].fillna(False).sum()) if "is_shipped" in order_level_df.columns else 0
    valid_orders = total_orders - canceled_orders
    total_units = float(order_level_df["units_sold"].sum()) if "units_sold" in order_level_df.columns else 0.0
    return {
        "valid_orders": valid_orders,
        "canceled_orders": canceled_orders,
        "refunded_orders": refunded_orders,
        "returned_orders": returned_orders,
        "delivered_orders": delivered_orders,
        "shipped_orders": shipped_orders,
        "units_per_paid_order": (total_units / total_orders) if total_orders else None,
        "cancellation_rate": (canceled_orders / total_orders) if total_orders else None,
        "refund_rate": (refunded_orders / total_orders) if total_orders else None,
        "return_rate": (returned_orders / total_orders) if total_orders else None,
        "delivery_rate": (delivered_orders / total_orders) if total_orders else None,
    }


def valid_customer_orders(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty or "customer_id" not in raw_df.columns:
        return pd.DataFrame()
    working = raw_df.loc[~raw_df["is_canceled"].fillna(False)].copy()
    if "customer_id_source" not in working.columns:
        working["customer_id_source"] = ""
    working = working.loc[working["customer_id"].astype("string").str.strip().ne("")]
    if working.empty:
        return pd.DataFrame()
    order_level = (
        working.sort_values("reporting_date")
        .groupby("Order ID", as_index=False)
        .agg(
            customer_id=("customer_id", "first"),
            customer_id_source=("customer_id_source", "first"),
            reporting_date=("reporting_date", "min"),
            source_type=("source_type", "first"),
        )
    )
    order_level["order_month"] = order_level["reporting_date"].dt.to_period("M").dt.to_timestamp()
    return order_level


def customer_proxy_mix(order_level: pd.DataFrame) -> dict[str, Any]:
    if order_level.empty or "customer_id_source" not in order_level.columns:
        return {
            "customer_proxy_username_pct": None,
            "customer_proxy_nickname_pct": None,
            "customer_proxy_recipient_pct": None,
            "customer_proxy_username_count": None,
            "customer_proxy_nickname_count": None,
            "customer_proxy_recipient_count": None,
        }
    first_selected = (
        order_level.sort_values("reporting_date")
        .groupby("customer_id", as_index=False)
        .agg(customer_id_source=("customer_id_source", "first"))
    )
    total = int(first_selected.shape[0])
    if total == 0:
        return {
            "customer_proxy_username_pct": None,
            "customer_proxy_nickname_pct": None,
            "customer_proxy_recipient_pct": None,
            "customer_proxy_username_count": None,
            "customer_proxy_nickname_count": None,
            "customer_proxy_recipient_count": None,
        }
    source_counts = first_selected["customer_id_source"].value_counts()
    username_count = int(source_counts.get("Buyer Username", 0))
    nickname_count = int(source_counts.get("Buyer Nickname", 0))
    recipient_count = int(source_counts.get("Recipient", 0))
    return {
        "customer_proxy_username_pct": username_count / total,
        "customer_proxy_nickname_pct": nickname_count / total,
        "customer_proxy_recipient_pct": recipient_count / total,
        "customer_proxy_username_count": username_count,
        "customer_proxy_nickname_count": nickname_count,
        "customer_proxy_recipient_count": recipient_count,
    }


def build_customer_metrics(raw_df: pd.DataFrame, full_history_df: pd.DataFrame | None = None) -> dict[str, Any]:
    order_level = valid_customer_orders(raw_df)
    full_order_level = valid_customer_orders(full_history_df if full_history_df is not None else raw_df)
    if order_level.empty:
        return {
            "selected_unique_customers": None,
            "selected_repeat_customers": None,
            "selected_first_time_buyers": None,
            "selected_returning_customers": None,
            "selected_repeat_customer_rate": None,
            "selected_first_time_buyer_rate": None,
            "customer_id_basis": "Buyer Username -> Buyer Nickname -> Recipient",
            **customer_proxy_mix(order_level),
        }
    customer_counts = order_level["customer_id"].value_counts()
    unique_customers = int(customer_counts.shape[0])
    repeat_customers = None
    first_time_buyers = None
    returning_customers = None
    if not full_order_level.empty:
        first_order_dates = full_order_level.groupby("customer_id", as_index=False).agg(first_order_date=("reporting_date", "min"))
        selected_customer_span = order_level.groupby("customer_id", as_index=False).agg(
            first_selected_date=("reporting_date", "min"),
            last_selected_date=("reporting_date", "max"),
        )
        joined = selected_customer_span.merge(first_order_dates, on="customer_id", how="left")
        joined["is_first_time_buyer"] = joined["first_order_date"].eq(joined["first_selected_date"])
        joined["is_repeat_customer"] = joined["first_order_date"].lt(joined["first_selected_date"])
        first_time_buyers = int(joined["is_first_time_buyer"].sum())
        repeat_customers = int(joined["is_repeat_customer"].sum())
        returning_customers = repeat_customers
    return {
        "selected_unique_customers": unique_customers,
        "selected_repeat_customers": repeat_customers,
        "selected_first_time_buyers": first_time_buyers,
        "selected_returning_customers": returning_customers,
        "selected_repeat_customer_rate": (repeat_customers / unique_customers) if unique_customers else None,
        "selected_first_time_buyer_rate": (first_time_buyers / unique_customers) if unique_customers and first_time_buyers is not None else None,
        "customer_id_basis": "Buyer Username -> Buyer Nickname -> Recipient",
        **customer_proxy_mix(order_level),
    }


def build_customer_first_order_rows(raw_df: pd.DataFrame) -> pd.DataFrame:
    order_level = valid_customer_orders(raw_df)
    if order_level.empty:
        return pd.DataFrame()
    return (
        order_level.groupby("customer_id", as_index=False)
        .agg(first_order_date=("reporting_date", "min"))
        .sort_values("first_order_date")
        .reset_index(drop=True)
    )


def build_cohort_retention(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    order_level = valid_customer_orders(raw_df)
    if order_level.empty:
        return pd.DataFrame(), pd.DataFrame()
    first_orders = order_level.groupby("customer_id", as_index=False).agg(first_order_month=("order_month", "min"))
    cohort_df = order_level.merge(first_orders, on="customer_id", how="left")
    cohort_df["months_since_first"] = (
        (cohort_df["order_month"].dt.year - cohort_df["first_order_month"].dt.year) * 12
        + (cohort_df["order_month"].dt.month - cohort_df["first_order_month"].dt.month)
    )
    active = cohort_df.groupby(["first_order_month", "months_since_first"], as_index=False).agg(active_customers=("customer_id", "nunique"))
    cohort_size = (
        cohort_df.loc[cohort_df["months_since_first"].eq(0)]
        .groupby("first_order_month", as_index=False)
        .agg(cohort_size=("customer_id", "nunique"))
    )
    retention = active.merge(cohort_size, on="first_order_month", how="left")
    retention["retention_rate"] = retention["active_customers"] / retention["cohort_size"]
    retention["cohort_label"] = retention["first_order_month"].dt.strftime("%Y-%m")
    heatmap = (
        retention.pivot(index="cohort_label", columns="months_since_first", values="retention_rate")
        .sort_index(ascending=False)
        .fillna(0)
    )
    summary = retention.sort_values(["first_order_month", "months_since_first"]).reset_index(drop=True)
    return heatmap, summary


def filter_cohort_window(cohort_summary: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame]:
    if cohort_summary is None or cohort_summary.empty:
        return pd.DataFrame(), pd.DataFrame()
    start_month = start_date.to_period("M").to_timestamp()
    end_month = end_date.to_period("M").to_timestamp()
    visible = cohort_summary.loc[cohort_summary["first_order_month"].between(start_month, end_month)].copy()
    if visible.empty:
        return pd.DataFrame(), pd.DataFrame()
    heatmap = (
        visible.assign(cohort_label=visible["first_order_month"].dt.strftime("%Y-%m"))
        .pivot(index="cohort_label", columns="months_since_first", values="retention_rate")
        .sort_index(ascending=False)
        .fillna(0)
    )
    return heatmap, visible


def build_order_daily_table(finance_df: pd.DataFrame, operational_df: pd.DataFrame) -> pd.DataFrame:
    if finance_df is None or finance_df.empty:
        return pd.DataFrame()
    finance_daily = (
        finance_df.groupby("reporting_date", as_index=False)
        .agg(
            gross_product_sales=("gross_product_sales", "sum"),
            net_product_sales=("approx_product_sales_after_export_refunds", "sum"),
            seller_discount=("seller_discount", "sum"),
            export_refund_amount=("order_refund_amount", "sum"),
            paid_orders=("paid_orders", "sum"),
        )
        .sort_values("reporting_date", ascending=False)
    )
    if operational_df is None or operational_df.empty:
        finance_daily["sales_units"] = 0.0
        finance_daily["sample_units"] = 0.0
        finance_daily["replacement_units"] = 0.0
        finance_daily["operational_units"] = 0.0
        return finance_daily
    units_daily = (
        operational_df.groupby(["reporting_date", "source_type"], as_index=False)
        .agg(units_sold=("Quantity", "sum"))
        .pivot(index="reporting_date", columns="source_type", values="units_sold")
        .fillna(0)
        .reset_index()
    )
    if "Sales" not in units_daily.columns:
        units_daily["Sales"] = 0.0
    if "Samples" not in units_daily.columns:
        units_daily["Samples"] = 0.0
    if "Replacements" not in units_daily.columns:
        units_daily["Replacements"] = 0.0
    units_daily["operational_units"] = units_daily["Sales"] + units_daily["Samples"] + units_daily["Replacements"]
    units_daily = units_daily.rename(columns={"Sales": "sales_units", "Samples": "sample_units", "Replacements": "replacement_units"})
    return finance_daily.merge(units_daily, on="reporting_date", how="left").fillna(0)


def build_statement_daily_table(statement_rows: pd.DataFrame) -> pd.DataFrame:
    if statement_rows is None or statement_rows.empty:
        return pd.DataFrame()
    daily = (
        statement_rows.groupby("statement_date", as_index=False)
        .agg(
            gross_sales=("statement_gross_sales", "sum"),
            gross_sales_refund=("statement_gross_sales_refund", "sum"),
            seller_discount=("statement_seller_discount", "sum"),
            seller_discount_refund=("statement_seller_discount_refund", "sum"),
            net_sales=("statement_net_sales", "sum"),
            shipping_total=("statement_shipping", "sum"),
            fees_total=("statement_fees", "sum"),
            adjustments_total=("statement_adjustment_amount", "sum"),
            payout_amount=("total_settlement_amount", "sum"),
        )
        .rename(columns={"statement_date": "reporting_date"})
        .sort_values("reporting_date", ascending=False)
    )
    return daily.reset_index(drop=True)


def upgrade_cached_finance(raw_finance: pd.DataFrame) -> pd.DataFrame:
    if raw_finance is None or raw_finance.empty:
        return pd.DataFrame() if raw_finance is None else raw_finance.copy()
    working = raw_finance.copy()
    for column in ["reporting_date", "source_file_month"]:
        if column in working.columns:
            working[column] = pd.to_datetime(working[column], errors="coerce")
    if "reporting_date" not in working.columns and "Paid Time" in working.columns:
        paid_time = pd.to_datetime(working["Paid Time"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
        source_file_month = pd.to_datetime(working.get("source_file_month"), errors="coerce")
        working["reporting_date"] = paid_time.dt.normalize().fillna(source_file_month)
    return working


def upgrade_cached_operational(raw_operational: pd.DataFrame) -> pd.DataFrame:
    if raw_operational is None or raw_operational.empty:
        return pd.DataFrame() if raw_operational is None else raw_operational.copy()
    working = raw_operational.copy()

    if "source_file_month" not in working.columns:
        working["source_file_month"] = working.get("source_file", pd.Series("", index=working.index)).map(infer_month_start_from_filename)
    working["source_file_month"] = pd.to_datetime(working["source_file_month"], errors="coerce")

    if "paid_time_date" not in working.columns:
        paid_time = pd.to_datetime(working.get("Paid Time"), format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
        working["paid_time_date"] = paid_time.dt.normalize()
    else:
        working["paid_time_date"] = pd.to_datetime(working["paid_time_date"], errors="coerce")

    if "reporting_date" not in working.columns:
        working["reporting_date"] = working["paid_time_date"].fillna(working["source_file_month"])
    else:
        working["reporting_date"] = pd.to_datetime(working["reporting_date"], errors="coerce")

    if "paid_time_inferred_from_file_month" not in working.columns:
        working["paid_time_inferred_from_file_month"] = working["paid_time_date"].isna() & working["source_file_month"].notna()

    virtual_bundle_sku = working.get("Virtual Bundle Seller SKU", pd.Series("", index=working.index)).astype("string").fillna("").str.strip()
    seller_sku = working.get("Seller SKU", pd.Series("", index=working.index)).astype("string").fillna("").str.strip()
    if "seller_sku_resolved" not in working.columns:
        working["seller_sku_resolved"] = seller_sku.where(seller_sku.str.len().gt(0), virtual_bundle_sku)
    if "is_virtual_bundle_listing" not in working.columns:
        working["is_virtual_bundle_listing"] = virtual_bundle_sku.str.len().gt(0)

    if "customer_id" not in working.columns:
        customer_priority = [col for col in ["Buyer Username", "Buyer Nickname", "Recipient"] if col in working.columns]
        if customer_priority:
            customer_id = pd.Series("", index=working.index, dtype="string")
            customer_id_source = pd.Series("", index=working.index, dtype="string")
            for col in customer_priority:
                values = working[col].astype("string").fillna("").str.strip()
                use_mask = customer_id.str.len().eq(0) & values.str.len().gt(0)
                customer_id = customer_id.mask(use_mask, values)
                customer_id_source = customer_id_source.mask(use_mask, col)
            working["customer_id"] = customer_id
            working["customer_id_source"] = customer_id_source
        else:
            working["customer_id"] = ""
            working["customer_id_source"] = ""

    if "order_created_date" in working.columns:
        working["order_created_date"] = pd.to_datetime(working["order_created_date"], errors="coerce")
    elif "Created Time" in working.columns:
        created_time = pd.to_datetime(working["Created Time"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
        working["order_created_date"] = created_time.dt.normalize()

    return working


@dataclass
class DataStore:
    raw_finance: pd.DataFrame
    raw_operational: pd.DataFrame
    zip_reference: pd.DataFrame

    @classmethod
    def load(cls) -> "DataStore":
        latest_inputs = max(
            latest_source_mtime(),
            ZIP_REFERENCE_FILE.stat().st_mtime if ZIP_REFERENCE_FILE.exists() else 0.0,
        )
        if CACHE_FILE.exists() and CACHE_FILE.stat().st_mtime >= latest_inputs:
            cached = pd.read_pickle(CACHE_FILE)
            cache_version = cached.get("cache_schema_version", 1) if isinstance(cached, dict) else 1
            raw_finance = upgrade_cached_finance(cached["raw_finance"])
            raw_operational = upgrade_cached_operational(cached["raw_operational"])
            required_operational_columns = {
                "seller_sku_resolved",
                "is_virtual_bundle_listing",
                "customer_id",
                "source_type",
                "source_file_month",
                "paid_time_date",
                "reporting_date",
            }
            required_finance_columns = {"source_type", "reporting_date"}
            if required_operational_columns.issubset(raw_operational.columns) and required_finance_columns.issubset(raw_finance.columns):
                if cache_version != CACHE_SCHEMA_VERSION:
                    pd.to_pickle(
                        {
                            "cache_schema_version": CACHE_SCHEMA_VERSION,
                            "raw_finance": raw_finance,
                            "raw_operational": raw_operational,
                        },
                        CACHE_FILE,
                    )
                return cls(raw_finance=raw_finance, raw_operational=raw_operational, zip_reference=load_zip_reference())

        store = cls(
            raw_finance=load_paid_time_finance(),
            raw_operational=load_paid_time_operational(),
            zip_reference=load_zip_reference(),
        )
        pd.to_pickle(
            {
                "cache_schema_version": CACHE_SCHEMA_VERSION,
                "raw_finance": store.raw_finance,
                "raw_operational": store.raw_operational,
            },
            CACHE_FILE,
        )
        return store

    def available_sources(self) -> list[str]:
        values = sorted(self.raw_operational["source_type"].dropna().unique().tolist()) if not self.raw_operational.empty else []
        return values or ["Sales"]

    def min_date(self) -> str | None:
        if self.raw_finance.empty:
            return None
        return self.raw_finance["reporting_date"].min().strftime("%Y-%m-%d")

    def max_date(self) -> str | None:
        if self.raw_finance.empty:
            return None
        return self.raw_finance["reporting_date"].max().strftime("%Y-%m-%d")


def detect_order_source_dirs() -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    base_dir = source_base_dir()
    for folder_name, label in ORDER_SOURCE_FOLDERS.items():
        path = base_dir / folder_name
        if path.exists():
            found.append((str(path), label))
    return found


def latest_source_mtime() -> float:
    latest = 0.0
    for folder, _label in detect_order_source_dirs():
        for path in Path(folder).glob("*.csv"):
            latest = max(latest, path.stat().st_mtime)
    return latest


def load_paid_time_finance() -> pd.DataFrame:
    order_sources = detect_order_source_dirs()
    usecols = [
        "Order ID",
        "Paid Time",
        "SKU Subtotal Before Discount",
        "SKU Seller Discount",
        "SKU Platform Discount",
        "SKU Subtotal After Discount",
        "Order Amount",
        "Order Refund Amount",
        "Taxes",
        "Shipping Fee After Discount",
    ]
    frames: list[pd.DataFrame] = []
    for source_folder, source_label in order_sources:
        directory = Path(source_folder)
        for path in sorted(directory.glob("*.csv")):
            frame = pd.read_csv(path, dtype=str, keep_default_na=False, usecols=lambda col: col in usecols)
            if not frame.empty:
                frame["source_type"] = source_label
                frame["source_file"] = path.name
                frame["source_file_mtime"] = path.stat().st_mtime
                frame["source_file_month"] = infer_month_start_from_filename(path.name)
                frames.append(frame)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = df[df["Order ID"].astype(str).str.strip() != "Order ID"].copy()
    for col in df.columns:
        df[col] = df[col].astype(str).str.replace("\t", "", regex=False).str.strip()
    for col in [
        "SKU Subtotal Before Discount",
        "SKU Seller Discount",
        "SKU Platform Discount",
        "SKU Subtotal After Discount",
        "Order Amount",
        "Order Refund Amount",
        "Taxes",
        "Shipping Fee After Discount",
    ]:
        df[col] = pd.to_numeric(df[col].replace({"": None}), errors="coerce")
    df["Paid Time"] = pd.to_datetime(df["Paid Time"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
    df["source_file_month"] = pd.to_datetime(df["source_file_month"], errors="coerce")
    df["reporting_date"] = df["Paid Time"].dt.normalize()
    df["paid_time_inferred_from_file_month"] = df["reporting_date"].isna() & df["source_file_month"].notna()
    df["reporting_date"] = df["reporting_date"].fillna(df["source_file_month"])
    df = df.dropna(subset=["reporting_date"]).copy()
    df = keep_latest_file_rows_by_date(
        df,
        date_column="reporting_date",
        file_column="source_file",
        mtime_column="source_file_mtime",
        partition_columns=["source_type"],
    )
    if df.empty:
        return pd.DataFrame()
    line_daily = (
        df.groupby(["reporting_date", "source_type"], as_index=False)
        .agg(
            gross_product_sales=("SKU Subtotal Before Discount", "sum"),
            seller_discount=("SKU Seller Discount", "sum"),
            platform_discount=("SKU Platform Discount", "sum"),
            product_sales_after_all_discounts=("SKU Subtotal After Discount", "sum"),
        )
        .sort_values("reporting_date")
    )
    line_daily["product_sales_after_seller_discount"] = line_daily["gross_product_sales"] - line_daily["seller_discount"]
    order_daily = (
        df.groupby(["Order ID", "source_type"], as_index=False)
        .agg(
            reporting_date=("reporting_date", "min"),
            collected_order_amount=("Order Amount", "max"),
            order_refund_amount=("Order Refund Amount", "max"),
            taxes=("Taxes", "max"),
            shipping_fee_after_discount=("Shipping Fee After Discount", "max"),
        )
        .groupby(["reporting_date", "source_type"], as_index=False)
        .agg(
            paid_orders=("Order ID", "count"),
            collected_order_amount=("collected_order_amount", "sum"),
            order_refund_amount=("order_refund_amount", "sum"),
            taxes=("taxes", "sum"),
            shipping_fee_after_discount=("shipping_fee_after_discount", "sum"),
        )
        .sort_values("reporting_date")
    )
    daily = line_daily.merge(order_daily, on=["reporting_date", "source_type"], how="outer").fillna(0)
    daily["approx_product_sales_after_export_refunds"] = daily["product_sales_after_seller_discount"] - daily["order_refund_amount"]
    return daily.sort_values("reporting_date").reset_index(drop=True)


def load_paid_time_operational() -> pd.DataFrame:
    order_sources = detect_order_source_dirs()
    usecols = [
        "Order ID",
        "Order Status",
        "Order Substatus",
        "Cancelation/Return Type",
        "Cancelled Time",
        "Delivered Time",
        "Shipped Time",
        "Paid Time",
        "Seller SKU",
        " Virtual Bundle Seller SKU",
        "Virtual Bundle Seller SKU",
        "Product Name",
        "Combined Listing",
        "Buyer Username",
        "Buyer Nickname",
        "Recipient",
        "Created Time",
        "State",
        "City",
        "Zipcode",
        "Quantity",
        "Sku Quantity of return",
        "SKU Subtotal Before Discount",
        "SKU Seller Discount",
        "SKU Platform Discount",
        "SKU Subtotal After Discount",
        "Order Refund Amount",
        "Order Amount",
        "Taxes",
        "Shipping Fee After Discount",
    ]
    frames: list[pd.DataFrame] = []
    for source_folder, source_label in order_sources:
        directory = Path(source_folder)
        for path in sorted(directory.glob("*.csv")):
            frame = pd.read_csv(path, dtype=str, keep_default_na=False, usecols=lambda col: col in usecols)
            if not frame.empty:
                frame["source_type"] = source_label
                frame["source_file"] = path.name
                frame["source_file_mtime"] = path.stat().st_mtime
                frame["source_file_month"] = infer_month_start_from_filename(path.name)
                frames.append(frame)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = df[df["Order ID"].astype(str).str.strip() != "Order ID"].copy()
    for col in df.columns:
        df[col] = df[col].astype(str).str.replace("\t", "", regex=False).str.strip()
    for original in [" Virtual Bundle Seller SKU", "Virtual Bundle Seller SKU"]:
        if original in df.columns and "Virtual Bundle Seller SKU" not in df.columns:
            df = df.rename(columns={original: "Virtual Bundle Seller SKU"})
    for col in ["Buyer Username", "Buyer Nickname", "Recipient"]:
        if col in df.columns:
            df[col] = df[col].astype("string").fillna("").str.strip()
    for col in ["Seller SKU", "Virtual Bundle Seller SKU", "Combined Listing"]:
        if col in df.columns:
            df[col] = df[col].astype("string").fillna("").str.strip()
    for col in [
        "Quantity",
        "Sku Quantity of return",
        "SKU Subtotal Before Discount",
        "SKU Seller Discount",
        "SKU Platform Discount",
        "SKU Subtotal After Discount",
        "Order Refund Amount",
        "Order Amount",
        "Taxes",
        "Shipping Fee After Discount",
    ]:
        df[col] = pd.to_numeric(df[col].replace({"": None}), errors="coerce")
    for col in ["Created Time", "Paid Time", "Cancelled Time", "Delivered Time", "Shipped Time"]:
        df[col] = pd.to_datetime(df[col], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
    df["source_file_month"] = pd.to_datetime(df["source_file_month"], errors="coerce")
    df["paid_time_date"] = df["Paid Time"].dt.normalize()
    df["reporting_date"] = df["paid_time_date"].fillna(df["source_file_month"])
    df["paid_time_inferred_from_file_month"] = df["paid_time_date"].isna() & df["source_file_month"].notna()
    df = df.dropna(subset=["reporting_date"]).copy()
    df = keep_latest_file_rows_by_date(
        df,
        date_column="reporting_date",
        file_column="source_file",
        mtime_column="source_file_mtime",
        partition_columns=["source_type"],
    )
    if df.empty:
        return pd.DataFrame()
    df["order_created_date"] = df["Created Time"].dt.normalize()
    status_text = (df["Order Status"].fillna("") + " | " + df["Order Substatus"].fillna("")).str.lower()
    cancel_type = df["Cancelation/Return Type"].fillna("").str.lower()
    df["is_canceled"] = status_text.str.contains("cancel", na=False) | df["Cancelled Time"].notna() | cancel_type.str.contains("cancel", na=False)
    df["is_refunded"] = df["Order Refund Amount"].fillna(0).gt(0) | cancel_type.str.contains("refund", na=False)
    df["is_returned"] = df["Sku Quantity of return"].fillna(0).gt(0) | cancel_type.str.contains("return", na=False)
    df["is_delivered"] = df["Delivered Time"].notna() | status_text.str.contains("delivered|completed", na=False)
    df["is_shipped"] = df["Shipped Time"].notna() | status_text.str.contains("shipped|in transit|awaiting collection", na=False)
    if "Virtual Bundle Seller SKU" in df.columns:
        df["seller_sku_resolved"] = df["Seller SKU"].where(df["Seller SKU"].str.len().gt(0), df["Virtual Bundle Seller SKU"])
    else:
        df["seller_sku_resolved"] = df["Seller SKU"]
    df["is_virtual_bundle_listing"] = df.get("Virtual Bundle Seller SKU", pd.Series("", index=df.index)).astype("string").str.len().gt(0)
    customer_priority = [col for col in ["Buyer Username", "Buyer Nickname", "Recipient"] if col in df.columns]
    if customer_priority:
        customer_id = pd.Series("", index=df.index, dtype="string")
        customer_id_source = pd.Series("", index=df.index, dtype="string")
        for col in customer_priority:
            values = df[col].astype("string").fillna("").str.strip()
            use_mask = customer_id.str.len().eq(0) & values.str.len().gt(0)
            customer_id = customer_id.mask(use_mask, values)
            customer_id_source = customer_id_source.mask(use_mask, col)
        df["customer_id"] = customer_id
        df["customer_id_source"] = customer_id_source
    else:
        df["customer_id"] = ""
        df["customer_id_source"] = ""
    return df.reset_index(drop=True)


def detect_output_dirs() -> list[Path]:
    return sorted(
        [path for path in BASE_DIR.iterdir() if path.is_dir() and path.name.startswith("analysis_output")],
        key=lambda path: path.name,
    )


def load_output_data(output_name: str | None) -> dict[str, Any]:
    choices = detect_output_dirs()
    default = DEFAULT_OUTPUT_DIR if DEFAULT_OUTPUT_DIR.exists() else (choices[0] if choices else None)
    selected = default
    if output_name:
        candidate = BASE_DIR / output_name
        if candidate.exists() and candidate.is_dir():
            selected = candidate
    if selected is None:
        return {"directory": None, "kpi_full": pd.DataFrame(), "report_md": ""}
    kpi_path = selected / "kpi_full.csv"
    report_path = selected / "report.md"
    kpi_df = pd.read_csv(kpi_path) if kpi_path.exists() else pd.DataFrame()
    report_md = report_path.read_text(encoding="utf-8") if report_path.exists() else ""
    return {"directory": selected.name, "kpi_full": kpi_df, "report_md": report_md}


def get_store() -> DataStore:
    global STORE
    if STORE is None:
        STORE = DataStore.load()
    return STORE


def dashboard_signature() -> str:
    parts = [
        str(CACHE_SCHEMA_VERSION),
        str(CACHE_FILE.stat().st_mtime if CACHE_FILE.exists() else 0.0),
        str(STATEMENT_CACHE_FILE.stat().st_mtime if STATEMENT_CACHE_FILE.exists() else 0.0),
        str(INVENTORY_CACHE_FILE.stat().st_mtime if INVENTORY_CACHE_FILE.exists() else 0.0),
    ]
    for path in detect_output_dirs():
        parts.append(path.name)
        parts.append(str(path.stat().st_mtime if path.exists() else 0.0))
    return "|".join(parts)


def meta_payload() -> dict[str, Any]:
    store = get_store()
    output_dirs = [path.name for path in detect_output_dirs()]
    statement_min, statement_max = statement_date_bounds()
    inventory_history = load_tiktok_inventory_history()
    inventory_snapshot = latest_tiktok_inventory_snapshot(inventory_history)
    return {
        "availableSources": store.available_sources(),
        "minDate": store.min_date(),
        "maxDate": store.max_date(),
        "statementMinDate": statement_min,
        "statementMaxDate": statement_max,
        "inventorySnapshotDate": json_safe(inventory_snapshot.get("snapshot_date")),
        "statementFilesDetected": len(detect_statement_sources()),
        "outputDirs": output_dirs,
        "defaultOutputDir": DEFAULT_OUTPUT_DIR.name if DEFAULT_OUTPUT_DIR.exists() else (output_dirs[0] if output_dirs else None),
        "orderBucketModes": ["paid_time", "file_month"],
        "uploadTargets": [{"value": key, "label": value} for key, value in UPLOAD_TARGET_FOLDERS.items()],
        "planningDefaults": planning_defaults(),
    }


def dashboard_payload(params: dict[str, list[str]]) -> dict[str, Any]:
    signature = dashboard_signature()
    cached_payload = DASHBOARD_RESPONSE_CACHE.get(params, signature)
    if cached_payload is not None:
        return cached_payload

    store = get_store()
    start_raw = params.get("start", [store.min_date()])[0]
    end_raw = params.get("end", [store.max_date()])[0]
    output_dir = params.get("output", [None])[0]
    sources_raw = params.get("sources", [",".join(store.available_sources())])[0]
    date_basis = params.get("date_basis", ["order"])[0]
    order_bucket_mode = params.get("order_bucket_mode", ["paid_time"])[0]
    target_zip = params.get("target_zip", [""])[0]
    radius_miles = int(params.get("radius_miles", ["20"])[0] or 20)
    target_city = params.get("target_city", [""])[0]
    target_state = params.get("target_state", [""])[0]
    city_radius_miles = int(params.get("city_radius_miles", [str(radius_miles)])[0] or radius_miles)
    planning_baseline = params.get("planning_baseline", ["last_30_days"])[0]
    planning_baseline_start_raw = params.get("planning_baseline_start", [""])[0]
    planning_baseline_end_raw = params.get("planning_baseline_end", [""])[0]
    planning_horizon_start_raw = params.get("planning_horizon_start", [""])[0]
    planning_horizon_end_raw = params.get("planning_horizon_end", [""])[0]
    planning_default_uplift = float(params.get("planning_default_uplift", [str(DEFAULT_FORECAST_UPLIFT_PCT)])[0] or DEFAULT_FORECAST_UPLIFT_PCT)
    selected_sources = [part for part in sources_raw.split(",") if part] or store.available_sources()

    start_date = pd.Timestamp(start_raw)
    end_date = pd.Timestamp(end_raw)
    planning_baseline_start = pd.Timestamp(planning_baseline_start_raw) if str(planning_baseline_start_raw).strip() else start_date
    planning_baseline_end = pd.Timestamp(planning_baseline_end_raw) if str(planning_baseline_end_raw).strip() else end_date
    planning_horizon_start = pd.Timestamp(planning_horizon_start_raw) if str(planning_horizon_start_raw).strip() else start_date
    planning_horizon_end = pd.Timestamp(planning_horizon_end_raw) if str(planning_horizon_end_raw).strip() else end_date
    planning_horizon_start, planning_horizon_end = resolve_planning_horizon(
        start_date,
        end_date,
        planning_horizon_start,
        planning_horizon_end,
    )

    output_data = load_output_data(output_dir)
    kpi_full_df = output_data["kpi_full"]
    report_md = output_data["report_md"]
    statement_rows = load_statement_rows(start_date, end_date)
    statement_rows_all = load_statement_rows()
    filtered_statement_rows = (
        statement_rows.loc[statement_rows["statement_date"].between(start_date, end_date)].copy()
        if statement_rows is not None and not statement_rows.empty
        else pd.DataFrame()
    )

    selected_source_operational_df = store.raw_operational.loc[store.raw_operational["source_type"].isin(selected_sources)].copy()
    selected_source_operational_df = apply_order_bucket_mode(selected_source_operational_df, order_bucket_mode)
    latest_operational_date = (
        pd.to_datetime(selected_source_operational_df["reporting_date"], errors="coerce").max()
        if selected_source_operational_df is not None and not selected_source_operational_df.empty
        else end_date
    )
    baseline_start, baseline_end, baseline_label = choose_baseline_window(
        latest_operational_date,
        planning_baseline,
        start_date,
        end_date,
        planning_baseline_start,
        planning_baseline_end,
    )
    forecast_overrides = {product: planning_default_uplift for product in FORECAST_PRODUCT_PARAM_MAP}
    for product, param_name in FORECAST_PRODUCT_PARAM_MAP.items():
        raw_value = params.get(param_name, [""])[0]
        if str(raw_value).strip():
            forecast_overrides[product] = float(raw_value)
    filtered_operational_df = filter_daily(selected_source_operational_df, start_date, end_date, selected_sources)
    planning_operational_df = filter_daily(selected_source_operational_df, baseline_start, baseline_end, selected_sources)
    filtered_finance_df = build_order_export_finance_view(filtered_operational_df)
    order_level_df = build_order_level_view(selected_source_operational_df)
    filtered_order_level_df = (
        order_level_df.loc[order_level_df["reporting_date"].between(start_date, end_date)].copy()
        if order_level_df is not None and not order_level_df.empty
        else pd.DataFrame()
    )

    order_summary = summarize_order_period(filtered_finance_df)
    statement_summary = summarize_statement_period(filtered_statement_rows)
    source_units = summarize_source_units(filtered_operational_df)
    order_health_metrics = build_order_health_metrics(filtered_order_level_df)
    customer_metrics = build_customer_metrics(filtered_operational_df, selected_source_operational_df)
    statement_fee_breakdown_df = build_statement_fee_breakdown(filtered_statement_rows)
    expense_structure_df, expense_structure_detail_df = build_statement_expense_structure(filtered_statement_rows)
    data_quality_summary = build_data_quality_summary(filtered_operational_df, order_bucket_mode)
    raw_product_name_df = build_raw_product_name_view(filtered_operational_df)
    raw_product_name_all_df = build_raw_product_name_rows(selected_source_operational_df)
    filtered_product_df = build_filtered_product_view(filtered_operational_df)
    planning_product_df = build_filtered_product_view(planning_operational_df)
    filtered_status_df = build_filtered_status_view(filtered_operational_df)
    listing_cogs_df, component_cogs_df = build_cogs_views(filtered_product_df)
    _planning_listing_cogs_df, planning_component_cogs_df = build_cogs_views(planning_product_df)
    customer_first_order_df = build_customer_first_order_rows(selected_source_operational_df)
    inventory_history_df = load_tiktok_inventory_history()
    inventory_snapshot = latest_tiktok_inventory_snapshot(inventory_history_df)
    inventory_planning_df = build_inventory_planning_rows(
        planning_component_cogs_df,
        inventory_snapshot,
        planning_horizon_start,
        planning_horizon_end,
        baseline_start,
        baseline_end,
        forecast_overrides,
        baseline_label,
    )
    math_audit_rows = build_math_audit(filtered_operational_df, filtered_product_df, component_cogs_df)
    reconciliation_rows, unmatched_statement_rows, unmatched_order_rows, reconciliation_summary = build_reconciliation_view(
        order_level_df,
        filtered_statement_rows,
        date_basis,
        start_date,
        end_date,
    )
    location_views = build_location_views(
        filtered_order_level_df,
        store.zip_reference,
        target_zip,
        radius_miles,
        target_city,
        target_state,
        city_radius_miles,
    )
    cohort_heatmap_all, cohort_summary_all = build_cohort_retention(selected_source_operational_df)
    cohort_heatmap, cohort_summary = filter_cohort_window(cohort_summary_all, start_date, end_date)
    order_daily_df = build_order_daily_table(filtered_finance_df, filtered_operational_df)
    statement_daily_df = build_statement_daily_table(filtered_statement_rows)
    product_daily_df = build_product_daily_view(filtered_operational_df)
    order_level_geo_df = build_order_level_geo_view(order_level_df, store.zip_reference)

    summary = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "selected_sources": selected_sources,
        "date_basis": date_basis,
        "order_bucket_mode": order_bucket_mode,
        "target_zip": normalize_zip(target_zip),
        "radius_miles": radius_miles,
        "target_city": target_city.strip() if isinstance(target_city, str) else "",
        "target_state": target_state.strip().upper() if isinstance(target_state, str) else "",
        "city_radius_miles": city_radius_miles,
        "planning_baseline": planning_baseline,
        "planning_baseline_start": baseline_start.strftime("%Y-%m-%d"),
        "planning_baseline_end": baseline_end.strftime("%Y-%m-%d"),
        "planning_horizon_start": planning_horizon_start.strftime("%Y-%m-%d"),
        "planning_horizon_end": planning_horizon_end.strftime("%Y-%m-%d"),
        "planning_default_uplift": planning_default_uplift,
    }

    cohort_heatmap_payload: list[dict[str, Any]] = []
    if not cohort_heatmap.empty:
        for cohort_label, row in cohort_heatmap.iterrows():
            cohort_heatmap_payload.append(
                {"cohort": cohort_label, "values": {str(col): json_safe(val) for col, val in row.items()}}
            )

    kpi_rows = []
    if not kpi_full_df.empty:
        kpi_rows = records(kpi_full_df[["metric", "category", "formatted_value", "formula", "notes"]])

    payload = {
        "summary": {key: json_safe(value) for key, value in summary.items()},
        "orderSummary": {key: json_safe(value) for key, value in {**order_summary, **source_units, **order_health_metrics, **customer_metrics}.items()},
        "statementSummary": {key: json_safe(value) for key, value in statement_summary.items()},
        "reconciliationSummary": {key: json_safe(value) for key, value in reconciliation_summary.items()},
        "statementFeeBreakdownRows": records(statement_fee_breakdown_df),
        "expenseStructureRows": records(expense_structure_df),
        "expenseStructureDetailRows": records(expense_structure_detail_df),
        "dataQualitySummary": {key: json_safe(value) for key, value in data_quality_summary.items()},
        "orderDailyRows": records(order_daily_df.sort_values("reporting_date", ascending=False)),
        "statementDailyRows": records(statement_daily_df.sort_values("reporting_date", ascending=False)),
        "statementRowsAll": records(statement_rows_all.sort_values("statement_date", ascending=False)),
        "statusRows": records(filtered_status_df),
        "rawProductNameRows": records(raw_product_name_df.head(100)),
        "rawProductNameAllRows": records(raw_product_name_all_df.sort_values("reporting_date", ascending=False)),
        "productRows": records(filtered_product_df.head(50)),
        "productDailyRows": records(product_daily_df),
        "orderLevelRowsAll": records(order_level_geo_df.sort_values("reporting_date", ascending=False)),
        "customerFirstOrderAllRows": records(customer_first_order_df.sort_values("first_order_date", ascending=False)),
        "cogsSummaryRows": records(component_cogs_df),
        "cogsListingRows": records(listing_cogs_df.head(50)),
        "inventoryHistoryRows": records(inventory_history_df.sort_values("date", ascending=False).head(365)),
        "inventoryPlanningRows": records(inventory_planning_df),
        "inventorySnapshot": {key: json_safe(value) for key, value in inventory_snapshot.items()} if inventory_snapshot else {},
        "planningConfig": {
            "baselineLabel": baseline_label,
            "baselineStart": json_safe(baseline_start),
            "baselineEnd": json_safe(baseline_end),
            "horizonStart": json_safe(planning_horizon_start),
            "horizonEnd": json_safe(planning_horizon_end),
            "defaultUpliftPct": planning_default_uplift,
            "productForecasts": [{"product": product, "uplift_pct": json_safe(value)} for product, value in forecast_overrides.items()],
        },
        "mathAuditRows": records(pd.DataFrame(math_audit_rows)),
        "reconciliationRows": records(reconciliation_rows.head(200)),
        "unmatchedStatementRows": records(unmatched_statement_rows.head(200)),
        "unmatchedOrderRows": records(unmatched_order_rows.head(200)),
        "cityRows": records(location_views["cities"]),
        "zipRows": records(location_views["zips"]),
        "radiusRows": records(location_views["radius"]),
        "radiusSummary": {key: json_safe(value) for key, value in location_views["radius_summary"].items()},
        "targetCityRows": records(location_views["target_city_rows"]),
        "targetCitySummary": {key: json_safe(value) for key, value in location_views["target_city_summary"].items()},
        "cohortHeatmap": cohort_heatmap_payload,
        "cohortSummaryRows": records(cohort_summary),
        "cohortSummaryAllRows": records(cohort_summary_all),
        "kpiDefinitionRows": kpi_rows,
        "reportMarkdown": report_md,
        "selectedOutputDir": output_data["directory"],
    }
    DASHBOARD_RESPONSE_CACHE.set(params, signature, payload)
    return payload


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(WEB_DIR), **kwargs)

    def end_headers(self) -> None:
        # Prevent the browser from hanging on to stale HTML/JS/CSS while the local dashboard evolves.
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/meta":
            self.respond_json(meta_payload())
            return
        if parsed.path == "/api/dashboard":
            self.respond_json(dashboard_payload(parse_qs(parsed.query)))
            return
        if parsed.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/upload":
            self.handle_upload()
            return
        if parsed.path == "/api/rebuild":
            self.handle_rebuild()
            return
        self.send_error(404, "Not Found")

    def log_message(self, format: str, *args: Any) -> None:
        return

    def respond_json(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def respond_json_error(self, status: int, message: str) -> None:
        body = json.dumps({"error": message}).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_upload(self) -> None:
        content_type = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in content_type:
            self.respond_json_error(400, "Upload must use multipart form data.")
            return

        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={
                "REQUEST_METHOD": "POST",
                "CONTENT_TYPE": content_type,
            },
        )
        upload_kind = form.getfirst("upload_kind", "")
        use_hosted_uploads = hosted_uploads_enabled()
        try:
            target_dir = None if use_hosted_uploads else upload_directory_for_kind(BASE_DIR, upload_kind)
        except ValueError as error:
            self.respond_json_error(400, str(error))
            return

        file_fields = form["files"] if "files" in form else []
        if not isinstance(file_fields, list):
            file_fields = [file_fields]

        saved_files: list[dict[str, Any]] = []
        for field in file_fields:
            if not getattr(field, "filename", ""):
                continue
            try:
                filename = sanitize_upload_filename(field.filename)
            except ValueError as error:
                self.respond_json_error(400, str(error))
                return
            payload = field.file.read()
            if use_hosted_uploads:
                try:
                    uploaded = upload_hosted_file(upload_kind, filename, payload, notes="Uploaded from local dashboard")
                except Exception as error:  # noqa: BLE001
                    self.respond_json_error(500, f"Hosted upload failed: {error}")
                    return
                saved_files.append(
                    {
                        "filename": uploaded.get("original_filename", filename),
                        "size": uploaded.get("file_size_bytes", len(payload)),
                        "folder": upload_kind,
                        "storage_path": uploaded.get("storage_path"),
                    }
                )
            else:
                destination = target_dir / filename
                destination.write_bytes(payload)
                saved_files.append({"filename": filename, "size": len(payload), "folder": target_dir.name})

        if not saved_files:
            self.respond_json_error(400, "Choose at least one file to upload.")
            return

        global STORE
        STORE = None
        DASHBOARD_RESPONSE_CACHE.clear()
        if use_hosted_uploads:
            self.respond_json(
                {
                    "message": f"Uploaded {len(saved_files)} file(s) to hosted storage.",
                    "savedFiles": saved_files,
                    "storageMode": "supabase",
                    "rebuildTriggered": False,
                }
            )
            return
        self.respond_json({"message": f"Uploaded {len(saved_files)} file(s) to {target_dir.name}.", "savedFiles": saved_files, "storageMode": "local"})

    def handle_rebuild(self) -> None:
        if hosted_uploads_enabled():
            try:
                from deployment.sync_dashboard_to_supabase import main as sync_dashboard_to_supabase

                sync_dashboard_to_supabase()
            except Exception as error:  # noqa: BLE001
                self.respond_json_error(500, f"Hosted rebuild failed: {error}")
                return
            self.respond_json({"message": "Snapshot rebuild triggered.", "rebuildTriggered": True, "storageMode": "supabase"})
            return

        global STORE
        STORE = None
        DASHBOARD_RESPONSE_CACHE.clear()
        self.respond_json({"message": "Local dashboard cache cleared.", "rebuildTriggered": True, "storageMode": "local"})


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 8080), DashboardHandler)
    print("Dashboard running at http://127.0.0.1:8080")
    server.serve_forever()


if __name__ == "__main__":
    main()
