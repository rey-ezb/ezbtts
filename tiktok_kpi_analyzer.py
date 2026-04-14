#!/usr/bin/env python3
"""
TikTok Shop KPI analyzer for EZ Bombs order exports.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_CONFIG = {
    "input_paths": [],
    "exclude_globs": [
        "**/analysis_output/**",
        "**/kpi_summary.csv",
        "**/kpi_full.csv",
        "**/daily_breakdown.csv",
        "**/product_kpis.csv",
        "**/report.md",
    ],
    "prefer_folder": "All orders",
    "include_auxiliary_folders": False,
    "auxiliary_folder_names": ["Samples", "Replacements"],
    "output_dir": "analysis_output",
    "column_overrides": {},
    "kpi": {
        "report_date_basis": "paid_time",
        "fallback_date_basis": ["created_time", "shipped_time", "delivered_time"],
        "customer_id_priority": ["buyer_username", "buyer_nickname", "recipient"],
    },
}


CANONICAL_COLUMN_ALIASES = {
    "order_id": ["Order ID"],
    "order_status": ["Order Status"],
    "order_substatus": ["Order Substatus"],
    "cancel_return_type": ["Cancelation/Return Type", "Cancellation/Return Type"],
    "normal_preorder": ["Normal or Pre-order"],
    "sku_id": ["SKU ID"],
    "seller_sku": ["Seller SKU"],
    "virtual_bundle_seller_sku": [" Virtual Bundle Seller SKU", "Virtual Bundle Seller SKU"],
    "product_name": ["Product Name"],
    "variation": ["Variation"],
    "quantity": ["Quantity"],
    "returned_quantity": ["Sku Quantity of return", "SKU Quantity of return"],
    "sku_unit_original_price": ["SKU Unit Original Price"],
    "sku_subtotal_before_discount": ["SKU Subtotal Before Discount"],
    "sku_platform_discount": ["SKU Platform Discount"],
    "sku_seller_discount": ["SKU Seller Discount"],
    "sku_subtotal_after_discount": ["SKU Subtotal After Discount"],
    "shipping_fee_after_discount": ["Shipping Fee After Discount"],
    "original_shipping_fee": ["Original Shipping Fee"],
    "shipping_fee_seller_discount": ["Shipping Fee Seller Discount"],
    "cofunded_shipping_fee_discount": ["Co-Funded Shipping Fee Discount"],
    "shipping_fee_platform_discount": ["Shipping Fee Platform Discount"],
    "payment_platform_discount": ["Payment platform discount"],
    "retail_delivery_fee": ["Retail Delivery Fee"],
    "taxes": ["Taxes"],
    "order_amount": ["Order Amount"],
    "order_refund_amount": ["Order Refund Amount"],
    "created_time": ["Created Time"],
    "paid_time": ["Paid Time"],
    "rts_time": ["RTS Time"],
    "shipped_time": ["Shipped Time"],
    "delivered_time": ["Delivered Time"],
    "cancelled_time": ["Cancelled Time", "Canceled Time"],
    "cancel_by": ["Cancel By"],
    "cancel_reason": ["Cancel Reason"],
    "fulfillment_type": ["Fulfillment Type"],
    "warehouse_name": ["Warehouse Name"],
    "tracking_id": ["Tracking ID"],
    "delivery_option_type": ["Delivery Option Type"],
    "delivery_option": ["Delivery Option"],
    "shipping_provider_name": ["Shipping Provider Name"],
    "buyer_message": ["Buyer Message"],
    "buyer_nickname": ["Buyer Nickname"],
    "buyer_username": ["Buyer Username"],
    "recipient": ["Recipient"],
    "phone": ["Phone #"],
    "country": ["Country"],
    "state": ["State"],
    "city": ["City"],
    "zipcode": ["Zipcode"],
    "address_line_1": ["Address Line 1"],
    "address_line_2": ["Address Line 2"],
    "delivery_instruction": ["Delivery Instruction"],
    "payment_method": ["Payment Method"],
    "weight_kg": ["Weight(kg)"],
    "product_category": ["Product Category"],
    "package_id": ["Package ID"],
    "seller_note": ["Seller Note"],
    "shipping_information": ["Shipping Information"],
    "combined_listing": ["Combined Listing"],
}


LINE_SUM_COLUMNS = [
    "quantity",
    "returned_quantity",
    "sku_subtotal_before_discount",
    "sku_platform_discount",
    "sku_seller_discount",
    "sku_subtotal_after_discount",
]


ORDER_SINGLE_VALUE_COLUMNS = [
    "order_amount",
    "order_refund_amount",
    "shipping_fee_after_discount",
    "original_shipping_fee",
    "shipping_fee_seller_discount",
    "cofunded_shipping_fee_discount",
    "shipping_fee_platform_discount",
    "payment_platform_discount",
    "retail_delivery_fee",
    "taxes",
]


NUMERIC_COLUMNS = [
    "quantity",
    "returned_quantity",
    "sku_unit_original_price",
    "sku_subtotal_before_discount",
    "sku_platform_discount",
    "sku_seller_discount",
    "sku_subtotal_after_discount",
    "shipping_fee_after_discount",
    "original_shipping_fee",
    "shipping_fee_seller_discount",
    "cofunded_shipping_fee_discount",
    "shipping_fee_platform_discount",
    "payment_platform_discount",
    "retail_delivery_fee",
    "taxes",
    "order_amount",
    "order_refund_amount",
    "weight_kg",
]


DATETIME_COLUMNS = [
    "created_time",
    "paid_time",
    "rts_time",
    "shipped_time",
    "delivered_time",
    "cancelled_time",
]


ESSENTIAL_COLUMNS = ["order_id", "order_status", "order_substatus", "product_name", "quantity"]


def normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def clean_string(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value).replace("\u200b", "").replace("\ufeff", "")
    text = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    text = text.replace("\t", " ")
    return re.sub(r"\s+", " ", text).strip()


def parse_numeric(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype("string")
        .fillna("")
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace(r"^\((.*)\)$", r"-\1", regex=True)
        .str.strip()
    )
    cleaned = cleaned.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return pd.to_numeric(cleaned, errors="coerce")


def normalize_identifier_text(value: Any) -> str:
    text = clean_string(value)
    if not text:
        return ""
    text = re.sub(r"\.0$", "", text)
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?[eE][+-]?\d+", text):
        try:
            normalized = format(Decimal(text), "f")
            if "." in normalized:
                normalized = normalized.rstrip("0").rstrip(".")
            return normalized
        except InvalidOperation:
            return text
    return text


def choose_first_non_empty(row: pd.Series, columns: list[str]) -> str:
    for column in columns:
        if column in row.index:
            value = clean_string(row[column])
            if value:
                return value
    return ""


def bool_rate(numerator: float, denominator: float) -> float | None:
    if denominator in (0, None) or pd.isna(denominator):
        return None
    return numerator / denominator


def format_scalar(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "not available from this export"
    if isinstance(value, (int, float)):
        return f"{value:,.2f}" if isinstance(value, float) else f"{value:,}"
    return str(value)


@dataclass
class WarningLog:
    items: list[str] = field(default_factory=list)

    def add(self, message: str) -> None:
        if message not in self.items:
            self.items.append(message)


@dataclass
class MetricRecord:
    metric: str
    category: str
    value: Any
    status: str
    formula: str
    columns_used: str
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "category": self.category,
            "value": self.value if self.value is not None else "",
            "formatted_value": format_scalar(self.value),
            "status": self.status,
            "formula": self.formula,
            "columns_used": self.columns_used,
            "notes": self.notes,
        }


class TikTokKPIAnalyzer:
    def __init__(self, config: dict[str, Any], base_dir: Path) -> None:
        self.config = config
        self.base_dir = base_dir
        self.warning_log = WarningLog()
        self.detected_columns: dict[str, str] = {}
        self.available_columns: dict[Path, list[str]] = {}
        self.used_date_basis = ""
        self.input_files: list[Path] = []
        self.excluded_files: list[Path] = []

    def run(self, explicit_inputs: list[str] | None = None, output_dir: str | None = None) -> None:
        self.input_files, self.excluded_files = self.discover_input_files(explicit_inputs or [])
        if not self.input_files:
            raise SystemExit("No input export files were found.")

        self.collect_available_columns()
        self.detected_columns = self.resolve_column_mapping()
        self.validate_mapping()

        raw_df = self.load_input_files()
        cleaned_df = self.clean_and_normalize(raw_df)
        order_df = self.build_order_level(cleaned_df)
        daily_df, weekly_df, monthly_df = self.build_time_breakdowns(order_df)
        product_df = self.build_product_table(cleaned_df, order_df)
        metrics = self.build_kpis(cleaned_df, order_df, daily_df, weekly_df, monthly_df, product_df)

        destination = Path(output_dir or self.config.get("output_dir") or "analysis_output")
        if not destination.is_absolute():
            destination = self.base_dir / destination
        destination.mkdir(parents=True, exist_ok=True)

        self.write_outputs(destination, metrics, daily_df, product_df)
        self.write_report(destination, metrics, daily_df, weekly_df, monthly_df, product_df, cleaned_df, order_df)

    def discover_input_files(self, explicit_inputs: list[str]) -> tuple[list[Path], list[Path]]:
        if explicit_inputs:
            files = []
            for value in explicit_inputs:
                path = Path(value).expanduser()
                if not path.is_absolute():
                    path = (self.base_dir / path).resolve()
                else:
                    path = path.resolve()
                if path.is_dir():
                    files.extend(self._files_from_directory(path))
                elif path.exists():
                    files.append(path)
        elif self.config.get("input_paths"):
            files = []
            for value in self.config["input_paths"]:
                path = Path(value)
                if not path.is_absolute():
                    path = (self.base_dir / path).resolve()
                if path.is_dir():
                    files.extend(self._files_from_directory(path))
                elif path.exists():
                    files.append(path.resolve())
        else:
            prefer_folder = clean_string(self.config.get("prefer_folder"))
            preferred_path = (self.base_dir / prefer_folder).resolve() if prefer_folder else None
            if preferred_path and preferred_path.exists() and preferred_path.is_dir():
                files = self._files_from_directory(preferred_path)
                if not self.config.get("include_auxiliary_folders", False):
                    excluded = []
                    for folder in self.config.get("auxiliary_folder_names", []):
                        aux = (self.base_dir / folder).resolve()
                        if aux.exists():
                            excluded.extend(self._files_from_directory(aux))
                    if excluded:
                        self.warning_log.add(
                            "Default discovery used the `All orders` folder and excluded auxiliary folders "
                            "`Samples` and `Replacements`. Set `include_auxiliary_folders` to true to include them."
                        )
                    return sorted(files), sorted(set(excluded))
            files = self._files_from_directory(self.base_dir)

        filtered: list[Path] = []
        excluded: list[Path] = []
        patterns = self.config.get("exclude_globs", [])
        for path in sorted(set(files)):
            relative = path.relative_to(self.base_dir) if self.base_dir in path.parents or path == self.base_dir else path
            if any(path.match(pattern) or str(relative).replace("\\", "/").endswith(pattern.replace("**/", "")) for pattern in patterns):
                excluded.append(path)
                continue
            filtered.append(path)
        return filtered, excluded

    def _files_from_directory(self, directory: Path) -> list[Path]:
        matches: list[Path] = []
        for suffix in ("*.csv", "*.xlsx", "*.xls"):
            matches.extend(directory.rglob(suffix))
        return sorted(set(path.resolve() for path in matches if path.is_file()))

    def collect_available_columns(self) -> None:
        for path in self.input_files:
            if path.suffix.lower() == ".csv":
                columns = list(pd.read_csv(path, nrows=0).columns)
            else:
                columns = list(pd.read_excel(path, nrows=0).columns)
            self.available_columns[path] = [str(column) for column in columns]

    def resolve_column_mapping(self) -> dict[str, str]:
        normalized_columns: dict[str, str] = {}
        for columns in self.available_columns.values():
            for column in columns:
                normalized_columns.setdefault(normalize_name(column), column)

        overrides = self.config.get("column_overrides", {})
        mapping: dict[str, str] = {}
        for canonical, aliases in CANONICAL_COLUMN_ALIASES.items():
            override = clean_string(overrides.get(canonical, ""))
            if override:
                mapping[canonical] = override
                continue
            for alias in aliases:
                matched = normalized_columns.get(normalize_name(alias))
                if matched:
                    mapping[canonical] = matched
                    break
        return mapping

    def validate_mapping(self) -> None:
        missing = [column for column in ESSENTIAL_COLUMNS if column not in self.detected_columns]
        if missing:
            formatted = ", ".join(missing)
            raise SystemExit(
                "The export is missing essential columns after auto-detection: "
                f"{formatted}. Add explicit `column_overrides` in the config if the file uses different labels."
            )

        optional_critical = [
            "order_amount",
            "order_refund_amount",
            "paid_time",
            "delivered_time",
            "buyer_username",
            "seller_sku",
            "sku_id",
        ]
        for column in optional_critical:
            if column not in self.detected_columns:
                self.warning_log.add(
                    f"Optional column `{column}` was not detected. Some KPIs will be marked as not available."
                )

    def load_input_files(self) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        canonical_columns = sorted(CANONICAL_COLUMN_ALIASES.keys())
        actual_columns = set(self.detected_columns.values())

        for path in self.input_files:
            available = self.available_columns[path]
            usecols = [column for column in available if column in actual_columns]
            if path.suffix.lower() == ".csv":
                frame = pd.read_csv(path, dtype=str, usecols=usecols, keep_default_na=False)
            else:
                frame = pd.read_excel(path, dtype=str, usecols=usecols)
            rename_map = {actual: canonical for canonical, actual in self.detected_columns.items() if actual in frame.columns}
            frame = frame.rename(columns=rename_map)
            for canonical in canonical_columns:
                if canonical not in frame.columns:
                    frame[canonical] = pd.NA
            frame["source_file"] = str(path.relative_to(self.base_dir))
            frames.append(frame[canonical_columns + ["source_file"]])

        if not frames:
            raise SystemExit("No readable rows were loaded from the selected input files.")
        return pd.concat(frames, ignore_index=True)

    def clean_and_normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        working = df.copy()

        for column in working.columns:
            working[column] = working[column].map(clean_string)

        header_rows = working["order_id"].eq("Order ID")
        if header_rows.any():
            self.warning_log.add(f"Removed {int(header_rows.sum())} embedded header row(s) from the data.")
            working = working.loc[~header_rows].copy()

        for column in NUMERIC_COLUMNS:
            if column in working.columns:
                working[column] = parse_numeric(working[column])

        for column in DATETIME_COLUMNS:
            if column in working.columns:
                parsed = pd.to_datetime(working[column], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
                fallback_mask = parsed.isna() & working[column].astype("string").str.len().gt(0)
                if fallback_mask.any():
                    parsed.loc[fallback_mask] = pd.to_datetime(working.loc[fallback_mask, column], errors="coerce")
                working[column] = parsed

        for column in ["order_id", "sku_id"]:
            if column in working.columns:
                working[column] = working[column].map(normalize_identifier_text).astype("string")

        for column in ["seller_sku", "virtual_bundle_seller_sku"]:
            if column in working.columns:
                working[column] = working[column].map(normalize_identifier_text).astype("string")

        for column in ["buyer_username", "buyer_nickname", "recipient"]:
            if column in working.columns:
                working[column] = working[column].astype("string").fillna("").str.strip()

        before = len(working)
        working = working.drop_duplicates().copy()
        dropped = before - len(working)
        if dropped:
            self.warning_log.add(f"Removed {dropped} exact duplicate row(s) during cleaning.")

        if "seller_sku" in working.columns and "virtual_bundle_seller_sku" in working.columns:
            working["seller_sku_resolved"] = working["seller_sku"].where(
                working["seller_sku"].astype("string").str.len() > 0,
                working["virtual_bundle_seller_sku"],
            )
        else:
            working["seller_sku_resolved"] = working.get("seller_sku", pd.Series("", index=working.index))

        customer_priority = self.config.get("kpi", {}).get(
            "customer_id_priority", ["buyer_username", "buyer_nickname", "recipient"]
        )
        existing_priority = [column for column in customer_priority if column in working.columns]
        if existing_priority:
            working["customer_id"] = (
                working[existing_priority]
                .replace("", pd.NA)
                .bfill(axis=1)
                .iloc[:, 0]
                .fillna("")
                .astype("string")
            )
        else:
            working["customer_id"] = ""
            self.warning_log.add("No customer identifier columns were available for repeat-customer KPIs.")

        status_series = working.get("order_status", pd.Series("", index=working.index)).fillna("").astype("string")
        substatus_series = working.get("order_substatus", pd.Series("", index=working.index)).fillna("").astype("string")
        cancel_text = working.get("cancel_return_type", pd.Series("", index=working.index)).fillna("").astype("string").str.lower()
        working["status_text"] = (status_series + " | " + substatus_series).str.lower()
        working["is_canceled"] = (
            working["status_text"].str.contains("cancel", na=False)
            | cancel_text.str.contains("cancel", na=False)
            | working.get("cancelled_time", pd.Series(pd.NaT, index=working.index)).notna()
        )
        working["is_refunded"] = (
            working.get("order_refund_amount", pd.Series(0.0, index=working.index)).fillna(0).gt(0)
            | cancel_text.str.contains("refund", na=False)
        )
        working["is_returned"] = (
            working.get("returned_quantity", pd.Series(0.0, index=working.index)).fillna(0).gt(0)
            | cancel_text.str.contains("return", na=False)
        )
        working["is_paid"] = working.get("paid_time", pd.Series(pd.NaT, index=working.index)).notna()
        working["is_shipped"] = (
            working.get("shipped_time", pd.Series(pd.NaT, index=working.index)).notna()
            | working["status_text"].str.contains("shipped|in transit|awaiting collection", na=False)
        )
        working["is_delivered"] = (
            working.get("delivered_time", pd.Series(pd.NaT, index=working.index)).notna()
            | working["status_text"].str.contains("delivered|completed", na=False)
        )
        working["is_valid_order_line"] = ~working["is_canceled"]
        working["status_bucket"] = np.select(
            [
                working["is_canceled"],
                working["is_returned"] & working["is_refunded"],
                working["is_returned"],
                working["is_refunded"],
                working["is_delivered"],
                working["is_shipped"],
                working["is_paid"],
            ],
            [
                "canceled",
                "returned_refunded",
                "returned",
                "refunded",
                "delivered",
                "shipped",
                "paid",
            ],
            default="placed",
        )

        if working["order_id"].eq("").any():
            blank_count = int(working["order_id"].eq("").sum())
            self.warning_log.add(f"{blank_count} row(s) have a blank order ID and will be excluded from order-level KPIs.")

        return working

    def build_order_level(self, df: pd.DataFrame) -> pd.DataFrame:
        order_lines = df.loc[df["order_id"].ne("")].copy()
        if order_lines.empty:
            raise SystemExit("No usable order IDs remained after cleaning.")

        agg_map: dict[str, Any] = {
            "customer_id": "first",
            "is_canceled": "max",
            "is_refunded": "max",
            "is_returned": "max",
            "is_paid": "max",
            "is_shipped": "max",
            "is_delivered": "max",
        }

        for column in LINE_SUM_COLUMNS:
            if column in order_lines.columns:
                agg_map[column] = "sum"
        for column in ORDER_SINGLE_VALUE_COLUMNS:
            if column in order_lines.columns:
                agg_map[column] = "max"
        for column in DATETIME_COLUMNS:
            if column in order_lines.columns:
                agg_map[column] = "min" if column in {"created_time", "paid_time"} else "max"

        grouped = order_lines.groupby("order_id", dropna=False).agg(agg_map).reset_index()
        grouped["line_count"] = order_lines.groupby("order_id").size().values
        grouped["product_count"] = order_lines.groupby("order_id")["product_name"].nunique(dropna=True).values
        grouped["is_valid_order"] = ~grouped["is_canceled"]
        grouped["status_bucket"] = np.select(
            [
                grouped["is_canceled"],
                grouped["is_returned"] & grouped["is_refunded"],
                grouped["is_returned"],
                grouped["is_refunded"],
                grouped["is_delivered"],
                grouped["is_shipped"],
                grouped["is_paid"],
            ],
            [
                "canceled",
                "returned_refunded",
                "returned",
                "refunded",
                "delivered",
                "shipped",
                "paid",
            ],
            default="placed",
        )
        return grouped

    def select_reporting_date(self, order_df: pd.DataFrame) -> pd.Series:
        preferences = [self.config.get("kpi", {}).get("report_date_basis", "paid_time")]
        preferences.extend(self.config.get("kpi", {}).get("fallback_date_basis", []))
        for column in preferences:
            if column in order_df.columns and order_df[column].notna().any():
                self.used_date_basis = column
                return order_df[column]
        self.used_date_basis = "created_time"
        self.warning_log.add("No configured reporting date basis was fully available. Falling back to `created_time`.")
        return order_df.get("created_time", pd.Series(pd.NaT, index=order_df.index))

    def build_time_breakdowns(
        self, order_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        working = order_df.copy()
        working["reporting_datetime"] = self.select_reporting_date(working)
        if working["reporting_datetime"].isna().all():
            self.warning_log.add("No usable reporting date column was available for day/week/month KPIs.")
            empty = pd.DataFrame()
            return empty, empty, empty

        working = working.loc[working["reporting_datetime"].notna()].copy()
        working["reporting_date"] = working["reporting_datetime"].dt.date
        working["week_start"] = working["reporting_datetime"].dt.to_period("W-SUN").apply(lambda value: value.start_time.date())
        working["month_start"] = working["reporting_datetime"].dt.to_period("M").apply(lambda value: value.start_time.date())

        daily = self._aggregate_period_table(working, "reporting_date")
        weekly = self._aggregate_period_table(working, "week_start")
        monthly = self._aggregate_period_table(working, "month_start")
        return daily, weekly, monthly

    def _aggregate_period_table(self, order_df: pd.DataFrame, period_column: str) -> pd.DataFrame:
        grouped = order_df.groupby(period_column).agg(
            gross_sales=("order_amount", "sum"),
            refund_amount=("order_refund_amount", "sum"),
            total_orders=("order_id", "count"),
            valid_orders=("is_valid_order", "sum"),
            canceled_orders=("is_canceled", "sum"),
            refunded_orders=("is_refunded", "sum"),
            returned_orders=("is_returned", "sum"),
            delivered_orders=("is_delivered", "sum"),
            units_sold=("quantity", "sum"),
            returned_units=("returned_quantity", "sum"),
        ).reset_index()
        grouped["net_sales"] = grouped["gross_sales"] - grouped["refund_amount"].fillna(0)
        grouped["aov_gross"] = grouped.apply(
            lambda row: row["gross_sales"] / row["valid_orders"] if row["valid_orders"] else None, axis=1
        )
        grouped["aov_net"] = grouped.apply(
            lambda row: row["net_sales"] / row["valid_orders"] if row["valid_orders"] else None, axis=1
        )
        return grouped.sort_values(period_column).reset_index(drop=True)

    def build_product_table(self, line_df: pd.DataFrame, order_df: pd.DataFrame) -> pd.DataFrame:
        required = ["product_name", "quantity"]
        if any(column not in line_df.columns for column in required):
            return pd.DataFrame()

        valid_lines = line_df.loc[line_df["is_valid_order_line"]].copy()
        if valid_lines.empty:
            return pd.DataFrame()

        valid_lines["product_key"] = (
            valid_lines[["seller_sku_resolved", "sku_id", "product_name"]]
            .replace("", pd.NA)
            .bfill(axis=1)
            .iloc[:, 0]
            .fillna("")
            .astype("string")
        )
        valid_order_count = max(int(order_df["is_valid_order"].sum()), 1)
        product_dimensions = ["product_key", "product_name", "seller_sku_resolved", "sku_id"]
        grouped = valid_lines.groupby(product_dimensions, dropna=False).agg(
            line_count=("order_id", "count"),
            order_count=("order_id", pd.Series.nunique),
            units_sold=("quantity", "sum"),
            returned_units=("returned_quantity", "sum"),
            gross_merchandise_sales=("sku_subtotal_before_discount", "sum"),
            net_merchandise_sales=("sku_subtotal_after_discount", "sum"),
            platform_discount=("sku_platform_discount", "sum"),
            seller_discount=("sku_seller_discount", "sum"),
        ).reset_index()
        per_product_order = (
            valid_lines[product_dimensions + ["order_id", "is_refunded", "is_returned", "is_delivered"]]
            .drop_duplicates()
            .groupby(product_dimensions, dropna=False)
            .agg(
                refunded_order_count=("is_refunded", "sum"),
                returned_order_count=("is_returned", "sum"),
                delivered_order_count=("is_delivered", "sum"),
            )
            .reset_index()
        )
        grouped = grouped.merge(per_product_order, on=product_dimensions, how="left")
        grouped["units_per_order"] = grouped.apply(
            lambda row: row["units_sold"] / row["order_count"] if row["order_count"] else None, axis=1
        )
        grouped["return_rate_by_units"] = grouped.apply(
            lambda row: row["returned_units"] / row["units_sold"] if row["units_sold"] else None, axis=1
        )
        grouped["share_of_valid_orders"] = grouped["order_count"] / valid_order_count
        grouped = grouped.sort_values(["net_merchandise_sales", "units_sold"], ascending=False).reset_index(drop=True)

        if (order_df["line_count"] > 1).any():
            self.warning_log.add(
                "Product-level refund dollars are not calculated because refunds are order-level and many orders are multi-line."
            )

        return grouped

    def build_kpis(
        self,
        line_df: pd.DataFrame,
        order_df: pd.DataFrame,
        daily_df: pd.DataFrame,
        weekly_df: pd.DataFrame,
        monthly_df: pd.DataFrame,
        product_df: pd.DataFrame,
    ) -> list[MetricRecord]:
        metrics: list[MetricRecord] = []

        total_orders = float(len(order_df))
        valid_orders = float(order_df["is_valid_order"].sum())
        canceled_orders = float(order_df["is_canceled"].sum())
        refunded_orders = float(order_df["is_refunded"].sum())
        returned_orders = float(order_df["is_returned"].sum())
        delivered_orders = float(order_df["is_delivered"].sum())
        paid_orders = float(order_df["is_paid"].sum())

        gross_sales = order_df.loc[order_df["is_valid_order"], "order_amount"].sum(min_count=1)
        refund_amount = order_df["order_refund_amount"].sum(min_count=1)
        net_sales = gross_sales - refund_amount if pd.notna(gross_sales) and pd.notna(refund_amount) else None
        gross_merchandise_sales = line_df.loc[line_df["is_valid_order_line"], "sku_subtotal_before_discount"].sum(min_count=1)
        net_merchandise_sales = line_df.loc[line_df["is_valid_order_line"], "sku_subtotal_after_discount"].sum(min_count=1)
        units_sold = line_df.loc[line_df["is_valid_order_line"], "quantity"].sum(min_count=1)
        returned_units = line_df["returned_quantity"].sum(min_count=1)
        unique_customers = order_df.loc[order_df["is_valid_order"] & order_df["customer_id"].ne(""), "customer_id"].nunique()

        if "customer_id" in order_df.columns and order_df["customer_id"].ne("").any():
            customer_order_counts = order_df.loc[order_df["is_valid_order"] & order_df["customer_id"].ne(""), "customer_id"].value_counts()
            repeat_customers = int((customer_order_counts > 1).sum())
            repeat_customer_rate = bool_rate(repeat_customers, len(customer_order_counts))
        else:
            repeat_customers = None
            repeat_customer_rate = None

        metrics.extend(
            [
                MetricRecord(
                    "gross_sales",
                    "Revenue KPIs",
                    gross_sales,
                    "available" if pd.notna(gross_sales) else "not available from this export",
                    "SUM(order_amount) across unique valid orders",
                    "order_amount, order_id, order_status, order_substatus, cancelled_time, cancel_return_type",
                    "Order-level total. Uses unique orders because `Order Amount` repeats on each line item.",
                ),
                MetricRecord(
                    "net_sales",
                    "Revenue KPIs",
                    net_sales,
                    "available" if net_sales is not None else "not available from this export",
                    "gross_sales - SUM(order_refund_amount)",
                    "order_amount, order_refund_amount, order_id",
                    "Represents order-total net after refunds, not merchant payout net. Fees and commissions are not present in the export.",
                ),
                MetricRecord(
                    "gross_merchandise_sales",
                    "Revenue KPIs",
                    gross_merchandise_sales,
                    "available" if pd.notna(gross_merchandise_sales) else "not available from this export",
                    "SUM(sku_subtotal_before_discount) across valid line items",
                    "sku_subtotal_before_discount, order_status, order_substatus, cancelled_time, cancel_return_type",
                    "Line-level merchandise gross before discounts and before order-level refunds.",
                ),
                MetricRecord(
                    "net_merchandise_sales_before_refunds",
                    "Revenue KPIs",
                    net_merchandise_sales,
                    "available" if pd.notna(net_merchandise_sales) else "not available from this export",
                    "SUM(sku_subtotal_after_discount) across valid line items",
                    "sku_subtotal_after_discount, order_status, order_substatus, cancelled_time, cancel_return_type",
                    "Line-level merchandise net after item discounts but before refunds.",
                ),
                MetricRecord("total_orders", "Order KPIs", int(total_orders), "available", "COUNT(DISTINCT order_id)", "order_id"),
                MetricRecord(
                    "valid_orders",
                    "Order KPIs",
                    int(valid_orders),
                    "available",
                    "COUNT(DISTINCT order_id WHERE is_canceled = false)",
                    "order_id, order_status, order_substatus, cancelled_time, cancel_return_type",
                    "Valid orders exclude canceled orders. Refunded and returned orders remain valid because they were placed and processed.",
                ),
                MetricRecord(
                    "paid_orders",
                    "Order KPIs",
                    int(paid_orders),
                    "available" if "paid_time" in self.detected_columns else "not available from this export",
                    "COUNT(DISTINCT order_id WHERE paid_time is not null)",
                    "order_id, paid_time",
                ),
                MetricRecord("canceled_orders", "Refund / Return / Cancellation KPIs", int(canceled_orders), "available", "COUNT(DISTINCT order_id WHERE status indicates cancel)", "order_id, order_status, order_substatus, cancel_return_type, cancelled_time"),
                MetricRecord(
                    "refunded_orders",
                    "Refund / Return / Cancellation KPIs",
                    int(refunded_orders),
                    "available" if "order_refund_amount" in self.detected_columns or "cancel_return_type" in self.detected_columns else "not available from this export",
                    "COUNT(DISTINCT order_id WHERE order_refund_amount > 0 OR cancel_return_type includes refund)",
                    "order_id, order_refund_amount, cancel_return_type",
                ),
                MetricRecord(
                    "returned_orders",
                    "Refund / Return / Cancellation KPIs",
                    int(returned_orders),
                    "available" if "returned_quantity" in self.detected_columns or "cancel_return_type" in self.detected_columns else "not available from this export",
                    "COUNT(DISTINCT order_id WHERE returned_quantity > 0 OR cancel_return_type includes return)",
                    "order_id, returned_quantity, cancel_return_type",
                ),
                MetricRecord("delivered_orders", "Order KPIs", int(delivered_orders), "available", "COUNT(DISTINCT order_id WHERE delivered_time exists OR status indicates delivered/completed)", "order_id, delivered_time, order_status, order_substatus"),
                MetricRecord("refund_rate", "Refund / Return / Cancellation KPIs", bool_rate(refunded_orders, valid_orders), "available" if valid_orders else "not available from this export", "refunded_orders / valid_orders", "order_id, order_refund_amount, cancel_return_type"),
                MetricRecord("return_rate", "Refund / Return / Cancellation KPIs", bool_rate(returned_orders, valid_orders), "available" if valid_orders else "not available from this export", "returned_orders / valid_orders", "order_id, returned_quantity, cancel_return_type"),
                MetricRecord("cancellation_rate", "Refund / Return / Cancellation KPIs", bool_rate(canceled_orders, total_orders), "available" if total_orders else "not available from this export", "canceled_orders / total_orders", "order_id, order_status, order_substatus, cancel_return_type, cancelled_time"),
                MetricRecord("refund_amount", "Refund / Return / Cancellation KPIs", refund_amount, "available" if "order_refund_amount" in self.detected_columns else "not available from this export", "SUM(order_refund_amount) across unique orders", "order_refund_amount, order_id", "Order-level total. Does not allocate refunds down to product level."),
                MetricRecord("aov_gross", "AOV and Item KPIs", gross_sales / valid_orders if valid_orders else None, "available" if valid_orders else "not available from this export", "gross_sales / valid_orders", "order_amount, order_id"),
                MetricRecord("aov_net", "AOV and Item KPIs", net_sales / valid_orders if valid_orders and net_sales is not None else None, "available" if valid_orders and net_sales is not None else "not available from this export", "net_sales / valid_orders", "order_amount, order_refund_amount, order_id"),
                MetricRecord("units_sold", "AOV and Item KPIs", units_sold, "available" if pd.notna(units_sold) else "not available from this export", "SUM(quantity) across valid line items", "quantity, order_status, order_substatus, cancelled_time, cancel_return_type"),
                MetricRecord("returned_units", "Refund / Return / Cancellation KPIs", returned_units, "available" if "returned_quantity" in self.detected_columns else "not available from this export", "SUM(returned_quantity)", "returned_quantity"),
                MetricRecord("units_per_order", "AOV and Item KPIs", units_sold / valid_orders if valid_orders and pd.notna(units_sold) else None, "available" if valid_orders and pd.notna(units_sold) else "not available from this export", "units_sold / valid_orders", "quantity, order_id"),
                MetricRecord("unique_customers", "Customer KPIs", int(unique_customers) if unique_customers is not None else None, "available" if unique_customers is not None else "not available from this export", "COUNT(DISTINCT customer_id) across valid orders", "buyer_username or buyer_nickname or recipient"),
                MetricRecord("repeat_customers", "Customer KPIs", repeat_customers, "available" if repeat_customers is not None else "not available from this export", "COUNT(customer_id with more than one valid order)", "buyer_username or buyer_nickname or recipient, order_id"),
                MetricRecord("repeat_customer_rate", "Customer KPIs", repeat_customer_rate, "available" if repeat_customer_rate is not None else "not available from this export", "repeat_customers / unique_customers", "buyer_username or buyer_nickname or recipient, order_id", "Uses the first populated customer identifier based on config priority."),
            ]
        )

        metrics.extend(self.build_status_mix_metrics(order_df))
        metrics.extend(self.build_time_metrics(daily_df, weekly_df, monthly_df))

        if not product_df.empty:
            top_product = product_df.iloc[0]
            metrics.append(
                MetricRecord(
                    "top_product_by_net_merchandise_sales",
                    "Product / SKU KPIs",
                    top_product["product_name"],
                    "available",
                    "Top row from product_kpis ordered by net_merchandise_sales DESC",
                    "product_name, seller_sku_resolved, sku_id, sku_subtotal_after_discount",
                )
            )
            metrics.append(
                MetricRecord(
                    "top_product_net_merchandise_sales",
                    "Product / SKU KPIs",
                    top_product["net_merchandise_sales"],
                    "available",
                    "MAX(product net_merchandise_sales)",
                    "product_name, sku_subtotal_after_discount",
                )
            )
        else:
            metrics.append(
                MetricRecord(
                    "top_product_by_net_merchandise_sales",
                    "Product / SKU KPIs",
                    None,
                    "not available from this export",
                    "Not calculated",
                    "product_name, seller_sku or sku_id",
                    "Product-level columns were not available.",
                )
            )

        return metrics

    def build_status_mix_metrics(self, order_df: pd.DataFrame) -> list[MetricRecord]:
        metrics: list[MetricRecord] = []
        counts = order_df["status_bucket"].value_counts(dropna=False).to_dict()
        for status_name in [
            "placed",
            "paid",
            "shipped",
            "delivered",
            "refunded",
            "returned",
            "returned_refunded",
            "canceled",
        ]:
            count = int(counts.get(status_name, 0))
            metrics.append(
                MetricRecord(
                    f"status_mix_{status_name}",
                    "Status Breakdowns",
                    count,
                    "available",
                    f"COUNT(DISTINCT order_id WHERE status_bucket = '{status_name}')",
                    "order_status, order_substatus, cancel_return_type, delivered_time, shipped_time, paid_time, cancelled_time",
                )
            )
        return metrics

    def build_time_metrics(
        self, daily_df: pd.DataFrame, weekly_df: pd.DataFrame, monthly_df: pd.DataFrame
    ) -> list[MetricRecord]:
        metrics: list[MetricRecord] = []
        time_frames = [("daily", daily_df), ("weekly", weekly_df), ("monthly", monthly_df)]
        for label, frame in time_frames:
            if frame.empty:
                metrics.append(
                    MetricRecord(
                        f"{label}_gross_sales_average",
                        "Time-based KPIs",
                        None,
                        "not available from this export",
                        "Not calculated",
                        self.used_date_basis or "configured report date basis",
                        f"No usable {label} breakdown was available.",
                    )
                )
                continue
            metrics.append(
                MetricRecord(
                    f"{label}_gross_sales_average",
                    "Time-based KPIs",
                    frame["gross_sales"].mean(),
                    "available",
                    f"AVERAGE({label}.gross_sales)",
                    "order_amount, reporting date basis",
                    f"Reporting date basis used: {self.used_date_basis}",
                )
            )
            metrics.append(
                MetricRecord(
                    f"{label}_gross_sales_best",
                    "Time-based KPIs",
                    frame["gross_sales"].max(),
                    "available",
                    f"MAX({label}.gross_sales)",
                    "order_amount, reporting date basis",
                )
            )
            metrics.append(
                MetricRecord(
                    f"{label}_period_count",
                    "Time-based KPIs",
                    len(frame),
                    "available",
                    f"COUNT({label} periods with at least one order)",
                    "reporting date basis",
                )
            )
        return metrics

    def write_outputs(
        self,
        output_dir: Path,
        metrics: list[MetricRecord],
        daily_df: pd.DataFrame,
        product_df: pd.DataFrame,
    ) -> None:
        kpi_full = pd.DataFrame([metric.to_dict() for metric in metrics])
        kpi_summary = kpi_full.loc[
            kpi_full["metric"].isin(
                [
                    "gross_sales",
                    "net_sales",
                    "total_orders",
                    "valid_orders",
                    "paid_orders",
                    "canceled_orders",
                    "refunded_orders",
                    "returned_orders",
                    "refund_rate",
                    "return_rate",
                    "cancellation_rate",
                    "aov_gross",
                    "aov_net",
                    "units_sold",
                    "units_per_order",
                    "repeat_customer_rate",
                ]
            )
        ].copy()

        kpi_summary.to_csv(output_dir / "kpi_summary.csv", index=False)
        kpi_full.to_csv(output_dir / "kpi_full.csv", index=False)

        if daily_df.empty:
            pd.DataFrame(columns=["reporting_date"]).to_csv(output_dir / "daily_breakdown.csv", index=False)
        else:
            daily_df.to_csv(output_dir / "daily_breakdown.csv", index=False)

        product_path = output_dir / "product_kpis.csv"
        if product_df.empty:
            if product_path.exists():
                product_path.unlink()
        else:
            product_df.to_csv(product_path, index=False)

    def write_report(
        self,
        output_dir: Path,
        metrics: list[MetricRecord],
        daily_df: pd.DataFrame,
        weekly_df: pd.DataFrame,
        monthly_df: pd.DataFrame,
        product_df: pd.DataFrame,
        line_df: pd.DataFrame,
        order_df: pd.DataFrame,
    ) -> None:
        metric_map = {metric.metric: metric for metric in metrics}
        lines: list[str] = []
        lines.append("# TikTok Shop KPI Report")
        lines.append("")
        lines.append("## Inputs Used")
        for path in self.input_files:
            lines.append(f"- `{path.relative_to(self.base_dir)}`")
        if self.excluded_files:
            lines.append("")
            lines.append("## Excluded By Discovery")
            for path in self.excluded_files:
                lines.append(f"- `{path.relative_to(self.base_dir)}`")

        lines.append("")
        lines.append("## Summary")
        for key in [
            "gross_sales",
            "net_sales",
            "total_orders",
            "valid_orders",
            "canceled_orders",
            "refunded_orders",
            "returned_orders",
            "refund_rate",
            "return_rate",
            "cancellation_rate",
            "aov_gross",
            "units_sold",
            "repeat_customer_rate",
        ]:
            metric = metric_map[key]
            lines.append(f"- **{metric.metric}**: {metric.to_dict()['formatted_value']}")

        lines.append("")
        lines.append("## Detected Column Mapping")
        lines.append("| Canonical field | Source column |")
        lines.append("| --- | --- |")
        for canonical, source in sorted(self.detected_columns.items()):
            lines.append(f"| `{canonical}` | `{source}` |")

        lines.append("")
        lines.append("## Coverage and Assumptions")
        lines.append(f"- Reporting date basis used for time KPIs: `{self.used_date_basis or 'not available'}`")
        lines.append("- `gross_sales` uses unique order-level `Order Amount` values because the export repeats them on every line item.")
        lines.append("- `net_sales` is gross sales minus order refund amount. It is not merchant payout net because fee and commission fields are not present.")
        lines.append("- `valid_orders` excludes canceled orders but keeps refunded and returned orders as processed orders.")
        lines.append("- Product-level refund dollars are not allocated when refunds occur on multi-line orders.")

        lines.append("")
        lines.append("## Dataset Quality Checks")
        lines.append(f"- Raw line rows after cleaning: `{len(line_df):,}`")
        lines.append(f"- Unique orders: `{len(order_df):,}`")
        lines.append(f"- Multi-line orders: `{int((order_df['line_count'] > 1).sum()):,}`")
        lines.append(f"- Unique customers with chosen identifier: `{int(order_df.loc[order_df['customer_id'].ne(''), 'customer_id'].nunique()):,}`")

        if not daily_df.empty:
            top_day = daily_df.sort_values("gross_sales", ascending=False).iloc[0]
            lines.append("")
            lines.append("## Daily Trend Highlight")
            lines.append(f"- Best day by gross sales: `{top_day['reporting_date']}` with `{format_scalar(top_day['gross_sales'])}`")

        if not weekly_df.empty:
            top_week = weekly_df.sort_values("gross_sales", ascending=False).iloc[0]
            lines.append("")
            lines.append("## Weekly Trend Highlight")
            lines.append(f"- Best week start: `{top_week['week_start']}` with `{format_scalar(top_week['gross_sales'])}`")

        if not monthly_df.empty:
            top_month = monthly_df.sort_values("gross_sales", ascending=False).iloc[0]
            lines.append("")
            lines.append("## Monthly Trend Highlight")
            lines.append(f"- Best month start: `{top_month['month_start']}` with `{format_scalar(top_month['gross_sales'])}`")

        if not product_df.empty:
            lines.append("")
            lines.append("## Top Products")
            lines.append("| Product | Orders | Units | Net Merchandise Sales |")
            lines.append("| --- | ---: | ---: | ---: |")
            for _, row in product_df.head(10).iterrows():
                lines.append(
                    f"| {row['product_name']} | {int(row['order_count'])} | {format_scalar(row['units_sold'])} | {format_scalar(row['net_merchandise_sales'])} |"
                )

        lines.append("")
        lines.append("## Exceptions and Warnings")
        if self.warning_log.items:
            for warning in self.warning_log.items:
                lines.append(f"- {warning}")
        else:
            lines.append("- No validation warnings were raised.")

        lines.append("")
        lines.append("## Output Files")
        lines.append("- `kpi_summary.csv`")
        lines.append("- `kpi_full.csv`")
        lines.append("- `daily_breakdown.csv`")
        if not product_df.empty:
            lines.append("- `product_kpis.csv`")
        else:
            lines.append("- `product_kpis.csv` was not created because product KPI inputs were not sufficient.")
        lines.append("- `report.md`")

        (output_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


def merge_config(config_path: Path | None) -> dict[str, Any]:
    config = json.loads(json.dumps(DEFAULT_CONFIG))
    if not config_path:
        return config
    loaded = json.loads(config_path.read_text(encoding="utf-8"))
    return deep_merge(config, loaded)


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze TikTok Shop export files and generate KPI outputs.")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to a JSON config file. Defaults to built-in settings if omitted.",
    )
    parser.add_argument(
        "--input",
        nargs="*",
        default=None,
        help="Optional explicit file or directory paths. Overrides auto-discovery when provided.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory override.",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    base_dir = Path.cwd()
    config_path = args.config.resolve() if args.config else None
    config = merge_config(config_path)
    analyzer = TikTokKPIAnalyzer(config=config, base_dir=base_dir)
    analyzer.run(explicit_inputs=args.input, output_dir=args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
