from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = BASE_DIR / "analysis_output"
ORDER_SOURCE_FOLDERS = {
    "All orders": "Sales",
    "Samples": "Samples",
    "Replacements": "Replacements",
}
COGS_MAP = {
    "Birria Bomb 2-Pack": {"list_price": 19.99, "cogs": 3.10},
    "Pozole Bomb 2-Pack": {"list_price": 19.99, "cogs": 3.05},
    "Tinga Bomb 2-Pack": {"list_price": 19.99, "cogs": 3.15},
    "Pozole Verde Bomb 2-Pack": {"list_price": 19.99, "cogs": 3.75},
    "Brine Bomb": {"list_price": 19.99, "cogs": 4.20},
    "Variety Pack": {"list_price": 49.99, "cogs": 13.35},
}

st.set_page_config(
    page_title="EZ Bombs TikTok Shop Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
      :root {
        --bg: #f4efe8;
        --panel: rgba(255, 253, 250, 0.96);
        --panel-2: #f8f3ec;
        --ink: #1f1813;
        --muted: #6d5b4d;
        --line: rgba(31, 24, 19, 0.10);
        --accent: #b44f1f;
        --accent-soft: rgba(180, 79, 31, 0.10);
        --green: #2f6c4f;
        --red: #a13e31;
      }
      .stApp {
        background:
          linear-gradient(180deg, #faf6ef 0%, #f1e9dd 100%);
        color: var(--ink);
      }
      .stApp, .stApp p, .stApp span, .stApp label, .stApp div {
        color: var(--ink);
      }
      .block-container {
        max-width: 1440px;
        padding-top: 1.4rem;
        padding-bottom: 2rem;
      }
      .top-shell, .panel-shell, .metric-card, .mini-card {
        background: var(--panel);
        border: 1px solid var(--line);
        box-shadow: 0 12px 28px rgba(31, 24, 19, 0.05);
      }
      .top-shell {
        border-radius: 22px;
        padding: 1.25rem 1.35rem;
        margin-bottom: 1rem;
      }
      .page-kicker {
        font-size: 0.76rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--muted);
        margin-bottom: 0.35rem;
      }
      .page-title {
        font-size: 2.2rem;
        line-height: 1.0;
        font-weight: 800;
        color: var(--ink);
        margin-bottom: 0.4rem;
      }
      .page-subtitle {
        color: var(--muted);
        max-width: 60rem;
      }
      .filter-shell {
        background: var(--panel-2);
        border: 1px solid var(--line);
        border-radius: 18px;
        padding: 1rem 1rem 0.4rem;
        margin-bottom: 1rem;
      }
      .metric-card {
        border-radius: 18px;
        padding: 1rem 1.05rem;
        min-height: 136px;
      }
      .metric-label {
        font-size: 0.74rem;
        letter-spacing: 0.10em;
        text-transform: uppercase;
        color: var(--muted);
        margin-bottom: 0.45rem;
      }
      .metric-value {
        font-size: 2.05rem;
        line-height: 1.0;
        font-weight: 800;
        color: var(--ink);
      }
      .metric-value.negative {
        color: var(--red);
      }
      .metric-note {
        margin-top: 0.45rem;
        font-size: 0.84rem;
        color: var(--muted);
      }
      .mini-card {
        border-radius: 16px;
        padding: 0.95rem 1rem;
        min-height: 108px;
      }
      .mini-value {
        font-size: 1.45rem;
        line-height: 1.0;
        font-weight: 760;
        color: var(--accent);
      }
      .panel-shell {
        border-radius: 22px;
        padding: 1rem 1.05rem 1.15rem;
        height: 100%;
      }
      .scope-chip {
        display: inline-block;
        margin-bottom: 0.55rem;
        padding: 0.26rem 0.58rem;
        border-radius: 999px;
        background: var(--accent-soft);
        color: var(--accent);
        font-size: 0.72rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }
      .section-note {
        color: var(--muted);
        font-size: 0.88rem;
        margin-bottom: 0.8rem;
      }
      [data-testid="stSelectbox"] label,
      [data-testid="stDateInput"] label,
      [data-testid="stTextInput"] label,
      [data-testid="stRadio"] label,
      [data-testid="stTabs"] button,
      [data-testid="stMarkdownContainer"],
      [data-testid="stMetricLabel"],
      [data-testid="stDataFrame"] div {
        color: var(--ink) !important;
      }
      [data-baseweb="select"] > div,
      [data-baseweb="input"] > div,
      .stDateInput input,
      .stTextInput input {
        background: #fffdfa !important;
        color: var(--ink) !important;
        border-color: var(--line) !important;
      }
      [data-baseweb="tag"] {
        background: rgba(180, 79, 31, 0.12) !important;
        color: var(--accent) !important;
      }
      .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem;
      }
      .stTabs [data-baseweb="tab"] {
        background: rgba(255,255,255,0.7);
        border: 1px solid var(--line);
        border-radius: 999px;
        padding-inline: 0.9rem;
      }
      .stTabs [aria-selected="true"] {
        background: var(--accent-soft) !important;
        border-color: rgba(180,79,31,0.22) !important;
      }
      h1, h2, h3 {
        color: var(--ink);
      }
      [data-testid="stMetricValue"] {
        color: var(--ink);
      }
    </style>
    """,
    unsafe_allow_html=True,
)


def format_value(metric_name: str, value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"

    percent_metrics = {"repeat_customer_rate", "selected_repeat_customer_rate"}
    integer_metrics = {
        "paid_orders",
        "operational_units",
        "sales_units",
        "sample_units",
        "replacement_units",
        "selected_unique_customers",
        "selected_repeat_customers",
    }
    if metric_name in percent_metrics:
        return f"{float(value):.1%}"
    if metric_name in integer_metrics:
        return f"{int(round(float(value))):,}"
    return f"${float(value):,.2f}"


def format_currency_series(series: pd.Series) -> pd.Series:
    return series.apply(lambda value: "" if pd.isna(value) else f"${float(value):,.2f}")


def format_integer_series(series: pd.Series) -> pd.Series:
    return series.apply(lambda value: "" if pd.isna(value) else f"{int(round(float(value))):,}")


def format_percent_series(series: pd.Series) -> pd.Series:
    return series.apply(lambda value: "" if pd.isna(value) else f"{float(value):.1%}")


def detect_product_components(product_name: str) -> tuple[list[tuple[str, int]], str]:
    name = (product_name or "").lower()
    if not name:
        return [], "No product name"

    if "variety pack" in name:
        return [("Variety Pack", 1)], "Mapped as fixed Variety Pack COGS"

    flavors: list[str] = []
    if "pozole verde" in name:
        flavors.append("Pozole Verde Bomb 2-Pack")
    if "birria" in name:
        flavors.append("Birria Bomb 2-Pack")
    if "tinga" in name:
        flavors.append("Tinga Bomb 2-Pack")
    if "brine" in name:
        flavors.append("Brine Bomb")
    pozole_plain_match = "pozole" in name and "pozole verde" not in name
    if pozole_plain_match:
        flavors.append("Pozole Bomb 2-Pack")

    # preserve order and uniqueness
    seen: set[str] = set()
    normalized_flavors = []
    for flavor in flavors:
        if flavor not in seen:
            normalized_flavors.append(flavor)
            seen.add(flavor)

    if not normalized_flavors:
        return [], "No COGS mapping matched product title"

    if "bundle" in name:
        if len(normalized_flavors) == 1:
            return [(normalized_flavors[0], 2)], "Single-flavor bundle assumed to contain 2 of the same 2-pack"
        return [(flavor, 1) for flavor in normalized_flavors], "Mixed bundle assumed to contain 1 of each named 2-pack"

    return [(normalized_flavors[0], 1)], "Standard listing mapped to one 2-pack"


def canonical_item_name(product_name: str) -> str:
    name = (product_name or "").lower()
    if "variety pack" in name:
        return "Variety Pack"
    if "pozole verde" in name:
        if "bundle" in name:
            if "birria" in name:
                return "Pozole Verde + Birria Bundle"
            if "tinga" in name:
                return "Pozole Verde + Tinga Bundle"
            if "pozole" in name:
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

        for component_name, component_qty_per_listing in components:
            component_units = units_sold * component_qty_per_listing
            component_cogs = COGS_MAP[component_name]["cogs"] * component_units
            estimated_cogs += component_cogs
            mapped_component_units += component_units
            component_rows.append(
                {
                    "Product": component_name,
                    "Component Units Sold": component_units,
                    "Unit COGS": COGS_MAP[component_name]["cogs"],
                    "Estimated COGS": component_cogs,
                }
            )

        listing_rows.append(
            {
                "Listing": product_name,
                "Units Sold": units_sold,
                "Mapped Component Units": mapped_component_units,
                "Net Merchandise Sales": net_sales,
                "Estimated COGS": estimated_cogs if components else pd.NA,
                "Estimated Gross Profit": (net_sales - estimated_cogs) if components else pd.NA,
                "COGS Assumption": assumption,
            }
        )

    listing_df = pd.DataFrame(listing_rows).sort_values("Net Merchandise Sales", ascending=False)
    component_df = pd.DataFrame(component_rows)
    if not component_df.empty:
        component_df = (
            component_df.groupby("Product", as_index=False)
            .agg(
                **{
                    "Component Units Sold": ("Component Units Sold", "sum"),
                    "Unit COGS": ("Unit COGS", "max"),
                    "Estimated COGS": ("Estimated COGS", "sum"),
                }
            )
            .sort_values("Estimated COGS", ascending=False)
        )
    return listing_df, component_df


def build_filtered_product_view(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    working = raw_df.copy()
    working["canonical_item_name"] = working["Product Name"].map(canonical_item_name)
    grouped = (
        working.groupby(["canonical_item_name", "Seller SKU"], dropna=False, as_index=False)
        .agg(
            order_count=("Order ID", pd.Series.nunique),
            units_sold=("Quantity", "sum"),
            returned_units=("Sku Quantity of return", "sum"),
            gross_merchandise_sales=("SKU Subtotal Before Discount", "sum"),
            seller_discount=("SKU Seller Discount", "sum"),
            platform_discount=("SKU Platform Discount", "sum"),
            net_merchandise_sales=("SKU Subtotal After Discount", "sum"),
        )
        .rename(columns={"canonical_item_name": "product_name", "Seller SKU": "seller_sku_resolved"})
        .sort_values("net_merchandise_sales", ascending=False)
        .reset_index(drop=True)
    )
    grouped["return_rate_by_units"] = grouped.apply(
        lambda row: row["returned_units"] / row["units_sold"] if row["units_sold"] else pd.NA,
        axis=1,
    )
    units_by_source = (
        working.groupby(["canonical_item_name", "Seller SKU", "source_type"], dropna=False, as_index=False)
        .agg(source_units=("Quantity", "sum"))
        .pivot(index=["canonical_item_name", "Seller SKU"], columns="source_type", values="source_units")
        .fillna(0)
        .reset_index()
        .rename(
            columns={
                "canonical_item_name": "product_name",
                "Seller SKU": "seller_sku_resolved",
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
    return grouped


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


def detect_output_dirs() -> list[Path]:
    return sorted(
        [path for path in BASE_DIR.iterdir() if path.is_dir() and path.name.startswith("analysis_output")],
        key=lambda path: path.name,
    )


def detect_order_source_dirs() -> dict[str, Path]:
    detected: dict[str, Path] = {}
    for folder_name in ORDER_SOURCE_FOLDERS:
        candidate = BASE_DIR / folder_name
        if candidate.exists():
            detected[folder_name] = candidate
    return detected


@st.cache_data(show_spinner=False)
def load_dashboard_data(output_dir: str) -> dict[str, pd.DataFrame | str | None]:
    directory = Path(output_dir)
    result: dict[str, pd.DataFrame | str | None] = {
        "kpi_full": None,
        "daily": None,
        "product": None,
        "report_md": None,
    }

    for key, filename in {
        "kpi_full": "kpi_full.csv",
        "daily": "daily_breakdown.csv",
        "product": "product_kpis.csv",
    }.items():
        file_path = directory / filename
        if file_path.exists():
            result[key] = pd.read_csv(file_path)

    report_path = directory / "report.md"
    if report_path.exists():
        result["report_md"] = report_path.read_text(encoding="utf-8")

    daily_df = result["daily"]
    if isinstance(daily_df, pd.DataFrame) and not daily_df.empty and "reporting_date" in daily_df.columns:
        daily_df["reporting_date"] = pd.to_datetime(daily_df["reporting_date"], errors="coerce")
        for col in [c for c in daily_df.columns if c != "reporting_date"]:
            daily_df[col] = pd.to_numeric(daily_df[col], errors="coerce")
        result["daily"] = daily_df.dropna(subset=["reporting_date"]).sort_values("reporting_date")

    product_df = result["product"]
    if isinstance(product_df, pd.DataFrame) and not product_df.empty:
        for col in [c for c in product_df.columns if c not in {"product_key", "product_name", "seller_sku_resolved", "sku_id"}]:
            product_df[col] = pd.to_numeric(product_df[col], errors="coerce")
        result["product"] = product_df

    return result


@st.cache_data(show_spinner="Loading finance-style daily view from raw order exports...")
def load_paid_time_finance(order_sources: tuple[tuple[str, str], ...]) -> pd.DataFrame:
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
    df = df.dropna(subset=["Paid Time"]).copy()
    if df.empty:
        return pd.DataFrame()

    df["reporting_date"] = df["Paid Time"].dt.normalize()

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
    line_daily["product_sales_after_seller_discount"] = (
        line_daily["gross_product_sales"] - line_daily["seller_discount"]
    )

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
    daily["approx_product_sales_after_export_refunds"] = (
        daily["product_sales_after_seller_discount"] - daily["order_refund_amount"]
    )
    return daily.sort_values("reporting_date").reset_index(drop=True)


@st.cache_data(show_spinner="Loading raw product and status data from order exports...")
def load_paid_time_operational(order_sources: tuple[tuple[str, str], ...]) -> pd.DataFrame:
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
        "Product Name",
        "Buyer Username",
        "Buyer Nickname",
        "Recipient",
        "Quantity",
        "Sku Quantity of return",
        "SKU Subtotal Before Discount",
        "SKU Seller Discount",
        "SKU Platform Discount",
        "SKU Subtotal After Discount",
        "Order Refund Amount",
    ]
    frames: list[pd.DataFrame] = []
    for source_folder, source_label in order_sources:
        directory = Path(source_folder)
        for path in sorted(directory.glob("*.csv")):
            frame = pd.read_csv(path, dtype=str, keep_default_na=False, usecols=lambda col: col in usecols)
            if not frame.empty:
                frame["source_type"] = source_label
                frames.append(frame)
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = df[df["Order ID"].astype(str).str.strip() != "Order ID"].copy()
    for col in df.columns:
        df[col] = df[col].astype(str).str.replace("\t", "", regex=False).str.strip()

    for col in ["Buyer Username", "Buyer Nickname", "Recipient"]:
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
    ]:
        df[col] = pd.to_numeric(df[col].replace({"": None}), errors="coerce")

    for col in ["Paid Time", "Cancelled Time", "Delivered Time", "Shipped Time"]:
        df[col] = pd.to_datetime(df[col], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")

    df = df.dropna(subset=["Paid Time"]).copy()
    if df.empty:
        return pd.DataFrame()

    df["reporting_date"] = df["Paid Time"].dt.normalize()
    status_text = (df["Order Status"].fillna("") + " | " + df["Order Substatus"].fillna("")).str.lower()
    cancel_type = df["Cancelation/Return Type"].fillna("").str.lower()
    df["is_canceled"] = status_text.str.contains("cancel", na=False) | df["Cancelled Time"].notna() | cancel_type.str.contains("cancel", na=False)
    df["is_refunded"] = df["Order Refund Amount"].fillna(0).gt(0) | cancel_type.str.contains("refund", na=False)
    df["is_returned"] = df["Sku Quantity of return"].fillna(0).gt(0) | cancel_type.str.contains("return", na=False)
    df["is_delivered"] = df["Delivered Time"].notna() | status_text.str.contains("delivered|completed", na=False)
    df["is_shipped"] = df["Shipped Time"].notna() | status_text.str.contains("shipped|in transit|awaiting collection", na=False)
    customer_priority = [col for col in ["Buyer Username", "Buyer Nickname", "Recipient"] if col in df.columns]
    if customer_priority:
        df["customer_id"] = (
            df[customer_priority]
            .replace("", pd.NA)
            .bfill(axis=1)
            .iloc[:, 0]
            .fillna("")
            .astype("string")
        )
    else:
        df["customer_id"] = ""
    return df.reset_index(drop=True)


def get_kpi_lookup(kpi_df: pd.DataFrame | None) -> dict[str, pd.Series]:
    if kpi_df is None or kpi_df.empty:
        return {}
    return {row["metric"]: row for _, row in kpi_df.iterrows()}


def render_page_header() -> None:
    st.markdown(
        """
        <div class="top-shell">
          <div class="page-kicker">EZ Bombs TikTok Shop</div>
          <div class="page-title">Finance and order operating view</div>
          <div class="page-subtitle">
            Use the controls below to change the date window, output folder, and counted order sources.
            Sales, Samples, and Replacements can now be included separately so units sold stay explicit.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_filters(
    output_options: list[str],
    default_index: int,
    finance_daily_df: pd.DataFrame | None,
    available_sources: list[str],
) -> tuple[str, pd.Timestamp, pd.Timestamp, list[str]]:
    st.markdown('<div class="filter-shell">', unsafe_allow_html=True)
    c1, c2, c3, c4, c5 = st.columns([1.15, 0.95, 1.2, 0.8, 0.8])

    with c1:
        selected_dir = st.selectbox("Output Folder", options=output_options, index=default_index)
    with c2:
        preset = st.selectbox(
            "Date Window",
            options=["All data", "Last 30 days", "Last 90 days", "Year to date", "Custom range"],
            index=0,
        )

    min_date = finance_daily_df["reporting_date"].min().date()
    max_date = finance_daily_df["reporting_date"].max().date()
    if preset == "Last 30 days":
        start_default = max(min_date, (pd.Timestamp(max_date) - pd.Timedelta(days=29)).date())
        end_default = max_date
    elif preset == "Last 90 days":
        start_default = max(min_date, (pd.Timestamp(max_date) - pd.Timedelta(days=89)).date())
        end_default = max_date
    elif preset == "Year to date":
        start_default = max(min_date, pd.Timestamp(max_date).replace(month=1, day=1).date())
        end_default = max_date
    else:
        start_default = min_date
        end_default = max_date

    with c3:
        selected_sources = st.multiselect(
            "Count These Sources",
            options=available_sources,
            default=["Sales"] if "Sales" in available_sources else available_sources,
            help="Sales comes from `All orders`. Samples and Replacements are optional so units sold stay intentional.",
        )
    with c4:
        start_date = st.date_input("Start Date", value=start_default, min_value=min_date, max_value=max_date)
    with c5:
        end_date = st.date_input("End Date", value=end_default, min_value=min_date, max_value=max_date)

    if start_date > end_date:
        start_date, end_date = end_date, start_date
    if not selected_sources:
        selected_sources = ["Sales"] if "Sales" in available_sources else available_sources

    st.markdown("</div>", unsafe_allow_html=True)
    return selected_dir, pd.Timestamp(start_date), pd.Timestamp(end_date), selected_sources


def filter_daily(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, selected_sources: list[str] | None = None) -> pd.DataFrame:
    filtered = df.loc[df["reporting_date"].between(start, end)].copy()
    if selected_sources and "source_type" in filtered.columns:
        filtered = filtered.loc[filtered["source_type"].isin(selected_sources)].copy()
    return filtered


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


def valid_customer_orders(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    working = raw_df.loc[~raw_df["is_canceled"].fillna(False)].copy()
    if "customer_id" not in working.columns:
        return pd.DataFrame()
    working = working.loc[working["customer_id"].astype("string").str.strip().ne("")]
    if working.empty:
        return pd.DataFrame()
    order_level = (
        working.sort_values("reporting_date")
        .groupby("Order ID", as_index=False)
        .agg(
            customer_id=("customer_id", "first"),
            reporting_date=("reporting_date", "min"),
            source_type=("source_type", "first"),
        )
    )
    order_level["order_month"] = order_level["reporting_date"].dt.to_period("M").dt.to_timestamp()
    return order_level


def build_customer_metrics(raw_df: pd.DataFrame) -> dict[str, Any]:
    order_level = valid_customer_orders(raw_df)
    if order_level.empty:
        return {
            "selected_unique_customers": None,
            "selected_repeat_customers": None,
            "selected_repeat_customer_rate": None,
            "customer_id_basis": "Buyer Username -> Buyer Nickname -> Recipient",
        }
    customer_counts = order_level["customer_id"].value_counts()
    unique_customers = int(customer_counts.shape[0])
    repeat_customers = int((customer_counts > 1).sum())
    repeat_rate = (repeat_customers / unique_customers) if unique_customers else None
    return {
        "selected_unique_customers": unique_customers,
        "selected_repeat_customers": repeat_customers,
        "selected_repeat_customer_rate": repeat_rate,
        "customer_id_basis": "Buyer Username -> Buyer Nickname -> Recipient",
    }


def build_cohort_retention(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    order_level = valid_customer_orders(raw_df)
    if order_level.empty:
        return pd.DataFrame(), pd.DataFrame()

    first_orders = (
        order_level.groupby("customer_id", as_index=False)
        .agg(first_order_month=("order_month", "min"))
    )
    cohort_df = order_level.merge(first_orders, on="customer_id", how="left")
    cohort_df["months_since_first"] = (
        (cohort_df["order_month"].dt.year - cohort_df["first_order_month"].dt.year) * 12
        + (cohort_df["order_month"].dt.month - cohort_df["first_order_month"].dt.month)
    )
    active = (
        cohort_df.groupby(["first_order_month", "months_since_first"], as_index=False)
        .agg(active_customers=("customer_id", "nunique"))
    )
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
    visible = cohort_summary.loc[
        cohort_summary["first_order_month"].between(start_month, end_month)
    ].copy()
    if visible.empty:
        return pd.DataFrame(), pd.DataFrame()
    heatmap = (
        visible.assign(cohort_label=visible["first_order_month"].dt.strftime("%Y-%m"))
        .pivot(index="cohort_label", columns="months_since_first", values="retention_rate")
        .sort_index(ascending=False)
        .fillna(0)
    )
    return heatmap, visible


def summarize_finance_period(filtered_finance: pd.DataFrame, all_time_lookup: dict[str, pd.Series]) -> dict[str, Any]:
    if filtered_finance.empty:
        return {
            "gross_product_sales": None,
            "net_product_sales": None,
            "product_sales_after_seller_discount": None,
            "product_sales_after_all_discounts": None,
            "collected_order_amount": None,
            "seller_discount": None,
            "platform_discount": None,
            "order_refund_amount": None,
            "paid_orders": None,
            "repeat_customer_rate": all_time_lookup.get("repeat_customer_rate", {}).get("value"),
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
    result = values.to_dict()
    if (
        result["gross_product_sales"] is not None
        and result["seller_discount"] is not None
        and result["order_refund_amount"] is not None
    ):
        result["net_product_sales"] = (
            result["gross_product_sales"] - result["seller_discount"] - result["order_refund_amount"]
        )
    else:
        result["net_product_sales"] = None
    result["repeat_customer_rate"] = all_time_lookup.get("repeat_customer_rate", {}).get("value")
    return result


def render_primary_metrics(
    summary: dict[str, Any],
    source_units: dict[str, float],
    customer_metrics: dict[str, Any],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    selected_sources: list[str],
) -> None:
    st.subheader("Selected Period KPIs")
    st.caption(
        f"{start_date.date()} to {end_date.date()} based on raw order export `Paid Time`. "
        f"Sources counted: {', '.join(selected_sources)}."
    )

    primary = [
        ("gross_product_sales", "Gross Product Sales", "Before discounts"),
        ("net_product_sales", "Net Product Sales", "Gross product sales - seller discount - export refund amount"),
        ("product_sales_after_seller_discount", "After Seller Discount", "Gross product sales - seller discount"),
        ("product_sales_after_all_discounts", "After All Discounts", "Seller and platform discounts applied"),
        ("collected_order_amount", "Collected Order Amount", "Unique order amount field"),
    ]
    cols = st.columns(5)
    for col, (key, label, note) in zip(cols, primary):
        value = summary.get(key)
        negative = " negative" if key in {"seller_discount", "platform_discount", "order_refund_amount"} else ""
        col.markdown(
            f"""
            <div class="metric-card">
              <div class="metric-label">{label}</div>
              <div class="metric-value{negative}">{format_value(key, value)}</div>
              <div class="metric-note">{note}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    secondary = [
        ("seller_discount", "Seller Discount", "Export subtotal discount"),
        ("platform_discount", "Platform Discount", "Export subtotal discount"),
        ("order_refund_amount", "Export Refund Amount", "Order export refund field"),
        ("paid_orders", "Paid Orders", "Unique orders in period"),
        ("operational_units", "Operational Units", "All selected sources combined"),
    ]
    cols = st.columns(5)
    for col, (key, label, note) in zip(cols, secondary):
        value = summary.get(key) if key in summary else source_units.get(key)
        negative = " negative" if key in {"seller_discount", "platform_discount", "order_refund_amount"} else ""
        col.markdown(
            f"""
            <div class="mini-card">
              <div class="metric-label">{label}</div>
              <div class="mini-value{negative}">{format_value(key, value)}</div>
              <div class="metric-note">{note}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    tertiary = [
        ("sales_units", "Sales Units", "Units from `All orders` only"),
        ("sample_units", "Samples Sent", "Units from `Samples` only"),
        ("replacement_units", "Replacement Units", "Units from `Replacements` only"),
        ("selected_repeat_customer_rate", "Repeat Customer Rate", "Selected period; uses first available customer identifier"),
    ]
    cols = st.columns(4)
    for col, (key, label, note) in zip(cols, tertiary):
        if key in summary:
            value = summary.get(key)
        elif key in source_units:
            value = source_units.get(key)
        else:
            value = customer_metrics.get(key)
        col.markdown(
            f"""
            <div class="mini-card">
              <div class="metric-label">{label}</div>
              <div class="mini-value">{format_value(key, value)}</div>
              <div class="metric-note">{note}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    quaternary = [
        ("selected_unique_customers", "Unique Customers", "Selected period; chosen customer identifier only"),
        ("selected_repeat_customers", "Repeat Customers", "Customers with more than one valid selected-period order"),
    ]
    cols = st.columns(2)
    for col, (key, label, note) in zip(cols, quaternary):
        value = customer_metrics.get(key)
        col.markdown(
            f"""
            <div class="mini-card">
              <div class="metric-label">{label}</div>
              <div class="mini-value">{format_value(key, value)}</div>
              <div class="metric-note">{note}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.caption(f"Customer basis: {customer_metrics.get('customer_id_basis', 'not available')}. This is an export-level proxy, not a guaranteed TikTok account ID.")


def render_finance_trend(filtered_finance: pd.DataFrame) -> None:
    st.markdown('<div class="panel-shell">', unsafe_allow_html=True)
    st.subheader("Finance Trend")
    st.markdown('<div class="section-note">Choose one series for the daily line chart. Monthly bars stay focused on the two most useful views.</div>', unsafe_allow_html=True)
    if filtered_finance.empty:
        st.warning("No finance rows for the selected date range.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    metric = st.radio(
        "Daily Series",
        options=[
            "gross_product_sales",
            "product_sales_after_seller_discount",
            "product_sales_after_all_discounts",
            "collected_order_amount",
            "paid_orders",
        ],
        format_func=lambda v: v.replace("_", " ").title(),
        horizontal=True,
    )

    line_fig = px.line(
        filtered_finance,
        x="reporting_date",
        y=metric,
        markers=True,
        template="plotly_white",
        color_discrete_sequence=["#b44f1f"],
        labels={"reporting_date": "Date", metric: metric.replace("_", " ").title()},
    )
    line_fig.update_layout(margin=dict(l=10, r=10, t=12, b=10))
    st.plotly_chart(line_fig, width="stretch")

    monthly = filtered_finance.copy()
    monthly["month"] = monthly["reporting_date"].dt.to_period("M").dt.to_timestamp()
    monthly = monthly.groupby("month", as_index=False)[["gross_product_sales", "product_sales_after_seller_discount"]].sum()
    bar_fig = px.bar(
        monthly,
        x="month",
        y=["gross_product_sales", "product_sales_after_seller_discount"],
        barmode="group",
        template="plotly_white",
        color_discrete_sequence=["#d48655", "#3d6a54"],
        labels={"value": "Amount", "month": "Month", "variable": "Metric"},
    )
    bar_fig.update_layout(margin=dict(l=10, r=10, t=12, b=10), legend_title_text="")
    st.plotly_chart(bar_fig, width="stretch")
    st.markdown("</div>", unsafe_allow_html=True)


def render_reconciliation_panel(filtered_finance: pd.DataFrame) -> None:
    st.markdown('<div class="panel-shell">', unsafe_allow_html=True)
    st.subheader("Reconciliation Table")
    st.markdown('<div class="section-note">This is the daily paid-time export view that should track closer to TikTok finance than the old order-total dashboard.</div>', unsafe_allow_html=True)
    if filtered_finance.empty:
        st.warning("No rows for the selected date range.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    if len(filtered_finance):
        best_row = filtered_finance.sort_values("gross_product_sales", ascending=False).iloc[0]
        c1, c2 = st.columns(2)
        c1.metric("Best Gross Product Day", best_row["reporting_date"].date().isoformat(), format_value("gross_product_sales", best_row["gross_product_sales"]))
        c2.metric("Average Paid Orders / Day", f"{filtered_finance['paid_orders'].mean():,.1f}")

    preview = filtered_finance[
        [
            "reporting_date",
            "gross_product_sales",
            "seller_discount",
            "product_sales_after_seller_discount",
            "platform_discount",
            "product_sales_after_all_discounts",
            "collected_order_amount",
            "order_refund_amount",
            "paid_orders",
        ]
    ].sort_values("reporting_date", ascending=False)
    st.dataframe(preview, width="stretch", hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_status_panel(status_df: pd.DataFrame) -> None:
    st.markdown('<div class="panel-shell">', unsafe_allow_html=True)
    st.markdown('<div class="scope-chip">Selected period</div>', unsafe_allow_html=True)
    st.subheader("Status Mix")
    if status_df is None or status_df.empty:
        st.info("Status mix metrics were not found.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    fig = px.bar(
        status_df.sort_values("order_count", ascending=True),
        x="order_count",
        y="status",
        orientation="h",
        template="plotly_white",
        color_discrete_sequence=["#3d6a54"],
        labels={"order_count": "Orders", "status": "Status"},
    )
    fig.update_layout(margin=dict(l=10, r=10, t=12, b=10))
    st.plotly_chart(fig, width="stretch")
    st.markdown("</div>", unsafe_allow_html=True)


def render_product_panel(product_df: pd.DataFrame | None) -> None:
    st.markdown('<div class="panel-shell">', unsafe_allow_html=True)
    st.markdown('<div class="scope-chip">Selected period</div>', unsafe_allow_html=True)
    st.subheader("Product Performance")
    if product_df is None or product_df.empty:
        st.info("No product KPI file was found.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    c1, c2, c3 = st.columns([1.3, 0.8, 1.1])
    with c1:
        search = st.text_input("Filter Product Name", value="")
    with c2:
        top_n = st.selectbox("Rows", options=[10, 15, 20, 25, 30], index=0)
    with c3:
        rank_metric = st.selectbox(
            "Rank By",
            options=["net_merchandise_sales", "units_sold", "order_count"],
            format_func=lambda v: v.replace("_", " ").title(),
        )

    filtered = product_df.copy()
    if search.strip():
        filtered = filtered.loc[filtered["product_name"].fillna("").str.contains(search.strip(), case=False, na=False)]
    filtered = filtered.sort_values(rank_metric, ascending=False).head(top_n)

    if filtered.empty:
        st.warning("No products matched the current filter.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    fig = px.bar(
        filtered.sort_values(rank_metric, ascending=True),
        x=rank_metric,
        y="product_name",
        orientation="h",
        template="plotly_white",
        color_discrete_sequence=["#b44f1f"],
        labels={rank_metric: rank_metric.replace("_", " ").title(), "product_name": "Product"},
    )
    fig.update_layout(margin=dict(l=10, r=10, t=12, b=10), yaxis=dict(automargin=True))
    st.plotly_chart(fig, width="stretch")
    product_table = filtered[
        [
            "product_name",
            "seller_sku_resolved",
            "order_count",
            "units_sold",
            "sales_units",
            "sample_units",
            "replacement_units",
            "net_merchandise_sales",
            "returned_units",
        ]
    ].rename(
        columns={
            "product_name": "Product",
            "seller_sku_resolved": "Seller SKU",
            "order_count": "Orders",
            "units_sold": "Units Sold",
            "sales_units": "Sales Units",
            "sample_units": "Sample Units",
            "replacement_units": "Replacement Units",
            "net_merchandise_sales": "Net Merchandise Sales",
            "returned_units": "Returned Units",
        }
    )
    for col in ["Orders", "Units Sold", "Sales Units", "Sample Units", "Replacement Units", "Returned Units"]:
        product_table[col] = format_integer_series(product_table[col])
    product_table["Net Merchandise Sales"] = format_currency_series(product_table["Net Merchandise Sales"])
    st.dataframe(product_table, width="stretch", hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_cogs_panel(component_cogs_df: pd.DataFrame, listing_cogs_df: pd.DataFrame) -> None:
    st.markdown('<div class="panel-shell">', unsafe_allow_html=True)
    st.markdown('<div class="scope-chip">Selected period</div>', unsafe_allow_html=True)
    st.subheader("COGS Summary")
    st.markdown(
        '<div class="section-note">Bundle expansion assumptions: single-flavor bundle = 2 of the same 2-pack, mixed bundle = 1 of each named 2-pack, Variety Pack = fixed COGS override.</div>',
        unsafe_allow_html=True,
    )
    if component_cogs_df.empty:
        st.info("No COGS mappings could be built from the current product listings.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    display = component_cogs_df.copy()
    display["Component Units Sold"] = format_integer_series(display["Component Units Sold"])
    display["Unit COGS"] = format_currency_series(display["Unit COGS"])
    display["Estimated COGS"] = format_currency_series(display["Estimated COGS"])
    st.dataframe(display, width="stretch", hide_index=True)

    unresolved = listing_cogs_df.loc[listing_cogs_df["Estimated COGS"].isna(), ["Listing", "COGS Assumption"]]
    if not unresolved.empty:
        st.caption("Listings that still need a manual COGS rule")
        st.dataframe(unresolved, width="stretch", hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_cohort_panel(cohort_heatmap: pd.DataFrame, cohort_summary: pd.DataFrame) -> None:
    st.markdown('<div class="panel-shell">', unsafe_allow_html=True)
    st.markdown('<div class="scope-chip">Selected sources</div>', unsafe_allow_html=True)
    st.subheader("Customer Cohort Analysis")
    st.markdown(
        '<div class="section-note">Monthly retention based on the first valid order seen for the chosen customer identifier in the selected source set.</div>',
        unsafe_allow_html=True,
    )
    if cohort_heatmap.empty or cohort_summary.empty:
        st.info("Not enough customer-id data is available to build cohorts for the current filters.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    max_periods = min(6, len(cohort_heatmap.columns))
    visible_columns = list(cohort_heatmap.columns[:max_periods])
    heatmap_input = cohort_heatmap[visible_columns].copy()
    heatmap_fig = px.imshow(
        heatmap_input,
        aspect="auto",
        color_continuous_scale=["#f7efe3", "#d48655", "#8f3b17"],
        labels={"x": "Months Since First Order", "y": "Cohort Month", "color": "Retention"},
        text_auto=".0%",
    )
    heatmap_fig.update_layout(margin=dict(l=10, r=10, t=12, b=10), coloraxis_colorbar_tickformat=".0%")
    st.plotly_chart(heatmap_fig, width="stretch")

    display = cohort_summary.copy()
    display["first_order_month"] = display["first_order_month"].dt.strftime("%Y-%m")
    display = display.rename(
        columns={
            "first_order_month": "Cohort Month",
            "months_since_first": "Months Since First Order",
            "active_customers": "Active Customers",
            "cohort_size": "Cohort Size",
            "retention_rate": "Retention Rate",
        }
    )
    display["Active Customers"] = format_integer_series(display["Active Customers"])
    display["Cohort Size"] = format_integer_series(display["Cohort Size"])
    display["Retention Rate"] = format_percent_series(display["Retention Rate"])
    st.dataframe(display, width="stretch", hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def build_selected_source_daily_table(finance_df: pd.DataFrame, operational_df: pd.DataFrame) -> pd.DataFrame:
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
        finance_daily["operational_units"] = 0.0
        finance_daily["sample_units"] = 0.0
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
    units_daily = units_daily.rename(
        columns={
            "Sales": "sales_units",
            "Samples": "sample_units",
            "Replacements": "replacement_units",
        }
    )
    return finance_daily.merge(units_daily, on="reporting_date", how="left").fillna(0)


def render_tabs(
    kpi_full_df: pd.DataFrame,
    filtered_finance_df: pd.DataFrame,
    filtered_operational_df: pd.DataFrame,
    product_df: pd.DataFrame | None,
    listing_cogs_df: pd.DataFrame,
    report_md: str | None,
) -> None:
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["KPI Definitions", "Daily Finance Table", "Products", "COGS by Listing", "Report"])
    with tab1:
        display = (
            kpi_full_df[["metric", "category", "formatted_value", "formula", "notes"]]
            .rename(
                columns={
                    "metric": "KPI",
                    "category": "Category",
                    "formatted_value": "Value",
                    "formula": "How it's calculated",
                    "notes": "Interpretation / caveat",
                }
            )
            .sort_values(["Category", "KPI"])
        )
        st.caption("This table explains each KPI and how the dashboard or analyzer computes it.")
        st.dataframe(display, width="stretch", hide_index=True)
    with tab2:
        daily_selected = build_selected_source_daily_table(filtered_finance_df, filtered_operational_df)
        if daily_selected.empty:
            st.info("No rows for the selected date range.")
        else:
            daily_display = (
                daily_selected[
                    [
                        "reporting_date",
                        "gross_product_sales",
                        "net_product_sales",
                        "seller_discount",
                        "export_refund_amount",
                        "paid_orders",
                        "sales_units",
                        "sample_units",
                        "replacement_units",
                        "operational_units",
                    ]
                ]
                .rename(
                    columns={
                        "reporting_date": "Date",
                        "gross_product_sales": "Gross Product Sales",
                        "net_product_sales": "Net Product Sales",
                        "seller_discount": "Seller Discount",
                        "export_refund_amount": "Export Refund Amount",
                        "paid_orders": "Paid Orders",
                        "sales_units": "Sales Units",
                        "sample_units": "Sample Units",
                        "replacement_units": "Replacement Units",
                        "operational_units": "Operational Units",
                    }
                )
                .sort_values("Date", ascending=False)
            )
            for col in ["Gross Product Sales", "Net Product Sales", "Seller Discount", "Export Refund Amount"]:
                daily_display[col] = format_currency_series(daily_display[col])
            for col in ["Paid Orders", "Sales Units", "Sample Units", "Replacement Units", "Operational Units"]:
                daily_display[col] = format_integer_series(daily_display[col])
            st.caption("Selected-period raw export table. This one respects both the date window and the source toggles.")
            st.dataframe(daily_display, width="stretch", hide_index=True)
    with tab3:
        if product_df is None or product_df.empty:
            st.info("No product rows are available for the selected filters.")
        else:
            product_display = (
                product_df[
                    [
                        "product_name",
                        "seller_sku_resolved",
                        "order_count",
                        "units_sold",
                        "sales_units",
                        "sample_units",
                        "replacement_units",
                        "net_merchandise_sales",
                        "returned_units",
                        "return_rate_by_units",
                    ]
                ]
                .rename(
                    columns={
                        "product_name": "Product",
                        "seller_sku_resolved": "Seller SKU",
                        "order_count": "Orders",
                        "units_sold": "Units Sold",
                        "sales_units": "Sales Units",
                        "sample_units": "Sample Units",
                        "replacement_units": "Replacement Units",
                        "net_merchandise_sales": "Net Merchandise Sales",
                        "returned_units": "Returned Units",
                        "return_rate_by_units": "Return Rate by Units",
                    }
                )
                .sort_values("Net Merchandise Sales", ascending=False)
            )
            for col in ["Orders", "Units Sold", "Sales Units", "Sample Units", "Replacement Units", "Returned Units"]:
                product_display[col] = format_integer_series(product_display[col])
            product_display["Net Merchandise Sales"] = format_currency_series(product_display["Net Merchandise Sales"])
            product_display["Return Rate by Units"] = format_percent_series(product_display["Return Rate by Units"])
            st.caption("Selected-period product performance from raw paid-time order lines.")
            st.dataframe(product_display, width="stretch", hide_index=True)
    with tab4:
        if listing_cogs_df.empty:
            st.info("No listing-level COGS table is available.")
        else:
            display = listing_cogs_df.copy()
            display["Units Sold"] = format_integer_series(display["Units Sold"])
            display["Mapped Component Units"] = format_integer_series(display["Mapped Component Units"])
            display["Net Merchandise Sales"] = format_currency_series(display["Net Merchandise Sales"])
            display["Estimated COGS"] = format_currency_series(display["Estimated COGS"])
            display["Estimated Gross Profit"] = format_currency_series(display["Estimated Gross Profit"])
            st.caption("Selected-period listing level COGS estimate with explicit bundle assumptions.")
            st.dataframe(display, width="stretch", hide_index=True)
    with tab5:
        if report_md:
            st.markdown(report_md)
        else:
            st.info("No report file is available.")


render_page_header()

order_source_dirs = detect_order_source_dirs()
order_source_items = tuple((str(path), ORDER_SOURCE_FOLDERS[name]) for name, path in order_source_dirs.items())
finance_daily_df = load_paid_time_finance(order_source_items) if order_source_items else pd.DataFrame()
raw_operational_df = load_paid_time_operational(order_source_items) if order_source_items else pd.DataFrame()
if finance_daily_df is None or finance_daily_df.empty:
    st.error("Could not build the finance-style daily view from the raw order export folders.")
    st.stop()
if raw_operational_df is None or raw_operational_df.empty:
    st.error("Could not build the filtered product/status view from the raw order export folders.")
    st.stop()

available_dirs = detect_output_dirs()
output_options = [str(path) for path in available_dirs] or [str(DEFAULT_OUTPUT_DIR)]
default_index = output_options.index(str(DEFAULT_OUTPUT_DIR)) if str(DEFAULT_OUTPUT_DIR) in output_options else 0
available_sources = [label for _, label in order_source_items]
selected_dir, start_date, end_date, selected_sources = render_filters(
    output_options,
    default_index,
    finance_daily_df,
    available_sources,
)

data = load_dashboard_data(selected_dir)
kpi_full_df = data["kpi_full"]
report_md = data["report_md"]

if kpi_full_df is None or kpi_full_df.empty:
    st.error("This output folder is missing the files needed for the dashboard.")
    st.stop()

all_time_lookup = get_kpi_lookup(kpi_full_df)
filtered_finance_df = filter_daily(finance_daily_df, start_date, end_date, selected_sources)
filtered_operational_df = filter_daily(raw_operational_df, start_date, end_date, selected_sources)
selected_source_operational_df = raw_operational_df.loc[raw_operational_df["source_type"].isin(selected_sources)].copy()
summary_metrics = summarize_finance_period(filtered_finance_df, all_time_lookup)
source_units = summarize_source_units(filtered_operational_df)
customer_metrics = build_customer_metrics(filtered_operational_df)
cohort_heatmap_all, cohort_summary_all = build_cohort_retention(selected_source_operational_df)
cohort_heatmap, cohort_summary = filter_cohort_window(cohort_summary_all, start_date, end_date)
filtered_product_df = build_filtered_product_view(filtered_operational_df)
filtered_status_df = build_filtered_status_view(filtered_operational_df)
listing_cogs_df, component_cogs_df = build_cogs_views(filtered_product_df)

render_primary_metrics(summary_metrics, source_units, customer_metrics, start_date, end_date, selected_sources)

left, right = st.columns([1.25, 1.0])
with left:
    render_finance_trend(filtered_finance_df)
with right:
    render_reconciliation_panel(filtered_finance_df)

left, right = st.columns([0.8, 1.2])
with left:
    render_status_panel(filtered_status_df)
with right:
    render_product_panel(filtered_product_df)

render_cohort_panel(cohort_heatmap, cohort_summary)
render_cogs_panel(component_cogs_df, listing_cogs_df)
render_tabs(kpi_full_df, filtered_finance_df, filtered_operational_df, filtered_product_df, listing_cogs_df, report_md)
