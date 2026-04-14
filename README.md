# TikTok Shop KPI Analyzer

This project reads TikTok Shop order export files and produces reusable KPI outputs for EZ Bombs:

- `kpi_summary.csv`
- `kpi_full.csv`
- `daily_breakdown.csv`
- `product_kpis.csv` when product columns are available
- `report.md`

## Requirements

- Python 3.10+
- `pandas`
- `openpyxl` only if you want to read `.xlsx` files
- `streamlit` and `plotly` only if you still want the old Streamlit dashboard

Install dependencies:

```bash
python -m pip install pandas openpyxl
```

## Default Behavior

If you run the script from this workspace without specifying inputs:

- It looks for an `All orders` folder first.
- It excludes `Samples` and `Replacements` by default because those folders often contain zero-value operational orders that distort sales KPIs.
- It writes output files into `analysis_output/`.

## Run It

```bash
python tiktok_kpi_analyzer.py
python tiktok_kpi_analyzer.py --config config.sample.json
python tiktok_kpi_analyzer.py --input "All orders" "Samples\\Samples 2026.csv"
python tiktok_kpi_analyzer.py --output-dir custom_output
```

## Plain Web Dashboard

Launch the cleaner HTML/CSS/JS dashboard:

```bash
python web_dashboard/server.py
```

Then open:

```text
http://127.0.0.1:8080
```

The web dashboard reads from `analysis_output/` by default, lets you switch analyzer output folders, and reads raw `All orders/`, `Samples/`, and `Replacements/` exports directly for date-filtered metrics.

If you also drop TikTok finance workbooks into a folder like `Finance Tab/`, the dashboard will reference those finance rows back to the order exports by `Order ID` and expose statement-based reconciliation.

The web dashboard includes:

- overview KPI cards
- date and source filters
- date-basis toggle for order-export vs statement-based reconciliation views
- finance trend chart
- order status mix
- top product ranking
- finance statement reconciliation by `Order ID`
- target city lookup, top cities, top ZIPs, and ZIP-radius customer lookup
- cohort retention view
- daily, product, COGS, reconciliation, location, KPI, and report tabs

## Legacy Streamlit Dashboard

If you still want the previous Streamlit version:

```bash
python -m pip install streamlit plotly
streamlit run dashboard.py
```

## Config Notes

The config file is JSON and supports:

- `input_paths`: explicit file or folder paths
- `prefer_folder`: folder to prefer during auto-discovery
- `include_auxiliary_folders`: include `Samples` and `Replacements` during auto-discovery
- `exclude_globs`: patterns to ignore
- `column_overrides`: manual mapping from canonical fields to export column names
- `kpi.report_date_basis`: preferred date field for time-series output
- `kpi.customer_id_priority`: which fields to use for repeat-customer logic

## KPI Logic Highlights

- The export is line-item based, but `Order Amount` repeats across every line in a multi-line order.
- Revenue KPIs use unique orders for order-level sales amounts.
- Merchandise KPIs use line-level subtotal columns.
- Refunds, returns, and cancellations are tracked separately.
- `net_sales` means gross order sales minus `Order Refund Amount`. It is not payout net because fee and commission fields are not present in the export.
- If a KPI cannot be calculated exactly from the export, the script marks it as `not available from this export`.

## Known Limits

- True merchant net revenue, profit, margin, contribution margin, payout, and fee KPIs are not available unless the export includes settlement or fee columns.
- Product-level refund dollars are not allocated when a refund belongs to a multi-line order.
- Repeat-customer KPIs depend on the best available customer identifier in the export. The current priority is `Buyer Username`, then `Buyer Nickname`, then `Recipient`.
- The top finance cards still use raw order-export logic. Statement-based views are shown in the reconciliation and location sections and are date-basis aware.
