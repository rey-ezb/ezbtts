# KPI Definitions And Formulas

## Revenue KPIs

- `gross_sales`: Sum of unique-order `Order Amount` for valid orders.
- `net_sales`: `gross_sales - refund_amount`.
- `gross_merchandise_sales`: Sum of line-level `SKU Subtotal Before Discount` for valid order lines.
- `net_merchandise_sales_before_refunds`: Sum of line-level `SKU Subtotal After Discount` for valid order lines.
- `refund_amount`: Sum of unique-order `Order Refund Amount`.

## Order KPIs

- `total_orders`: Count of distinct `Order ID`.
- `valid_orders`: Count of distinct `Order ID` where the order is not classified as canceled.
- `paid_orders`: Count of distinct `Order ID` with a non-null `Paid Time`.
- `delivered_orders`: Count of distinct `Order ID` with a non-null `Delivered Time` or delivered/completed status signals.

## Refund / Return / Cancellation KPIs

- `canceled_orders`: Count of distinct orders where status text, cancel timestamps, or cancel type indicate cancellation.
- `refunded_orders`: Count of distinct orders where `Order Refund Amount > 0` or `Cancelation/Return Type` indicates refund.
- `returned_orders`: Count of distinct orders where `Sku Quantity of return > 0` or `Cancelation/Return Type` indicates return.
- `refund_rate`: `refunded_orders / valid_orders`.
- `return_rate`: `returned_orders / valid_orders`.
- `cancellation_rate`: `canceled_orders / total_orders`.
- `returned_units`: Sum of `Sku Quantity of return`.

## AOV And Item KPIs

- `aov_gross`: `gross_sales / valid_orders`.
- `aov_net`: `net_sales / valid_orders`.
- `units_sold`: Sum of `Quantity` for valid order lines.
- `units_per_order`: `units_sold / valid_orders`.

## Customer KPIs

- `unique_customers`: Count of distinct chosen customer identifier across valid orders.
- `repeat_customers`: Count of customer identifiers with more than one valid order.
- `repeat_customer_rate`: `repeat_customers / unique_customers`.

## Status Breakdown KPIs

- `status_mix_placed`
- `status_mix_paid`
- `status_mix_shipped`
- `status_mix_delivered`
- `status_mix_refunded`
- `status_mix_returned`
- `status_mix_returned_refunded`
- `status_mix_canceled`

## Time-Based KPIs

For each of `daily`, `weekly`, and `monthly`, the analyzer writes:

- average gross sales
- best gross sales period
- number of active periods

## Assumptions

- The export is treated as line-item data.
- `Order Amount`, shipping, and tax fields are treated as order-level fields and are not summed per line.
- Refunded and returned orders remain part of `valid_orders` because they were processed orders, not pre-fulfillment cancellations.
- Merchant payout net, fees, commissions, profit, and margin are not available unless those fields exist in the export.
- Product-level refund dollars are intentionally not allocated across multi-line orders because the export does not provide an exact per-line refund ledger.
