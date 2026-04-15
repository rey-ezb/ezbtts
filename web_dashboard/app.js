const state = {
  meta: null,
  payload: null,
  basePayload: null,
  deploymentMode: "dynamic",
  activeWorkspace: "orders",
  activeCard: "orders-gross-sales",
  chartModes: {
    "orders-status": "bar",
    "orders-products": "bar",
    "orders-cities": "pie",
    "finance-expense": "pie",
    "products-physical": "bar",
    "customers-cities": "pie",
  },
  activeSubtabs: {
    orders: "order-daily",
    finance: "statement-daily",
    reconciliation: "reconciliation",
    products: "products",
    customers: "target-city",
    audit: "audit",
  },
  detailExpanded: {},
  planningMessage: "",
  planningSettings: {
    baseline: "last_full_month",
    defaultUpliftPct: "35",
    baselineStart: "",
    baselineEnd: "",
    productForecasts: {},
    baselineOptions: [],
  },
  staticChunkCache: new Map(),
};

function getDashboardConfig() {
  return window.DASHBOARD_CONFIG || {};
}

function resolveDeploymentMode() {
  const configuredMode = getDashboardConfig().mode;
  if (configuredMode === "dynamic" || configuredMode === "static") return configuredMode;
  const host = window.location.hostname;
  if (host === "127.0.0.1" || host === "localhost") return "dynamic";
  return "static";
}

function metaUrl() {
  const config = getDashboardConfig();
  return state.deploymentMode === "static" ? config.staticMetaUrl || "./data/snapshot/meta.json" : "/api/meta";
}

function dashboardUrl() {
  const config = getDashboardConfig();
  if (state.deploymentMode === "static") return config.staticDashboardUrl || "./data/snapshot/dashboard.json";
  return `/api/dashboard?${buildQuery()}`;
}

function staticChunkUrl(path) {
  return new URL(path, dashboardUrl()).toString();
}

function uploadUrl() {
  const config = getDashboardConfig();
  return config.uploadUrl || "/api/upload";
}

function staticModeEnabled() {
  return state.deploymentMode === "static";
}

function monthRange(start, end) {
  const startDate = parseDateString(start);
  const endDate = parseDateString(end);
  if (!startDate || !endDate) return [];
  const cursor = new Date(startDate.getFullYear(), startDate.getMonth(), 1);
  const terminal = new Date(endDate.getFullYear(), endDate.getMonth(), 1);
  const months = [];
  while (cursor <= terminal) {
    months.push(`${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, "0")}`);
    cursor.setMonth(cursor.getMonth() + 1);
  }
  return months;
}

async function fetchStaticChunkRows(chunkKey, start, end) {
  const manifest = state.basePayload?.chunkManifest?.[chunkKey];
  if (!manifest) return [];
  const availableMonths = new Set(manifest.months || []);
  const neededMonths = monthRange(start, end).filter((month) => availableMonths.has(month));
  const chunks = await Promise.all(
    neededMonths.map(async (month) => {
      const paths = manifest.filesByMonth?.[month] || [];
      const monthParts = await Promise.all(
        paths.map(async (path) => {
          const cacheKey = `${chunkKey}:${path}`;
          if (state.staticChunkCache.has(cacheKey)) return state.staticChunkCache.get(cacheKey);
          const rows = await fetchJson(staticChunkUrl(path));
          state.staticChunkCache.set(cacheKey, rows || []);
          return rows || [];
        }),
      );
      return monthParts.flat();
    }),
  );
  return chunks.flat();
}

const planningInputs = [
  { product: "Birria Bomb 2-Pack", inputId: "forecastBirria", param: "forecast_birria" },
  { product: "Pozole Bomb 2-Pack", inputId: "forecastPozole", param: "forecast_pozole" },
  { product: "Tinga Bomb 2-Pack", inputId: "forecastTinga", param: "forecast_tinga" },
  { product: "Pozole Verde Bomb 2-Pack", inputId: "forecastPozoleVerde", param: "forecast_pozole_verde" },
  { product: "Brine Bomb", inputId: "forecastBrine", param: "forecast_brine" },
  { product: "Variety Pack", inputId: "forecastVarietyPack", param: "forecast_variety_pack" },
];

const defaultPlanningBaselineOptions = [
  { value: "last_full_month", label: "Last Full Month" },
  { value: "last_30_days", label: "Last 30 Days" },
  { value: "last_90_days", label: "Last 90 Days" },
  { value: "custom_range", label: "Custom Range" },
];

const inventoryProductMap = {
  "Birria Bomb 2-Pack": "Birria",
  "Pozole Bomb 2-Pack": "Pozole",
  "Tinga Bomb 2-Pack": "Tinga",
  "Brine Bomb": "Brine",
  "Variety Pack": "Variety Pack",
  "Pozole Verde Bomb 2-Pack": "Pozole Verde",
};

const cogsMap = {
  "Birria Bomb 2-Pack": { list_price: 19.99, unit_cogs: 3.10 },
  "Pozole Bomb 2-Pack": { list_price: 19.99, unit_cogs: 3.05 },
  "Tinga Bomb 2-Pack": { list_price: 19.99, unit_cogs: 3.15 },
  "Pozole Verde Bomb 2-Pack": { list_price: 19.99, unit_cogs: 3.75 },
  "Brine Bomb": { list_price: 19.99, unit_cogs: 4.2 },
  "Variety Pack": { list_price: 49.99, unit_cogs: 13.35 },
};

const inboundToFbtLeadDays = 5;
const reorderLeadDays = 8;

function planningSettingValue(param) {
  const defaultUplift = state.planningSettings.defaultUpliftPct || "35";
  return state.planningSettings.productForecasts[param] || defaultUplift;
}

function normalizeDateInput(value) {
  return value ? String(value).slice(0, 10) : "";
}

function parseDateString(value) {
  return value ? new Date(`${normalizeDateInput(value)}T00:00:00`) : null;
}

function ensureDate(value) {
  if (!value) return null;
  if (value instanceof Date) return value;
  return parseDateString(value);
}

function formatDateString(date) {
  const normalized = ensureDate(date);
  if (!normalized || Number.isNaN(normalized.getTime())) return "";
  return normalized.toISOString().slice(0, 10);
}

function addDays(date, days) {
  const normalized = ensureDate(date);
  if (!normalized || Number.isNaN(normalized.getTime())) return null;
  const next = new Date(normalized.getTime());
  next.setDate(next.getDate() + days);
  return next;
}

function daysBetweenInclusive(start, end) {
  return Math.max(1, Math.round((parseDateString(formatDateString(end)) - parseDateString(formatDateString(start))) / 86400000) + 1);
}

function inDateRange(value, start, end) {
  const normalized = normalizeDateInput(value);
  return Boolean(normalized && normalized >= start && normalized <= end);
}

function sumField(rows, key) {
  return rows.reduce((sum, row) => sum + Number(row?.[key] || 0), 0);
}

function groupBy(rows, keyFactory) {
  const map = new Map();
  rows.forEach((row) => {
    const key = keyFactory(row);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(row);
  });
  return map;
}

function aggregateProductRows(rows) {
  const grouped = groupBy(rows, (row) => `${row.product_name || ""}||${row.seller_sku_resolved || ""}`);
  return [...grouped.entries()]
    .map(([groupKey, productRows]) => {
      const [product_name, seller_sku_resolved] = groupKey.split("||");
      const units_sold = sumField(productRows, "units_sold");
      const returned_units = sumField(productRows, "returned_units");
      return {
        product_name,
        seller_sku_resolved,
        order_count: sumField(productRows, "order_count"),
        units_sold,
        returned_units,
        gross_merchandise_sales: sumField(productRows, "gross_merchandise_sales"),
        seller_discount: sumField(productRows, "seller_discount"),
        platform_discount: sumField(productRows, "platform_discount"),
        net_merchandise_sales: sumField(productRows, "net_merchandise_sales"),
        virtual_bundle_units: sumField(productRows, "virtual_bundle_units"),
        bundle_units_sold: sumField(productRows, "bundle_units_sold"),
        sales_units: sumField(productRows, "sales_units"),
        sample_units: sumField(productRows, "sample_units"),
        replacement_units: sumField(productRows, "replacement_units"),
        return_rate_by_units: units_sold ? returned_units / units_sold : null,
      };
    })
    .sort((a, b) => (b.net_merchandise_sales || 0) - (a.net_merchandise_sales || 0));
}

function buildCogsSummaryRows(productRows) {
  return productRows
    .filter((row) => cogsMap[row.product_name])
    .map((row) => ({
      product: row.product_name,
      component_units_sold: Number(row.units_sold || 0),
      list_price: cogsMap[row.product_name].list_price,
      unit_cogs: cogsMap[row.product_name].unit_cogs,
      estimated_cogs: Number(row.units_sold || 0) * cogsMap[row.product_name].unit_cogs,
    }))
    .sort((a, b) => (b.component_units_sold || 0) - (a.component_units_sold || 0));
}

function buildCogsListingRows(productRows) {
  return productRows
    .filter((row) => cogsMap[row.product_name])
    .map((row) => ({
      listing: row.product_name,
      listing_sku: row.seller_sku_resolved || "",
      units_sold: Number(row.units_sold || 0),
      mapped_component_units: Number(row.units_sold || 0),
      net_merchandise_sales: Number(row.net_merchandise_sales || 0),
      estimated_cogs: Number(row.units_sold || 0) * cogsMap[row.product_name].unit_cogs,
      estimated_gross_profit: Number(row.net_merchandise_sales || 0) - Number(row.units_sold || 0) * cogsMap[row.product_name].unit_cogs,
      cogs_assumption: "Hosted estimate from canonical product totals",
    }))
    .sort((a, b) => (b.net_merchandise_sales || 0) - (a.net_merchandise_sales || 0));
}

function choosePlanningBaselineWindow(horizonStart, horizonEnd) {
  const mode = state.planningSettings.baseline || "last_full_month";
  const latest = parseDateString(horizonEnd) || parseDateString(state.meta?.maxDate) || new Date();
  if (mode === "last_30_days") {
    return {
      baselineStart: formatDateString(addDays(latest, -29)),
      baselineEnd: formatDateString(latest),
      baselineLabel: "Last 30 Days",
    };
  }
  if (mode === "last_90_days") {
    return {
      baselineStart: formatDateString(addDays(latest, -89)),
      baselineEnd: formatDateString(latest),
      baselineLabel: "Last 90 Days",
    };
  }
  if (mode === "custom_range") {
    const start = normalizeDateInput(state.planningSettings.baselineStart || horizonStart);
    const end = normalizeDateInput(state.planningSettings.baselineEnd || horizonEnd);
    return start <= end
      ? { baselineStart: start, baselineEnd: end, baselineLabel: "Custom Range" }
      : { baselineStart: end, baselineEnd: start, baselineLabel: "Custom Range" };
  }
  const latestDate = parseDateString(horizonEnd) || latest;
  const currentMonthStart = new Date(latestDate.getFullYear(), latestDate.getMonth(), 1);
  const baselineEndDate = addDays(currentMonthStart, -1);
  const baselineStartDate = new Date(baselineEndDate.getFullYear(), baselineEndDate.getMonth(), 1);
  return {
    baselineStart: formatDateString(baselineStartDate),
    baselineEnd: formatDateString(baselineEndDate),
    baselineLabel: "Last Full Month",
  };
}

function safetyStockWeeksForDate(dateValue) {
  const date = parseDateString(dateValue);
  if (!date) return 3;
  const month = date.getMonth() + 1;
  const quarter = Math.ceil(month / 3);
  return quarter <= 2 ? 3 : 5;
}

function buildPlanningRows(productDailyRows, inventorySnapshot, horizonStart, horizonEnd) {
  const { baselineStart, baselineEnd, baselineLabel } = choosePlanningBaselineWindow(horizonStart, horizonEnd);
  const baselineRows = productDailyRows.filter((row) => inDateRange(row.reporting_date, baselineStart, baselineEnd));
  const baselineByProduct = aggregateProductRows(baselineRows);
  const demandLookup = Object.fromEntries(baselineByProduct.map((row) => [row.product_name, Number(row.units_sold || 0)]));
  const snapshotDate = normalizeDateInput(inventorySnapshot?.snapshot_date || horizonStart);
  const snapshotProducts = inventorySnapshot?.products || {};
  return planningInputs
    .map((input) => {
      const dashboardProduct = input.product;
      const inventoryProduct = inventoryProductMap[dashboardProduct];
      const inv = snapshotProducts[inventoryProduct] || {};
      const on_hand = Number(inv.on_hand || 0);
      const in_transit = Number(inv.in_transit || 0);
      const units_sold_in_window = Number(demandLookup[dashboardProduct] || 0);
      const baselineDays = daysBetweenInclusive(baselineStart, baselineEnd);
      const horizonDays = daysBetweenInclusive(horizonStart, horizonEnd);
      const upliftPct = Number(state.planningSettings.productForecasts[input.param] || state.planningSettings.defaultUpliftPct || 35);
      const avg_daily_demand = units_sold_in_window / baselineDays;
      const forecast_daily_demand = avg_daily_demand * (1 + upliftPct / 100);
      const projected_in_transit_arrival_date = in_transit > 0 ? formatDateString(addDays(parseDateString(snapshotDate), inboundToFbtLeadDays)) : null;
      const days_on_hand = forecast_daily_demand > 0 ? on_hand / forecast_daily_demand : null;
      const on_hand_stockout = days_on_hand != null ? formatDateString(addDays(parseDateString(snapshotDate), Math.floor(days_on_hand))) : null;
      const counted_in_transit = projected_in_transit_arrival_date && (!on_hand_stockout || projected_in_transit_arrival_date <= on_hand_stockout) ? in_transit : 0;
      const effective_total_supply = on_hand + counted_in_transit;
      const days_total_supply = forecast_daily_demand > 0 ? effective_total_supply / forecast_daily_demand : null;
      const projected_stockout_date = days_total_supply != null ? formatDateString(addDays(parseDateString(snapshotDate), Math.floor(days_total_supply))) : null;
      const safety_stock_weeks = safetyStockWeeksForDate(horizonStart);
      const safety_stock_units = forecast_daily_demand > 0 ? forecast_daily_demand * safety_stock_weeks * 7 : null;
      const forecast_units_in_horizon = forecast_daily_demand > 0 ? forecast_daily_demand * horizonDays : null;
      const reorder_quantity = Math.max(0, (forecast_units_in_horizon || 0) + (safety_stock_units || 0) - effective_total_supply);
      const reorder_date = projected_stockout_date
        ? formatDateString(addDays(parseDateString(projected_stockout_date), -((safety_stock_weeks * 7) + reorderLeadDays)))
        : null;
      let status = "No demand in baseline";
      if (forecast_daily_demand > 0 && reorder_quantity <= 0) status = "Covered";
      else if (forecast_daily_demand > 0 && reorder_date && reorder_date <= snapshotDate) status = "Urgent";
      else if (forecast_daily_demand > 0 && reorder_date && reorder_date <= formatDateString(addDays(parseDateString(snapshotDate), 7))) status = "Watch";
      else if (forecast_daily_demand > 0) status = "Healthy";
      return {
        product: dashboardProduct,
        inventory_product: inventoryProduct,
        snapshot_date: snapshotDate,
        baseline_label: baselineLabel,
        baseline_start: baselineStart,
        baseline_end: baselineEnd,
        on_hand,
        in_transit,
        counted_in_transit,
        effective_total_supply,
        units_sold_in_window,
        avg_daily_demand: avg_daily_demand || null,
        forecast_uplift_pct: upliftPct,
        forecast_daily_demand: forecast_daily_demand || null,
        forecast_units_in_horizon: forecast_units_in_horizon || null,
        safety_stock_weeks,
        safety_stock_units,
        projected_in_transit_arrival_date,
        days_on_hand,
        days_total_supply,
        weeks_on_hand: days_on_hand != null ? days_on_hand / 7 : null,
        weeks_total_supply: days_total_supply != null ? days_total_supply / 7 : null,
        projected_stockout_date,
        reorder_date,
        reorder_quantity,
        status,
      };
    })
    .sort((a, b) => (b.reorder_quantity || 0) - (a.reorder_quantity || 0));
}

function selectedSourcesFromDom() {
  const checked = [...document.querySelectorAll('#sourceToggles input[type="checkbox"]:checked')].map((input) => input.value);
  return checked.length ? checked : state.meta?.availableSources || [];
}

function filterOrderLevelRows(rows, start, end, selectedSources) {
  return (rows || []).filter((row) => {
    if (selectedSources?.length && !selectedSources.includes(row.source_type)) return false;
    return inDateRange(row.reporting_date, start, end);
  });
}

function filterOrderLevelRowsBySources(rows, selectedSources) {
  return (rows || []).filter((row) => !selectedSources?.length || selectedSources.includes(row.source_type));
}

function buildStatusRowsFromOrderLevel(orderLevelRows) {
  const counts = new Map();
  (orderLevelRows || []).forEach((row) => {
    let status = "Placed";
    if (row.is_shipped) status = "Shipped";
    if (row.is_delivered) status = "Delivered";
    if (row.is_returned) status = "Returned";
    if (row.is_refunded) status = "Refunded";
    if (row.is_canceled) status = "Canceled";
    counts.set(status, (counts.get(status) || 0) + 1);
  });
  return [...counts.entries()]
    .map(([status, order_count]) => ({ status, order_count }))
    .sort((a, b) => b.order_count - a.order_count);
}

function buildOrderHealthMetricsFromOrderLevel(orderLevelRows) {
  if (!orderLevelRows?.length) {
    return {
      valid_orders: null,
      canceled_orders: null,
      refunded_orders: null,
      returned_orders: null,
      delivered_orders: null,
      shipped_orders: null,
      units_per_paid_order: null,
      cancellation_rate: null,
      refund_rate: null,
      return_rate: null,
      delivery_rate: null,
    };
  }
  const totalOrders = orderLevelRows.length;
  const canceled_orders = orderLevelRows.filter((row) => row.is_canceled).length;
  const refunded_orders = orderLevelRows.filter((row) => row.is_refunded).length;
  const returned_orders = orderLevelRows.filter((row) => row.is_returned).length;
  const delivered_orders = orderLevelRows.filter((row) => row.is_delivered).length;
  const shipped_orders = orderLevelRows.filter((row) => row.is_shipped).length;
  const valid_orders = totalOrders - canceled_orders;
  const total_units = sumField(orderLevelRows, "units_sold");
  return {
    valid_orders,
    canceled_orders,
    refunded_orders,
    returned_orders,
    delivered_orders,
    shipped_orders,
    units_per_paid_order: totalOrders ? total_units / totalOrders : null,
    cancellation_rate: totalOrders ? canceled_orders / totalOrders : null,
    refund_rate: totalOrders ? refunded_orders / totalOrders : null,
    return_rate: totalOrders ? returned_orders / totalOrders : null,
    delivery_rate: totalOrders ? delivered_orders / totalOrders : null,
  };
}

function validCustomerOrdersFromOrderLevel(orderLevelRows) {
  const working = (orderLevelRows || []).filter((row) => !row.is_canceled && String(row.customer_id || "").trim());
  return working.map((row) => ({
    customer_id: row.customer_id,
    customer_id_source: row.customer_id_source,
    reporting_date: normalizeDateInput(row.reporting_date),
    order_month: `${normalizeDateInput(row.reporting_date).slice(0, 7)}-01`,
    source_type: row.source_type,
  }));
}

function buildCustomerProxyMix(firstSelectedByCustomer) {
  const counts = {
    "Buyer Username": 0,
    "Buyer Nickname": 0,
    Recipient: 0,
  };
  let total = 0;
  firstSelectedByCustomer.forEach((entry) => {
    if (!entry?.source) return;
    total += 1;
    if (counts[entry.source] == null) counts[entry.source] = 0;
    counts[entry.source] += 1;
  });
  return {
    customer_proxy_username_count: total ? counts["Buyer Username"] : null,
    customer_proxy_nickname_count: total ? counts["Buyer Nickname"] : null,
    customer_proxy_recipient_count: total ? counts.Recipient : null,
    customer_proxy_username_pct: total ? counts["Buyer Username"] / total : null,
    customer_proxy_nickname_pct: total ? counts["Buyer Nickname"] / total : null,
    customer_proxy_recipient_pct: total ? counts.Recipient / total : null,
  };
}

function buildCustomerMetricsFromOrderLevel(selectedRows, allRows, firstOrderRows = null) {
  const orderLevel = validCustomerOrdersFromOrderLevel(selectedRows);
  const fullOrderLevel = validCustomerOrdersFromOrderLevel(allRows);
  if (!orderLevel.length) {
    return {
      selected_unique_customers: null,
      selected_repeat_customers: null,
      selected_first_time_buyers: null,
      selected_returning_customers: null,
      selected_repeat_customer_rate: null,
      selected_first_time_buyer_rate: null,
      customer_id_basis: "Buyer Username -> Buyer Nickname -> Recipient",
      ...buildCustomerProxyMix(new Map()),
    };
  }
  const selectedCounts = new Map();
  orderLevel.forEach((row) => {
    selectedCounts.set(row.customer_id, (selectedCounts.get(row.customer_id) || 0) + 1);
  });
  const unique_customers = selectedCounts.size;
  const firstOrderByCustomer = new Map();
  if (firstOrderRows && firstOrderRows.length) {
    firstOrderRows.forEach((row) => {
      const customerId = String(row.customer_id || "").trim();
      const firstOrderDate = normalizeDateInput(row.first_order_date);
      if (customerId && firstOrderDate) firstOrderByCustomer.set(customerId, firstOrderDate);
    });
  } else {
    fullOrderLevel.forEach((row) => {
      const current = firstOrderByCustomer.get(row.customer_id);
      if (!current || row.reporting_date < current) firstOrderByCustomer.set(row.customer_id, row.reporting_date);
    });
  }
  const firstSelectedByCustomer = new Map();
  orderLevel.forEach((row) => {
    const current = firstSelectedByCustomer.get(row.customer_id);
    if (!current || row.reporting_date < current.date) {
      firstSelectedByCustomer.set(row.customer_id, { date: row.reporting_date, source: row.customer_id_source || "" });
    }
  });
  let selected_first_time_buyers = 0;
  let selected_repeat_customers = 0;
  firstSelectedByCustomer.forEach((selectedEntry, customerId) => {
    const selectedDate = selectedEntry.date;
    const firstOrderDate = firstOrderByCustomer.get(customerId);
    if (firstOrderDate === selectedDate) selected_first_time_buyers += 1;
    else if (firstOrderDate && firstOrderDate < selectedDate) selected_repeat_customers += 1;
  });
  const selected_returning_customers = selected_repeat_customers;
  return {
    selected_unique_customers: unique_customers,
    selected_repeat_customers,
    selected_first_time_buyers,
    selected_returning_customers,
    selected_repeat_customer_rate: unique_customers ? selected_repeat_customers / unique_customers : null,
    selected_first_time_buyer_rate: unique_customers ? selected_first_time_buyers / unique_customers : null,
    customer_id_basis: "Buyer Username -> Buyer Nickname -> Recipient",
    ...buildCustomerProxyMix(firstSelectedByCustomer),
  };
}

function filterCohortSummaryRows(allRows, start, end) {
  const startMonth = `${normalizeDateInput(start).slice(0, 7)}-01`;
  const endMonth = `${normalizeDateInput(end).slice(0, 7)}-01`;
  return (allRows || []).filter((row) => {
    const month = normalizeDateInput(row.first_order_month);
    return month && month >= startMonth && month <= endMonth;
  });
}

function buildCohortHeatmap(summaryRows) {
  const grouped = new Map();
  (summaryRows || []).forEach((row) => {
    const cohort = normalizeDateInput(row.first_order_month).slice(0, 7);
    if (!cohort) return;
    if (!grouped.has(cohort)) grouped.set(cohort, {});
    grouped.get(cohort)[String(row.months_since_first)] = Number(row.retention_rate || 0);
  });
  return [...grouped.entries()]
    .sort((a, b) => b[0].localeCompare(a[0]))
    .map(([cohort, values]) => ({ cohort, values }));
}

function haversineMiles(lat1, lon1, lat2, lon2) {
  const radius = 3958.8;
  const toRad = (value) => (Number(value) * Math.PI) / 180;
  const phi1 = toRad(lat1);
  const phi2 = toRad(lat2);
  const deltaPhi = toRad(Number(lat2) - Number(lat1));
  const deltaLambda = toRad(Number(lon2) - Number(lon1));
  const a = Math.sin(deltaPhi / 2) ** 2 + Math.cos(phi1) * Math.cos(phi2) * Math.sin(deltaLambda / 2) ** 2;
  return 2 * radius * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function buildLocationViewsFromOrderLevel(orderLevelRows, targetZip, radiusMiles, targetCity, targetState, cityRadiusMiles) {
  const validCustomers = (orderLevelRows || []).filter((row) => String(row.customer_id || "").trim());
  const citiesMap = groupBy(validCustomers, (row) => `${String(row.city || "").trim()}||${String(row.state || "").trim()}`);
  const cityRows = [...citiesMap.entries()]
    .map(([key, rows]) => {
      const [city, state] = key.split("||");
      return {
        city,
        state,
        unique_customers: new Set(rows.map((row) => row.customer_id)).size,
        orders: new Set(rows.map((row) => row.order_id)).size,
      };
    })
    .sort((a, b) => (b.unique_customers - a.unique_customers) || (b.orders - a.orders))
    .slice(0, 100);
  const zipsMap = groupBy(validCustomers.filter((row) => String(row.zipcode || "").trim()), (row) => String(row.zipcode || "").trim());
  const allZipRows = [...zipsMap.entries()]
    .map(([zipcode, rows]) => ({
      zipcode,
      unique_customers: new Set(rows.map((row) => row.customer_id)).size,
      orders: new Set(rows.map((row) => row.order_id)).size,
      latitude: rows.find((row) => row.latitude != null)?.latitude ?? null,
      longitude: rows.find((row) => row.longitude != null)?.longitude ?? null,
    }))
    .sort((a, b) => (b.unique_customers - a.unique_customers) || (b.orders - a.orders));
  const zipRows = allZipRows.slice(0, 100);

  const targetCityClean = String(targetCity || "").trim().toLowerCase();
  const targetStateClean = String(targetState || "").trim().toUpperCase();
  let targetCityRows = [];
  let targetCitySummary = { target_city: String(targetCity || "").trim(), target_state: targetStateClean, city_radius_miles: Number(cityRadiusMiles || radiusMiles || 0), customers_in_city: 0, orders_in_city: 0, zip_rows_in_city: 0 };
  if (targetCityClean) {
    const cityFiltered = validCustomers.filter((row) => {
      const cityMatch = String(row.city || "").trim().toLowerCase() === targetCityClean;
      const stateMatch = !targetStateClean || String(row.state_code || "").trim().toUpperCase() === targetStateClean;
      return cityMatch && stateMatch;
    });
    const cityZipMap = groupBy(cityFiltered.filter((row) => String(row.zipcode || "").trim()), (row) => `${String(row.zipcode || "").trim()}||${String(row.city || "").trim()}||${String(row.state || "").trim()}||${String(row.state_code || "").trim()}`);
    const cityZipRows = [...cityZipMap.entries()]
      .map(([key, rows]) => {
        const [zipcode, city, state, state_code] = key.split("||");
        return {
          zipcode,
          city,
          state,
          state_code,
          unique_customers: new Set(rows.map((row) => row.customer_id)).size,
          orders: new Set(rows.map((row) => row.order_id)).size,
        };
      })
      .sort((a, b) => (b.unique_customers - a.unique_customers) || (b.orders - a.orders));
    targetCityRows = cityZipRows.slice(0, 100);
    if (cityFiltered.length) {
      const cityRadius = Number(cityRadiusMiles || radiusMiles || 0);
      const cityZipCentroids = cityZipRows.filter((row) => row.latitude != null && row.longitude != null);
      if (cityZipCentroids.length) {
        const centerLat = cityZipCentroids.reduce((sum, row) => sum + Number(row.latitude || 0), 0) / cityZipCentroids.length;
        const centerLon = cityZipCentroids.reduce((sum, row) => sum + Number(row.longitude || 0), 0) / cityZipCentroids.length;
        targetCityRows = allZipRows
          .filter((row) => row.latitude != null && row.longitude != null)
          .map((row) => ({
            ...row,
            distance_miles: haversineMiles(centerLat, centerLon, row.latitude, row.longitude),
          }))
          .filter((row) => row.distance_miles <= cityRadius)
          .sort((a, b) => a.distance_miles - b.distance_miles)
          .slice(0, 100);
      }
      targetCitySummary = {
        target_city: cityFiltered[0].city,
        target_state: cityFiltered.find((row) => String(row.state_code || "").trim())?.state_code || targetStateClean,
        city_radius_miles: cityRadius,
        customers_in_city: targetCityRows.reduce((sum, row) => sum + Number(row.unique_customers || 0), 0),
        orders_in_city: targetCityRows.reduce((sum, row) => sum + Number(row.orders || 0), 0),
        zip_rows_in_city: targetCityRows.length,
      };
    }
  }

  let radiusRows = [];
  let radiusSummary = { target_zip: String(targetZip || "").trim(), radius_miles: Number(radiusMiles || 0), customers_within_radius: 0, orders_within_radius: 0, matched_zip_rows: 0 };
  const normalizedTargetZip = String(targetZip || "").replace(/\D/g, "").slice(0, 5);
  if (normalizedTargetZip) {
    const target = allZipRows.find((row) => row.zipcode === normalizedTargetZip && row.latitude != null && row.longitude != null);
    if (target) {
      radiusRows = allZipRows
        .filter((row) => row.latitude != null && row.longitude != null)
        .map((row) => ({
          ...row,
          distance_miles: haversineMiles(target.latitude, target.longitude, row.latitude, row.longitude),
        }))
        .filter((row) => row.distance_miles <= Number(radiusMiles || 0))
        .sort((a, b) => a.distance_miles - b.distance_miles)
        .slice(0, 100);
      radiusSummary = {
        target_zip: normalizedTargetZip,
        radius_miles: Number(radiusMiles || 0),
        customers_within_radius: radiusRows.reduce((sum, row) => sum + Number(row.unique_customers || 0), 0),
        orders_within_radius: radiusRows.reduce((sum, row) => sum + Number(row.orders || 0), 0),
        matched_zip_rows: radiusRows.length,
      };
    }
  }

  return { cityRows, zipRows, radiusRows, radiusSummary, targetCityRows, targetCitySummary };
}

function buildRawProductNameRows(rawRows, start, end, selectedSources) {
  const rows = (rawRows || []).filter((row) => {
    const source = String(row.source_type || "").trim();
    return (!selectedSources?.length || selectedSources.includes(source)) && inDateRange(row.reporting_date, start, end);
  });
  const grouped = groupBy(rows, (row) => String(row.product_name || "").trim());
  return [...grouped.entries()]
    .filter(([productName]) => productName)
    .map(([product_name, productRows]) => ({
      product_name,
      order_count: new Set(productRows.map((row) => String(row.order_id || "").trim()).filter(Boolean)).size,
      units_sold: productRows.reduce((sum, row) => sum + Number(row.units_sold || 0), 0),
    }))
    .sort((a, b) => (b.units_sold - a.units_sold) || (b.order_count - a.order_count))
    .slice(0, 100);
}

function buildStatementRollupRows(statementRows) {
  const grouped = groupBy((statementRows || []).filter((row) => String(row.order_id || "").trim()), (row) => String(row.order_id || "").trim());
  return [...grouped.entries()].map(([order_id, rows]) => {
    const firstDates = rows.map((row) => normalizeDateInput(row.statement_date)).filter(Boolean).sort();
    const sum = (key) => rows.reduce((total, row) => total + Number(row?.[key] || 0), 0);
    const output = {
      order_id,
      first_statement_date: firstDates[0] || null,
      last_statement_date: firstDates[firstDates.length - 1] || null,
      statement_row_count: rows.length,
      statement_amount_total: sum("amount"),
      total_settlement_amount: sum("total_settlement_amount"),
    };
    [
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
    ].forEach((key) => {
      output[key] = sum(key);
    });
    output.shipping_fee_total = [
      "statement_shipping",
      "statement_tiktok_shipping_fee",
      "statement_fbt_shipping_fee",
      "statement_customer_paid_shipping_fee",
      "statement_customer_paid_shipping_fee_refund",
      "statement_tiktok_shipping_incentive",
      "statement_tiktok_shipping_incentive_refund",
      "statement_customer_shipping_fee_offset",
      "typed_shipping_fee",
    ].reduce((total, key) => total + Number(output[key] || 0), 0);
    output.fulfillment_fee_total = Number(output.statement_fbt_fulfillment_fee || 0) + Number(output.typed_fulfillment_fee || 0);
    output.affiliate_fee_total = [
      "statement_affiliate_commission",
      "statement_affiliate_partner_commission",
      "statement_affiliate_shop_ads_commission",
      "statement_affiliate_partner_shop_ads_commission",
      "statement_tiktok_partner_commission",
      "typed_affiliate_fee",
    ].reduce((total, key) => total + Number(output[key] || 0), 0);
    output.marketing_fee_total = [
      "statement_cofunded_promotion",
      "statement_campaign_service_fee",
      "statement_smart_promotion_fee",
      "statement_marketing_benefits_package_fee",
      "typed_marketing_fee",
    ].reduce((total, key) => total + Number(output[key] || 0), 0);
    output.service_fee_total = [
      "statement_transaction_fee",
      "statement_referral_fee",
      "statement_refund_admin_fee",
      "typed_referral_fee",
      "typed_service_fee",
      "typed_other_fee",
    ].reduce((total, key) => total + Number(output[key] || 0), 0);
    output.actual_fee_total = output.shipping_fee_total + output.fulfillment_fee_total + output.affiliate_fee_total + output.marketing_fee_total + output.service_fee_total;
    output.statement_net_after_fees = Number(output.statement_net_sales || 0) + output.actual_fee_total + Number(output.statement_adjustment_amount || 0);
    return output;
  });
}

function buildReconciliationViewFromRows(orderLevelAllRows, statementRowsAll, dateBasis, start, end) {
  const orderById = new Map((orderLevelAllRows || []).map((row) => [String(row.order_id), row]));
  const allStatementRows = statementRowsAll || [];
  const statementRollupAll = buildStatementRollupRows(allStatementRows);
  let reconciliationRows = [];
  let unmatchedStatementRows = [];
  let unmatchedOrderRows = [];
  if (dateBasis === "statement") {
    const filteredStatementRows = allStatementRows.filter((row) => inDateRange(row.statement_date, start, end));
    const filteredRollup = buildStatementRollupRows(filteredStatementRows);
    reconciliationRows = filteredRollup.filter((row) => orderById.has(String(row.order_id))).map((row) => ({ ...row, ...orderById.get(String(row.order_id)) }));
    unmatchedStatementRows = filteredStatementRows.filter((row) => !orderById.has(String(row.order_id)));
  } else {
    const filteredOrders = filterOrderLevelRows(orderLevelAllRows, start, end, selectedSourcesFromDom());
    const rollupById = new Map(statementRollupAll.map((row) => [String(row.order_id), row]));
    reconciliationRows = filteredOrders.map((row) => ({ ...row, ...(rollupById.get(String(row.order_id)) || {}) }));
    unmatchedOrderRows = reconciliationRows.filter((row) => row.statement_amount_total == null);
    const filteredOrderIds = new Set(filteredOrders.map((row) => String(row.order_id)));
    unmatchedStatementRows = allStatementRows.filter((row) => !filteredOrderIds.has(String(row.order_id)));
  }
  const summary = {
    date_basis: dateBasis,
    matched_orders: new Set(reconciliationRows.filter((row) => row.statement_amount_total != null).map((row) => String(row.order_id))).size,
    unmatched_statement_rows: unmatchedStatementRows.length,
    unmatched_orders: new Set(unmatchedOrderRows.map((row) => String(row.order_id))).size,
    statement_amount_total: sumField(reconciliationRows.filter((row) => row.statement_amount_total != null), "statement_amount_total"),
    actual_fee_total: sumField(reconciliationRows.filter((row) => row.actual_fee_total != null), "actual_fee_total"),
    fulfillment_fee_total: sumField(reconciliationRows.filter((row) => row.fulfillment_fee_total != null), "fulfillment_fee_total"),
  };
  return { reconciliationRows, unmatchedStatementRows, unmatchedOrderRows, reconciliationSummary: summary };
}

async function buildStaticPayload(basePayload) {
  const start = normalizeDateInput(document.getElementById("startDate")?.value || state.meta?.minDate || basePayload.summary?.start_date);
  const end = normalizeDateInput(document.getElementById("endDate")?.value || state.meta?.maxDate || basePayload.summary?.end_date);
  const selectedSources = selectedSourcesFromDom();
  const dateBasis = document.getElementById("dateBasisSelect")?.value || "order";
  const targetZip = document.getElementById("targetZip")?.value || basePayload.summary?.target_zip || "";
  const radiusMiles = document.getElementById("radiusMilesSelect")?.value || basePayload.summary?.radius_miles || 20;
  const cityRadiusMiles = document.getElementById("cityRadiusMilesSelect")?.value || basePayload.summary?.city_radius_miles || radiusMiles || 20;
  const targetCity = document.getElementById("targetCity")?.value || basePayload.summary?.target_city || "";
  const targetState = document.getElementById("targetState")?.value || basePayload.summary?.target_state || "";
  const [productDailyChunkRows, orderLevelChunkRows, customerFirstOrderChunkRows, statementChunkRows, rawProductNameChunkRows] = await Promise.all([
    fetchStaticChunkRows("productDailyRows", start, end),
    fetchStaticChunkRows("orderLevelRowsAll", start, end),
    fetchStaticChunkRows(
      "customerFirstOrderAllRows",
      state.meta?.minDate || basePayload.summary?.start_date || start,
      end,
    ),
    fetchStaticChunkRows("statementRowsAll", start, end),
    fetchStaticChunkRows("rawProductNameAllRows", start, end),
  ]);
  const orderDailyRows = (basePayload.orderDailyRows || []).filter((row) => inDateRange(row.reporting_date, start, end));
  const statementDailyRows = (basePayload.statementDailyRows || []).filter((row) => inDateRange(row.reporting_date, start, end));
  const productDailyRows = (productDailyChunkRows && productDailyChunkRows.length)
    ? productDailyChunkRows
    : (basePayload.productDailyRows || []).filter((row) => inDateRange(row.reporting_date, start, end));
  const orderLevelAllRows = filterOrderLevelRowsBySources(orderLevelChunkRows || [], selectedSources);
  const filteredOrderLevelRows = filterOrderLevelRows(orderLevelAllRows, start, end, selectedSources);
  const customerMetrics = buildCustomerMetricsFromOrderLevel(
    filteredOrderLevelRows,
    orderLevelAllRows,
    customerFirstOrderChunkRows || [],
  );
  const orderHealthMetrics = buildOrderHealthMetricsFromOrderLevel(filteredOrderLevelRows);
  const statusRows = buildStatusRowsFromOrderLevel(filteredOrderLevelRows);
  const locationViews = buildLocationViewsFromOrderLevel(filteredOrderLevelRows, targetZip, radiusMiles, targetCity, targetState, cityRadiusMiles);
  const cohortSummaryRows = filterCohortSummaryRows(basePayload.cohortSummaryAllRows || [], start, end);
  const cohortHeatmap = buildCohortHeatmap(cohortSummaryRows);
  const reconciliationView = buildReconciliationViewFromRows(orderLevelAllRows, statementChunkRows || [], dateBasis, start, end);
  const rawProductNameRows = buildRawProductNameRows(rawProductNameChunkRows || [], start, end, selectedSources);
  const productRows = aggregateProductRows(productDailyRows);
  const cogsSummaryRows = buildCogsSummaryRows(productRows);
  const cogsListingRows = buildCogsListingRows(productRows);
  const planningRows = buildPlanningRows(productDailyRows, basePayload.inventorySnapshot || {}, start, end);
  const orders_gross_product_sales = sumField(orderDailyRows, "gross_product_sales");
  const product_sales_after_seller_discount = orders_gross_product_sales - sumField(orderDailyRows, "seller_discount");
  const orders_net_product_sales = sumField(orderDailyRows, "net_product_sales");
  const orders_paid_orders = sumField(orderDailyRows, "paid_orders");
  const sales_units = sumField(orderDailyRows, "sales_units");
  const sample_units = sumField(orderDailyRows, "sample_units");
  const replacement_units = sumField(orderDailyRows, "replacement_units");
  const operational_units = sumField(orderDailyRows, "operational_units");
  const planningConfig = planningRows.length
    ? {
        baselineLabel: planningRows[0].baseline_label,
        baselineStart: planningRows[0].baseline_start,
        baselineEnd: planningRows[0].baseline_end,
        defaultUpliftPct: Number(state.planningSettings.defaultUpliftPct || 35),
        productForecasts: planningInputs.map((item) => ({
          product: item.product,
          uplift_pct: Number(state.planningSettings.productForecasts[item.param] || state.planningSettings.defaultUpliftPct || 35),
        })),
      }
    : basePayload.planningConfig || {};
  return {
    ...basePayload,
    summary: {
      ...(basePayload.summary || {}),
      start_date: start,
      end_date: end,
      selected_sources: selectedSources,
      date_basis: dateBasis,
      target_zip: String(targetZip || "").replace(/\D/g, "").slice(0, 5),
      radius_miles: Number(radiusMiles || 20),
      target_city: String(targetCity || "").trim(),
      target_state: String(targetState || "").trim().toUpperCase(),
      city_radius_miles: Number(cityRadiusMiles || radiusMiles || 20),
      planning_baseline: state.planningSettings.baseline,
      planning_baseline_start: planningConfig.baselineStart || null,
      planning_baseline_end: planningConfig.baselineEnd || null,
      planning_default_uplift: Number(state.planningSettings.defaultUpliftPct || 35),
      deployment_mode: "static",
    },
    orderSummary: {
      ...(basePayload.orderSummary || {}),
      orders_gross_product_sales,
      product_sales_after_seller_discount,
      product_sales_after_all_discounts: orders_net_product_sales,
      orders_seller_discount: sumField(orderDailyRows, "seller_discount"),
      orders_refund_amount: sumField(orderDailyRows, "export_refund_amount"),
      orders_paid_orders,
      orders_net_product_sales,
      orders_aov: orders_paid_orders ? orders_gross_product_sales / orders_paid_orders : null,
      operational_units,
      sales_units,
      sample_units,
      replacement_units,
      units_per_paid_order: orderHealthMetrics.units_per_paid_order,
      ...customerMetrics,
      ...orderHealthMetrics,
    },
    statementSummary: {
      ...(basePayload.statementSummary || {}),
      finance_gross_sales: sumField(statementDailyRows, "gross_sales"),
      finance_gross_sales_refund: sumField(statementDailyRows, "gross_sales_refund"),
      finance_seller_discount: sumField(statementDailyRows, "seller_discount"),
      finance_seller_discount_refund: sumField(statementDailyRows, "seller_discount_refund"),
      finance_net_sales: sumField(statementDailyRows, "net_sales"),
      finance_shipping_total: sumField(statementDailyRows, "shipping_total"),
      finance_fees_total: sumField(statementDailyRows, "fees_total"),
      finance_adjustments_total: sumField(statementDailyRows, "adjustments_total"),
      finance_payout_amount: sumField(statementDailyRows, "payout_amount"),
    },
    dataQualitySummary: {
      ...(basePayload.dataQualitySummary || {}),
      message: basePayload.dataQualitySummary?.message || "Hosted mode recalculates views from the shared hosted dataset.",
    },
    orderDailyRows,
    statementDailyRows,
    statusRows,
    rawProductNameRows,
    productRows,
    productDailyRows,
    cogsSummaryRows,
    cogsListingRows,
    inventoryPlanningRows: planningRows,
    planningConfig,
    cityRows: locationViews.cityRows,
    zipRows: locationViews.zipRows,
    radiusRows: locationViews.radiusRows,
    radiusSummary: locationViews.radiusSummary,
    targetCityRows: locationViews.targetCityRows,
    targetCitySummary: locationViews.targetCitySummary,
    cohortSummaryRows,
    cohortHeatmap,
    reconciliationRows: reconciliationView.reconciliationRows,
    unmatchedStatementRows: reconciliationView.unmatchedStatementRows,
    unmatchedOrderRows: reconciliationView.unmatchedOrderRows,
    reconciliationSummary: reconciliationView.reconciliationSummary,
  };
}

const workspaces = [
  {
    key: "orders",
    label: "Orders",
    title: "Orders / paid time",
    note: "Operational view based on order paid time from the order exports.",
    subtabs: [
      ["order-daily", "Daily table"],
      ["audit", "Math audit"],
    ],
  },
  {
    key: "finance",
    label: "Finance",
    title: "Finance / statement date",
    note: "Settlement view based on finance statement dates, including prior-month orders that settled in the current month.",
    subtabs: [
      ["statement-daily", "Daily table"],
      ["expense-structure", "Expense structure"],
    ],
  },
  {
    key: "reconciliation",
    label: "Reconciliation",
    title: "Order ID reconciliation",
    note: "Join order exports back to statement rows without blending the two month bases.",
    subtabs: [["reconciliation", "Matched rows"]],
  },
  {
    key: "products",
    label: "Products",
    title: "Listings, physical units, and COGS",
    note: "Listing performance separated from physical products sent to TikTok.",
    subtabs: [
      ["products", "Products"],
      ["physical", "Products sent"],
      ["planning", "Planning"],
      ["inventory-history", "Inventory history"],
      ["cogs", "COGS"],
      ["raw", "Raw names"],
    ],
  },
  {
    key: "customers",
    label: "Customers",
    title: "Geography and retention",
    note: "Target cities, ZIP radius, top locations, and cohort behavior.",
    subtabs: [
      ["target-city", "Target city"],
      ["cities", "Cities"],
      ["zips", "ZIPs"],
      ["radius", "Radius"],
      ["cohorts", "Cohorts"],
    ],
  },
  {
    key: "audit",
    label: "Audit",
    title: "Traceability",
    note: "Raw names, formulas, and exported reporting notes.",
    subtabs: [
      ["audit", "Math audit"],
      ["kpis", "KPI definitions"],
      ["report", "Report"],
    ],
  },
];

const currency = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });
const integer = new Intl.NumberFormat("en-US");
const percent = new Intl.NumberFormat("en-US", {
  style: "percent",
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});

function fmtCurrency(value) {
  return value == null ? "N/A" : currency.format(value);
}

function fmtNumber(value) {
  return value == null ? "N/A" : integer.format(Math.round(value));
}

function fmtPercent(value) {
  return value == null ? "N/A" : percent.format(value);
}

function fmtDate(value) {
  return value ? escapeHtml(value) : "N/A";
}

function fmtDistance(value) {
  return value == null ? "N/A" : `${Number(value).toFixed(1)} mi`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Request failed: ${response.status}`);
  return response.json();
}

async function postForm(url, formData) {
  const response = await fetch(url, { method: "POST", body: formData });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || `Request failed: ${response.status}`);
  return payload;
}

function selectedSources() {
  return [...document.querySelectorAll("input[name='source']:checked")].map((el) => el.value);
}

function clampDateRange(minDate, maxDate) {
  const startInput = document.getElementById("startDate");
  const endInput = document.getElementById("endDate");
  if (!minDate || !maxDate) return;
  startInput.min = minDate;
  startInput.max = maxDate;
  endInput.min = minDate;
  endInput.max = maxDate;

  if (!startInput.value || startInput.value < minDate || startInput.value > maxDate) {
    const max = new Date(maxDate);
    const suggested = new Date(max);
    suggested.setDate(suggested.getDate() - 30);
    const min = new Date(minDate);
    startInput.value = (suggested < min ? min : suggested).toISOString().slice(0, 10);
  }
  if (!endInput.value || endInput.value < minDate || endInput.value > maxDate) {
    endInput.value = maxDate;
  }
  if (startInput.value > endInput.value) {
    startInput.value = minDate;
    endInput.value = maxDate;
  }
}

function syncDateBounds() {
  if (!state.meta) return;
  const basis = document.getElementById("dateBasisSelect").value;
  if (basis === "statement" && state.meta.statementMinDate && state.meta.statementMaxDate) {
    clampDateRange(state.meta.statementMinDate, state.meta.statementMaxDate);
    return;
  }
  clampDateRange(state.meta.minDate, state.meta.maxDate);
}

function buildQuery() {
  const params = new URLSearchParams();
  params.set("start", document.getElementById("startDate").value);
  params.set("end", document.getElementById("endDate").value);
  params.set("output", document.getElementById("outputSelect").value);
  params.set("sources", selectedSources().join(","));
  params.set("date_basis", document.getElementById("dateBasisSelect").value);
  params.set("order_bucket_mode", document.getElementById("orderBucketModeSelect").value);
  params.set("target_zip", document.getElementById("targetZip").value.trim());
  params.set("target_city", document.getElementById("targetCity").value.trim());
  params.set("target_state", document.getElementById("targetState").value.trim().toUpperCase());
  params.set("radius_miles", document.getElementById("radiusMilesSelect").value);
  params.set("city_radius_miles", document.getElementById("cityRadiusMilesSelect").value);
  params.set("planning_baseline", state.planningSettings.baseline || "last_full_month");
  params.set("planning_baseline_start", state.planningSettings.baselineStart || "");
  params.set("planning_baseline_end", state.planningSettings.baselineEnd || "");
  params.set("planning_default_uplift", state.planningSettings.defaultUpliftPct || "35");
  planningInputs.forEach((item) => {
    params.set(item.param, planningSettingValue(item.param));
  });
  return params.toString();
}

function setLoading(message = "Loading dashboard...") {
  document.getElementById("workspaceContent").innerHTML = `<div class="loading-state">${escapeHtml(message)}</div>`;
}

function setSubmitState(isLoading) {
  const submitButton = document.getElementById("applyFiltersButton");
  if (!submitButton) return;
  submitButton.disabled = isLoading;
  submitButton.textContent = isLoading ? "Submitting..." : "Submit";
  submitButton.setAttribute("aria-busy", isLoading ? "true" : "false");
}

function setUploadState(isLoading, message = "") {
  const uploadButton = document.getElementById("uploadButton");
  const uploadStatus = document.getElementById("uploadStatus");
  if (uploadButton) {
    uploadButton.disabled = isLoading;
    uploadButton.textContent = isLoading ? "Uploading..." : "Upload Files";
    uploadButton.setAttribute("aria-busy", isLoading ? "true" : "false");
  }
  if (uploadStatus && message) {
    uploadStatus.textContent = message;
  }
}

function renderSourceToggles(sources) {
  const container = document.getElementById("sourceToggles");
  container.innerHTML = sources
    .map(
      (source) => `
        <label class="toggle-pill">
          <input type="checkbox" name="source" value="${escapeHtml(source)}" ${source === "Sales" ? "checked" : ""}>
          <span>${escapeHtml(source)}</span>
        </label>
      `,
    )
    .join("");
}

function renderUploadTargets(targets) {
  const select = document.getElementById("uploadKind");
  if (!select) return;
  select.innerHTML = targets
    .map((target) => `<option value="${escapeHtml(target.value)}">${escapeHtml(target.label)}</option>`)
    .join("");
}

function renderTargetingExpanded() {
  const shell = document.getElementById("targetingBody");
  const button = document.getElementById("targetingToggle");
  if (!shell || !button) return;
  const expanded = !!state.targetingExpanded;
  shell.classList.toggle("targeting-shell-collapsed", !expanded);
  button.setAttribute("aria-expanded", expanded ? "true" : "false");
  const label = button.querySelector(".group-toggle-label");
  if (label) label.textContent = expanded ? "Hide targeting" : "Show targeting";
}

function renderPlanningDefaults(defaults) {
  const productForecasts = {};
  planningInputs.forEach((item) => {
    const productDefault = (defaults.productOverrides || []).find((override) => override.product === item.product);
    productForecasts[item.param] = String(productDefault?.default_uplift_pct ?? defaults.default_uplift_pct ?? 35);
  });
  state.planningSettings = {
    baseline: defaults.baseline || "last_full_month",
    defaultUpliftPct: String(defaults.default_uplift_pct ?? 35),
    baselineStart: "",
    baselineEnd: "",
    productForecasts,
    baselineOptions: defaults.baselineOptions || defaultPlanningBaselineOptions,
  };
}

function renderMeta(meta) {
  document.getElementById("outputSelect").innerHTML = meta.outputDirs
    .map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`)
    .join("");
  document.getElementById("outputSelect").value = meta.defaultOutputDir ?? "";
  document.getElementById("dateBasisSelect").value = "order";
  document.getElementById("orderBucketModeSelect").value = "paid_time";
  document.getElementById("radiusMilesSelect").value = "20";
  document.getElementById("cityRadiusMilesSelect").value = "20";
  renderSourceToggles(meta.availableSources);
  renderUploadTargets(meta.uploadTargets || []);
  renderPlanningDefaults(meta.planningDefaults || {});
  syncDateBounds();
  applyDeploymentModeUi();
  renderTargetingExpanded();
}

function applyDeploymentModeUi() {
  if (!staticModeEnabled()) return;
  document.getElementById("refreshButton").textContent = "Reload snapshot";
  const uploadButton = document.getElementById("uploadButton");
  if (uploadButton) {
    uploadButton.disabled = false;
    uploadButton.textContent = "Upload Files";
  }
  const uploadStatus = document.getElementById("uploadStatus");
  if (uploadStatus) {
    uploadStatus.textContent = "Hosted mode uploads to shared storage, then refreshes the hosted snapshot.";
  }
}

function kpiCard(card) {
  return `
    <button class="kpi-card ${state.activeCard === card.key ? "active" : ""}" data-card-key="${escapeHtml(card.key)}" type="button">
      <div class="kpi-card-topline">
        <div class="kpi-label">${escapeHtml(card.label)}</div>
        <div class="kpi-card-hint">Details</div>
      </div>
      <div class="kpi-value mono">${card.value}</div>
      <div class="kpi-note">${escapeHtml(card.note)}</div>
    </button>
  `;
}

function statChip(label, value, tone = "default") {
  return `<span class="stat-chip tone-${tone}">${escapeHtml(label)}: ${escapeHtml(value)}</span>`;
}

function panelHtml(title, note, body, eyebrow = "Section", actions = "") {
  return `
    <article class="panel">
      <div class="panel-head">
        <div>
          <div class="eyebrow">${escapeHtml(eyebrow)}</div>
          <h3>${escapeHtml(title)}</h3>
          ${note ? `<div class="panel-note">${escapeHtml(note)}</div>` : ""}
        </div>
        ${actions}
      </div>
      ${body}
    </article>
  `;
}

function detailPanelKey(workspaceKey, subtab) {
  return `${workspaceKey}:${subtab}`;
}

function defaultDetailExpanded(workspaceKey, subtab) {
  if (workspaceKey === "products") return true;
  return false;
}

function isDetailExpanded(workspaceKey, subtab) {
  const key = detailPanelKey(workspaceKey, subtab);
  if (Object.prototype.hasOwnProperty.call(state.detailExpanded, key)) {
    return state.detailExpanded[key];
  }
  return defaultDetailExpanded(workspaceKey, subtab);
}

function openDetailPanel(workspaceKey, subtab) {
  if (!workspaceKey || !subtab) return;
  state.detailExpanded[detailPanelKey(workspaceKey, subtab)] = true;
}

function detailToggleActionsHtml(workspaceKey, subtab) {
  const expanded = isDetailExpanded(workspaceKey, subtab);
  return `
    <button
      class="detail-toggle-button"
      type="button"
      data-detail-toggle="${escapeHtml(detailPanelKey(workspaceKey, subtab))}"
      aria-expanded="${expanded ? "true" : "false"}"
    >${expanded ? "Hide details" : "Show details"}</button>
  `;
}

function collapsibleDetailPanelHtml(workspaceKey, subtab, title, note, body, eyebrow = "Details") {
  const expanded = isDetailExpanded(workspaceKey, subtab);
  return panelHtml(
    title,
    note,
    expanded ? body : '<div class="empty-state">Hidden by default. Expand when you need the detailed table.</div>',
    eyebrow,
    detailToggleActionsHtml(workspaceKey, subtab),
  );
}

function panelHeaderActionsHtml(panelKey) {
  if (!panelKey) return "";
  const active = state.chartModes[panelKey] || "bar";
  const modes = [
    ["bar", "Bar"],
    ["pie", "Pie"],
    ["line", "Linear"],
  ];
  return `
    <div class="panel-actions" role="tablist" aria-label="Chart view">
      ${modes
        .map(
          ([mode, label]) => `
            <button
              class="chart-mode-button ${active === mode ? "active" : ""}"
              type="button"
              data-chart-panel="${escapeHtml(panelKey)}"
              data-chart-mode="${escapeHtml(mode)}"
            >${escapeHtml(label)}</button>
          `,
        )
        .join("")}
    </div>
  `;
}

function chartPanelHtml(panelKey, title, note, rows, labelKey, valueKey, formatter, eyebrow = "Section", options = {}) {
  return panelHtml(
    title,
    note,
    categoricalChartHtml(panelKey, rows, labelKey, valueKey, formatter, options),
    eyebrow,
    panelHeaderActionsHtml(panelKey),
  );
}

function workspaceShell(title, note, subtabsHtml, bodyHtml) {
  return `
    <section class="workspace-shell">
      <header class="workspace-header">
        <div>
          <div class="eyebrow">Workspace</div>
          <h2>${escapeHtml(title)}</h2>
          <div class="panel-note">${escapeHtml(note)}</div>
        </div>
        <div class="workspace-subtabs">${subtabsHtml}</div>
      </header>
      <div class="workspace-main">${bodyHtml}</div>
    </section>
  `;
}

function detailListHtml(items) {
  const filtered = items.filter((item) => item && item.label && item.value != null);
  if (!filtered.length) return '<div class="empty-state">No detail is available for this card.</div>';
  return `
    <div class="detail-list">
      ${filtered
        .map(
          (item) => `
            <div class="detail-row">
              <div class="detail-label">${escapeHtml(item.label)}</div>
              <div class="detail-value mono">${item.value}</div>
            </div>
          `,
        )
        .join("")}
    </div>
  `;
}

function detailFormulaHtml(formula, fields = []) {
  return `
    <div class="detail-meta">
      <div class="detail-meta-block">
        <div class="detail-meta-label">Formula</div>
        <div class="detail-meta-value mono">${escapeHtml(formula)}</div>
      </div>
      <div class="detail-meta-block">
        <div class="detail-meta-label">Fields Used</div>
        <div class="detail-meta-value">${fields.map((field) => `<code>${escapeHtml(field)}</code>`).join(" ")}</div>
      </div>
    </div>
  `;
}

function customerProxyFieldMix(orderSummary) {
  const fields = [orderSummary?.customer_id_basis || "Buyer Username -> Buyer Nickname -> Recipient"];
  const parts = [];
  if (orderSummary?.customer_proxy_username_pct != null) {
    parts.push(`Buyer Username ${fmtPercent(orderSummary.customer_proxy_username_pct)} (${fmtNumber(orderSummary.customer_proxy_username_count)})`);
  }
  if (orderSummary?.customer_proxy_nickname_pct != null) {
    parts.push(`Buyer Nickname ${fmtPercent(orderSummary.customer_proxy_nickname_pct)} (${fmtNumber(orderSummary.customer_proxy_nickname_count)})`);
  }
  if (orderSummary?.customer_proxy_recipient_pct != null) {
    parts.push(`Recipient ${fmtPercent(orderSummary.customer_proxy_recipient_pct)} (${fmtNumber(orderSummary.customer_proxy_recipient_count)})`);
  }
  if (parts.length) fields.push(...parts);
  return fields;
}

function detailActionHtml(workspace, subtab, label = "Open related workspace") {
  if (!workspace) return "";
  return `<button class="button button-secondary detail-action" type="button" data-detail-workspace="${escapeHtml(workspace)}" data-detail-subtab="${escapeHtml(subtab || "")}">${escapeHtml(label)}</button>`;
}

function cardDetailHtml(cardKey, payload) {
  const orderSummary = payload.orderSummary || {};
  const statementSummary = payload.statementSummary || {};
  const reconciliationSummary = payload.reconciliationSummary || {};
  const feeRows = payload.statementFeeBreakdownRows || [];
  const radiusSummary = payload.radiusSummary || {};
  const targetCitySummary = payload.targetCitySummary || {};

  const detailTable = (rows, columns, caption) => tableHtml(rows || [], columns, caption);

  if (cardKey === "orders-gross-sales") {
    return panelHtml(
      "Orders Gross Sales",
      "Paid-time order-export view before discounts and refunds.",
      `
        ${detailFormulaHtml("Gross Product Sales = SUM(SKU Subtotal Before Discount)", ["SKU Subtotal Before Discount", "Paid Time"])}
        ${detailListHtml([
          { label: "Gross product sales", value: fmtCurrency(orderSummary.orders_gross_product_sales) },
          { label: "Seller discount", value: fmtCurrency(orderSummary.orders_seller_discount) },
          { label: "Platform discount", value: fmtCurrency(orderSummary.orders_platform_discount) },
          { label: "After seller discount", value: fmtCurrency(orderSummary.product_sales_after_seller_discount) },
          { label: "After all discounts", value: fmtCurrency(orderSummary.product_sales_after_all_discounts) },
        ])}
        ${detailActionHtml("orders", "order-daily")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "orders-net-product-sales") {
    return panelHtml(
      "Orders Net Product Sales",
      "Order-export proxy net sales for the selected paid-time window.",
      `
        ${detailFormulaHtml("Net Product Sales = Gross Product Sales - Seller Discount - Export Refund Amount", ["SKU Subtotal Before Discount", "SKU Seller Discount", "Order Refund Amount"])}
        ${detailListHtml([
          { label: "Gross product sales", value: fmtCurrency(orderSummary.orders_gross_product_sales) },
          { label: "Less seller discount", value: fmtCurrency(orderSummary.orders_seller_discount) },
          { label: "Less export refund amount", value: fmtCurrency(orderSummary.orders_refund_amount) },
          { label: "Net product sales", value: fmtCurrency(orderSummary.orders_net_product_sales) },
        ])}
        ${detailActionHtml("orders", "order-daily")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "orders-paid-orders") {
    return panelHtml(
      "Orders Paid Orders",
      "Unique order count in the paid-time order slice.",
      `
        ${detailFormulaHtml("Paid Orders = COUNT(DISTINCT Order ID)", ["Order ID", "Paid Time"])}
        ${detailListHtml([
          { label: "Paid orders", value: fmtNumber(orderSummary.orders_paid_orders) },
          { label: "Sales units", value: fmtNumber(orderSummary.sales_units) },
          { label: "Average units per paid order", value: orderSummary.orders_paid_orders ? fmtNumber(orderSummary.sales_units / orderSummary.orders_paid_orders) : "N/A" },
        ])}
        ${detailActionHtml("orders", "order-daily")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "orders-aov") {
    return panelHtml(
      "Average Order Value",
      "Gross product sales divided by paid orders in the selected paid-time slice.",
      `
        ${detailFormulaHtml("AOV = Gross Product Sales / Paid Orders", ["SKU Subtotal Before Discount", "Order ID"])}
        ${detailListHtml([
          { label: "Gross product sales", value: fmtCurrency(orderSummary.orders_gross_product_sales) },
          { label: "Paid orders", value: fmtNumber(orderSummary.orders_paid_orders) },
          { label: "AOV", value: fmtCurrency(orderSummary.orders_aov) },
        ])}
        ${detailActionHtml("orders", "order-daily")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "units-sold") {
    return panelHtml(
      "Units Sold",
      "Units sold from sales orders. Samples and replacements are shown separately.",
      `
        ${detailFormulaHtml("Units Sold = SUM(Quantity) for source_type = Sales", ["Quantity", "source_type"])}
        ${detailListHtml([
          { label: "Units sold", value: fmtNumber(orderSummary.sales_units) },
          { label: "Units per paid order", value: orderSummary.orders_paid_orders ? (orderSummary.sales_units == null ? "N/A" : Number(orderSummary.sales_units / orderSummary.orders_paid_orders).toFixed(2)) : "N/A" },
          { label: "Sample units", value: fmtNumber(orderSummary.sample_units) },
          { label: "Replacement units", value: fmtNumber(orderSummary.replacement_units) },
          { label: "Operational units", value: fmtNumber(orderSummary.operational_units) },
        ])}
        ${detailActionHtml("products", "products")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "unique-customers") {
    return panelHtml(
      "Unique Customers",
      "Distinct customer proxies in the selected paid-time slice.",
      `
        ${detailFormulaHtml("Unique Customers = COUNT(DISTINCT customer proxy)", customerProxyFieldMix(orderSummary))}
        ${detailListHtml([
          { label: "Unique customers", value: fmtNumber(orderSummary.selected_unique_customers) },
          { label: "New customers", value: fmtNumber(orderSummary.selected_first_time_buyers) },
          { label: "Repeat customers", value: fmtNumber(orderSummary.selected_repeat_customers) },
          { label: "Customer proxy basis", value: escapeHtml(orderSummary.customer_id_basis) },
        ])}
        ${detailActionHtml("customers", "cohorts")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "first-time-buyers") {
    return panelHtml(
      "New Customers",
      "Customer proxies whose first observed valid order in loaded history falls in this slice.",
      `
        ${detailFormulaHtml("New Customers = COUNT(DISTINCT selected-period customer proxy where first observed valid order date in available history falls in the selected period)", customerProxyFieldMix(orderSummary))}
        ${detailListHtml([
          { label: "New customers", value: fmtNumber(orderSummary.selected_first_time_buyers) },
          { label: "Repeat customers", value: fmtNumber(orderSummary.selected_returning_customers) },
          { label: "First-time buyer rate", value: fmtPercent(orderSummary.selected_first_time_buyer_rate) },
          { label: "Customer proxy basis", value: escapeHtml(orderSummary.customer_id_basis) },
        ])}
        ${detailActionHtml("customers", "cohorts")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "repeat-customers") {
    return panelHtml(
      "Repeat Customers",
      "Customer proxies whose first observed valid order happened before this slice and who ordered again in this slice.",
      `
        ${detailFormulaHtml("Repeat Customers = COUNT(DISTINCT customer proxy whose first observed valid order date is before the selected period and who has at least one valid order in the selected period)", customerProxyFieldMix(orderSummary))}
        ${detailListHtml([
          { label: "Repeat customers", value: fmtNumber(orderSummary.selected_repeat_customers) },
          { label: "Unique customers", value: fmtNumber(orderSummary.selected_unique_customers) },
          { label: "Repeat customer rate", value: fmtPercent(orderSummary.selected_repeat_customer_rate) },
          { label: "Customer proxy basis", value: escapeHtml(orderSummary.customer_id_basis) },
        ])}
        ${detailActionHtml("customers", "cohorts")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "finance-gross-sales") {
    return panelHtml(
      "Finance Gross Sales",
      "Settlement-month finance view by statement date.",
      `
        ${detailFormulaHtml("Finance Gross Sales = SUM(Gross sales)", ["Gross sales", "Statement Date"])}
        ${detailListHtml([
          { label: "Gross sales", value: fmtCurrency(statementSummary.finance_gross_sales) },
          { label: "Gross sales refund", value: fmtCurrency(statementSummary.finance_gross_sales_refund) },
          { label: "Seller discount", value: fmtCurrency(statementSummary.finance_seller_discount) },
          { label: "Seller discount refund", value: fmtCurrency(statementSummary.finance_seller_discount_refund) },
          { label: "Net sales", value: fmtCurrency(statementSummary.finance_net_sales) },
        ])}
        ${detailActionHtml("finance", "statement-daily")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "finance-net-sales") {
    return panelHtml(
      "Finance Net Sales",
      "TikTok finance statement net sales for the settlement month.",
      `
        ${detailFormulaHtml("Net Sales comes from the finance statement export; compare it against gross sales, refunds, and seller discounts.", ["Net sales", "Gross sales", "Gross sales refund", "Seller discount", "Seller discount refund"])}
        ${detailListHtml([
          { label: "Gross sales", value: fmtCurrency(statementSummary.finance_gross_sales) },
          { label: "Gross sales refund", value: fmtCurrency(statementSummary.finance_gross_sales_refund) },
          { label: "Seller discount", value: fmtCurrency(statementSummary.finance_seller_discount) },
          { label: "Seller discount refund", value: fmtCurrency(statementSummary.finance_seller_discount_refund) },
          { label: "Net sales", value: fmtCurrency(statementSummary.finance_net_sales) },
        ])}
        ${detailActionHtml("finance", "statement-daily")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "finance-payout-amount") {
    return panelHtml(
      "Finance Payout Amount",
      "Settlement amount recognized in the statement month.",
      `
        ${detailFormulaHtml("Payout Amount = Total settlement amount from the finance statement export", ["Total settlement amount", "Net sales", "Shipping", "Fees", "Adjustment amount"])}
        ${detailListHtml([
          { label: "Payout amount", value: fmtCurrency(statementSummary.finance_payout_amount) },
          { label: "Net sales", value: fmtCurrency(statementSummary.finance_net_sales) },
          { label: "Shipping", value: fmtCurrency(statementSummary.finance_shipping_total) },
          { label: "Fees", value: fmtCurrency(statementSummary.finance_fees_total) },
          { label: "Adjustments", value: fmtCurrency(statementSummary.finance_adjustments_total) },
        ])}
        ${detailActionHtml("finance", "statement-daily")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "finance-fees") {
    return panelHtml(
      "Finance Fees",
      "TikTok-style expense categories plus the underlying finance statement buckets.",
      `
        ${detailFormulaHtml("Expense Structure groups statement rows into Shipping, Product discount, Affiliate, Operation, and Marketing using the finance export buckets.", ["Seller discount", "Seller discount refund", "Affiliate Commission", "Referral fee", "Campaign service fee", "Smart Promotion fee", "FBT fulfillment fee"])}
        ${detailListHtml([
          { label: "Statement fee total", value: fmtCurrency(statementSummary.finance_fees_total) },
          { label: "Shipping total", value: fmtCurrency(statementSummary.finance_shipping_total) },
        ])}
        ${detailTable(
          payload.expenseStructureRows || [],
          [
            { key: "category", label: "Category" },
            { key: "amount", label: "Amount", format: fmtCurrency, numeric: true, mono: true },
          ],
          "Expense structure by TikTok-style category",
        )}
        ${detailTable(
          payload.expenseStructureDetailRows || feeRows,
          [
            { key: "category", label: "Category" },
            { key: "line_item", label: "Line Item" },
            { key: "fee_bucket", label: "Fee Bucket" },
            { key: "amount", label: "Amount", format: fmtCurrency, numeric: true, mono: true },
          ],
          "Underlying finance statement lines",
        )}
        ${detailActionHtml("finance", "statement-daily")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "matched-orders") {
    return panelHtml(
      "Matched Orders",
      "Orders that joined from the order export back to finance by Order ID.",
      `
        ${detailFormulaHtml("Matched Orders = COUNT(DISTINCT Order ID) where order export and statement export both contain the same Order ID", ["Order ID"])}
        ${detailListHtml([
          { label: "Matched orders", value: fmtNumber(reconciliationSummary.matched_orders) },
          { label: "Matched statement amount", value: fmtCurrency(reconciliationSummary.statement_amount_total) },
          { label: "Matched fee total", value: fmtCurrency(reconciliationSummary.actual_fee_total) },
          { label: "Matched fulfillment fees", value: fmtCurrency(reconciliationSummary.fulfillment_fee_total) },
        ])}
        ${detailActionHtml("reconciliation", "reconciliation")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "unmatched-statement-rows") {
    return panelHtml(
      "Unmatched Statement Rows",
      "Statement rows that did not find a matching order-export Order ID in the selected slice.",
      `
        ${detailFormulaHtml("Unmatched Statement Rows = COUNT(statement rows with no matched Order ID)", ["Order ID", "Statement Date"])}
        ${detailListHtml([
          { label: "Unmatched statement rows", value: fmtNumber(reconciliationSummary.unmatched_statement_rows) },
          { label: "Unmatched orders", value: fmtNumber(reconciliationSummary.unmatched_orders) },
        ])}
        ${detailActionHtml("reconciliation", "reconciliation")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "repeat-customer-rate") {
    return panelHtml(
      "Repeat Customer Rate",
      "Repeat customers divided by unique customers in the selected paid-time slice.",
      `
        ${detailFormulaHtml("Repeat Customer Rate = Repeat Customers / Unique Customers", customerProxyFieldMix(orderSummary))}
        ${detailListHtml([
          { label: "Unique customers", value: fmtNumber(orderSummary.selected_unique_customers) },
          { label: "Repeat customers", value: fmtNumber(orderSummary.selected_repeat_customers) },
          { label: "Repeat customer rate", value: fmtPercent(orderSummary.selected_repeat_customer_rate) },
          { label: "Customer proxy basis", value: escapeHtml(orderSummary.customer_id_basis) },
        ])}
        ${detailActionHtml("customers", "cohorts")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "customers-within-radius") {
    return panelHtml(
      "Customers Within Radius",
      "Customer concentration around the selected ZIP and mile radius.",
      `
        ${detailFormulaHtml("Counts customers and orders where customer ZIP centroid is within the selected radius from the target ZIP.", ["ZIP", "Radius miles"])}
        ${detailListHtml([
          { label: "Customers within radius", value: fmtNumber(radiusSummary.customers_within_radius) },
          { label: "Orders within radius", value: fmtNumber(radiusSummary.orders_within_radius) },
          { label: "Target ZIP", value: escapeHtml(payload.summary?.target_zip || "") },
        ])}
        ${detailActionHtml("customers", "radius")}
      `,
      "Card detail",
    );
  }

  if (cardKey === "customers-in-target-city") {
    return panelHtml(
      "Customers In Target City",
      "Exact city-match customer targeting for the selected order slice.",
      `
        ${detailFormulaHtml("Counts unique customers and orders where normalized city and optional state match the selected target.", ["Recipient City", "Recipient State"])}
        ${detailListHtml([
          { label: "Customers in city", value: fmtNumber(targetCitySummary.customers_in_city) },
          { label: "Orders in city", value: fmtNumber(targetCitySummary.orders_in_city) },
          { label: "ZIPs represented", value: fmtNumber(targetCitySummary.zip_rows_in_city) },
        ])}
        ${detailActionHtml("customers", "target-city")}
      `,
      "Card detail",
    );
  }

  return panelHtml(
    "Card Details",
    "Select a KPI card to inspect its components and source fields.",
    '<div class="empty-state">Click any KPI card above to inspect how it is calculated.</div>',
    "Card detail",
  );
}

function renderSummary(summary) {
  const orderSummary = state.payload?.orderSummary || {};
  const dataQuality = state.payload?.dataQualitySummary || {};
  const orderBucketMode = summary.order_bucket_mode === "file_month" ? "Raw Export File Month" : "Paid Time";
  const metaHtml = [
    staticModeEnabled() ? statChip("Deploy Mode", "Hosted snapshot", "accent") : "",
    state.meta?.snapshotGeneratedAt ? statChip("Snapshot", new Date(state.meta.snapshotGeneratedAt).toLocaleString(), "accent") : "",
    staticModeEnabled() ? statChip("Hosted filters", "Date range + planning enabled", "accent") : "",
    statChip("Output", state.payload?.selectedOutputDir || "analysis_output", "primary"),
    statChip("Sources", summary.selected_sources.join(", "), "primary"),
    statChip("Orders basis", "Paid time", "primary"),
    statChip("Order bucket", orderBucketMode, "primary"),
    statChip("Finance basis", "Statement date", "primary"),
    statChip("Customer proxy", "Username -> Nickname -> Recipient", "muted"),
    summary.target_zip ? statChip("Target ZIP", `${summary.target_zip} / ${summary.radius_miles} mi`, "accent") : "",
    summary.target_city ? statChip("Target City", `${summary.target_city}${summary.target_state ? `, ${summary.target_state}` : ""}`, "accent") : "",
  ]
    .filter(Boolean)
    .join("");
  document.getElementById("summaryMeta").innerHTML = metaHtml;
  document.getElementById("dataQualityBanner").innerHTML = [
    statChip("Data Quality", dataQuality.status || "N/A", dataQuality.status === "material" ? "accent" : dataQuality.status === "minor" ? "accent" : "primary"),
    statChip("Mode", orderBucketMode, "muted"),
    statChip("Mismatch", `${fmtNumber(dataQuality.mismatch_units_under_current_mode)} units`, "muted"),
    statChip("Mismatch %", fmtPercent(dataQuality.mismatch_pct_under_current_mode), "muted"),
    statChip("Blank Paid Time", fmtNumber(dataQuality.inferred_units), "muted"),
    statChip("Cross-Month Rows", fmtNumber(dataQuality.spillover_rows), "muted"),
  ].join("");

  const cards = [
    { key: "orders-gross-sales", label: "Gross Product Sales", value: fmtCurrency(orderSummary.orders_gross_product_sales), note: "Merchandise before discounts and refunds in the selected paid-time slice" },
    { key: "orders-net-product-sales", label: "Net Product Sales", value: fmtCurrency(orderSummary.orders_net_product_sales), note: "Gross product sales minus seller discount and export refund amount" },
    { key: "orders-aov", label: "AOV", value: fmtCurrency(orderSummary.orders_aov), note: "Gross product sales divided by paid orders in the selected paid-time slice" },
    { key: "orders-paid-orders", label: "Paid Orders", value: fmtNumber(orderSummary.orders_paid_orders), note: "Unique paid orders in the selected paid-time slice" },
    { key: "units-sold", label: "Units Sold", value: fmtNumber(orderSummary.sales_units), note: "Sales units in the selected paid-time slice; samples and replacements excluded" },
  ];

  if (orderSummary.selected_unique_customers != null) {
    cards.push(
      { key: "unique-customers", label: "Unique Customers", value: fmtNumber(orderSummary.selected_unique_customers), note: "Distinct customer proxies in the selected paid-time slice" },
      { key: "first-time-buyers", label: "New Customers", value: fmtNumber(orderSummary.selected_first_time_buyers), note: "Customer proxies whose first observed valid order in loaded history falls in this slice" },
      { key: "repeat-customers", label: "Repeat Customers", value: fmtNumber(orderSummary.selected_repeat_customers), note: "Customer proxies whose first observed valid order date is before this slice and who placed an order in this slice" },
      { key: "repeat-customer-rate", label: "Repeat Customer Rate", value: fmtPercent(orderSummary.selected_repeat_customer_rate), note: "Repeat customers divided by unique customers in the selected paid-time slice" },
    );
  }

  if (summary.target_zip) {
    cards.push(
      {
        key: "customers-within-radius",
        label: `Customers Within ${summary.radius_miles} Mi`,
        value: fmtNumber(state.payload?.radiusSummary?.customers_within_radius),
        note: `Orders: ${fmtNumber(state.payload?.radiusSummary?.orders_within_radius)}`,
      },
    );
  }

  if (summary.target_city) {
    cards.push(
      {
        key: "customers-in-target-city",
        label: "Customers In Target City",
        value: fmtNumber(state.payload?.targetCitySummary?.customers_in_city),
        note: `Orders: ${fmtNumber(state.payload?.targetCitySummary?.orders_in_city)}`,
      },
    );
  }

  if (!cards.some((card) => card.key === state.activeCard)) {
    state.activeCard = cards[0]?.key || null;
  }

  document.getElementById("kpiGrid").innerHTML = cards.map((card) => kpiCard(card)).join("");
  document.getElementById("kpiDetail").innerHTML = cardDetailHtml(state.activeCard, state.payload);

  document.querySelectorAll(".kpi-card").forEach((card) => {
    card.addEventListener("click", () => {
      state.activeCard = card.dataset.cardKey;
      renderSummary(summary);
    });
  });

  document.querySelectorAll(".detail-action").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeWorkspace = button.dataset.detailWorkspace;
      if (button.dataset.detailSubtab) {
        state.activeSubtabs[state.activeWorkspace] = button.dataset.detailSubtab;
      }
      renderSummary(summary);
      renderWorkspaceNav();
      renderWorkspace();
    });
  });
}

function aggregateByDate(rows, keys) {
  const map = new Map();
  rows.forEach((row) => {
    const date = row.reporting_date;
    if (!date) return;
    if (!map.has(date)) map.set(date, { date });
    const target = map.get(date);
    keys.forEach((key) => {
      target[key] = (target[key] || 0) + (row[key] || 0);
    });
  });
  return [...map.values()].sort((a, b) => a.date.localeCompare(b.date));
}

function financeChartHtml(rows, grossKey = "gross_product_sales", netKey = "net_product_sales", grossLabel = "Gross", netLabel = "Net") {
  if (!rows.length) {
    return '<div class="empty-state">No finance rows for the selected filters.</div>';
  }

  const width = 1000;
  const height = 320;
  const margin = { top: 20, right: 24, bottom: 38, left: 68 };
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  const daily = aggregateByDate(rows, [grossKey, netKey]);
  const maxValue = Math.max(...daily.flatMap((row) => [row[grossKey] || 0, row[netKey] || 0]), 1);
  const xStep = daily.length > 1 ? innerW / (daily.length - 1) : innerW;
  const y = (value) => margin.top + innerH - (value / maxValue) * innerH;
  const x = (index) => margin.left + index * xStep;
  const linePath = (key) =>
    daily
      .map((row, index) => `${index === 0 ? "M" : "L"} ${x(index).toFixed(2)} ${y(row[key] || 0).toFixed(2)}`)
      .join(" ");
  const areaPath = (key) => {
    const line = linePath(key);
    return `${line} L ${x(daily.length - 1).toFixed(2)} ${(margin.top + innerH).toFixed(2)} L ${x(0).toFixed(2)} ${(margin.top + innerH).toFixed(2)} Z`;
  };

  const grid = Array.from({ length: 5 }, (_, i) => {
    const value = (maxValue / 4) * i;
    const yy = y(value);
    return `
      <line class="grid-line" x1="${margin.left}" y1="${yy}" x2="${width - margin.right}" y2="${yy}"></line>
      <text class="axis-text" x="8" y="${yy + 4}">${fmtCurrency(value)}</text>
    `;
  }).join("");

  const xLabels = daily
    .filter((_, index) => index === 0 || index === daily.length - 1 || index % Math.max(1, Math.ceil(daily.length / 6)) === 0)
    .map((row) => {
      const actualIndex = daily.findIndex((item) => item.date === row.date);
      return `<text class="axis-text" x="${x(actualIndex)}" y="${height - 10}" text-anchor="middle">${row.date.slice(5)}</text>`;
    })
    .join("");

  return `
    <div class="chart-frame">
      <svg viewBox="0 0 ${width} ${height}" class="svg-chart" role="img" aria-label="Finance trend">
        <defs>
          <linearGradient id="grossFill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stop-color="#3b82f6" stop-opacity="0.35"></stop>
            <stop offset="100%" stop-color="#3b82f6" stop-opacity="0.02"></stop>
          </linearGradient>
          <linearGradient id="netFill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stop-color="#f59e0b" stop-opacity="0.26"></stop>
            <stop offset="100%" stop-color="#f59e0b" stop-opacity="0.02"></stop>
          </linearGradient>
        </defs>
        ${grid}
        <path d="${areaPath(grossKey)}" fill="url(#grossFill)"></path>
        <path d="${areaPath(netKey)}" fill="url(#netFill)"></path>
        <path d="${linePath(grossKey)}" fill="none" stroke="#1e40af" stroke-width="3"></path>
        <path d="${linePath(netKey)}" fill="none" stroke="#f59e0b" stroke-width="3"></path>
        ${daily
          .map(
            (row, index) => `
              <circle cx="${x(index)}" cy="${y(row[grossKey] || 0)}" r="3" fill="#1e40af"></circle>
              <circle cx="${x(index)}" cy="${y(row[netKey] || 0)}" r="3" fill="#f59e0b"></circle>
            `,
          )
          .join("")}
        ${xLabels}
      </svg>
      <div class="table-toolbar">
        <div class="table-caption">${escapeHtml(grossLabel)} vs ${escapeHtml(netLabel)} by day.</div>
        <div>
          ${statChip(grossLabel, "Blue", "primary")}
          ${statChip(netLabel, "Amber", "accent")}
        </div>
      </div>
    </div>
  `;
}

function chartPercent(value, total) {
  return total ? fmtPercent(value / total) : "0.0%";
}

function barListHtml(rows, labelKey, valueKey, formatter = fmtNumber, green = false) {
  if (!rows.length) return '<div class="empty-state">No rows for the current filters.</div>';
  const max = Math.max(...rows.map((row) => Math.abs(row[valueKey] || 0)), 1);
  const total = rows.reduce((sum, row) => sum + Math.abs(Number(row[valueKey] || 0)), 0);
  return `
    <div class="bar-list">
      ${rows
        .map(
          (row) => `
            <div class="bar-row">
              <div class="bar-meta">
                <span>${escapeHtml(row[labelKey])}</span>
                <strong class="mono">${formatter(row[valueKey])} <span class="chart-share">${chartPercent(Math.abs(Number(row[valueKey] || 0)), total)}</span></strong>
              </div>
              <div class="bar-track"><div class="bar-fill ${green ? "green" : ""} ${(row[valueKey] || 0) < 0 ? "negative" : ""}" style="width:${(Math.abs(row[valueKey] || 0) / max) * 100}%"></div></div>
            </div>
          `,
        )
        .join("")}
      </div>
    `;
}

function normalizedChartRows(rows, labelKey, valueKey) {
  return (rows || [])
    .map((row) => ({
      label: String(row?.[labelKey] ?? ""),
      rawValue: Number(row?.[valueKey] || 0),
      value: Math.abs(Number(row?.[valueKey] || 0)),
    }))
    .filter((row) => row.label && Number.isFinite(row.value));
}

function categoricalLineHtml(rows, formatter = fmtNumber) {
  if (!rows.length) return '<div class="empty-state">No rows for the current filters.</div>';
  const width = 1000;
  const height = 280;
  const margin = { top: 24, right: 24, bottom: 72, left: 68 };
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  const maxValue = Math.max(...rows.map((row) => row.value), 1);
  const xStep = rows.length > 1 ? innerW / (rows.length - 1) : innerW;
  const x = (index) => margin.left + index * xStep;
  const y = (value) => margin.top + innerH - (value / maxValue) * innerH;
  const path = rows
    .map((row, index) => `${index === 0 ? "M" : "L"} ${x(index).toFixed(2)} ${y(row.value).toFixed(2)}`)
    .join(" ");
  const grid = Array.from({ length: 5 }, (_, index) => {
    const value = (maxValue / 4) * index;
    const yy = y(value);
    return `
      <line class="grid-line" x1="${margin.left}" y1="${yy}" x2="${width - margin.right}" y2="${yy}"></line>
      <text class="axis-text" x="8" y="${yy + 4}">${formatter(value)}</text>
    `;
  }).join("");
  const xLabels = rows
    .map(
      (row, index) => `
        <text class="axis-text" x="${x(index)}" y="${height - 16}" text-anchor="end" transform="rotate(-28 ${x(index)} ${height - 16})">
          ${escapeHtml(row.label.length > 22 ? `${row.label.slice(0, 22)}…` : row.label)}
        </text>
      `,
    )
    .join("");
  return `
    <div class="chart-frame chart-frame-compact">
      <svg viewBox="0 0 ${width} ${height}" class="svg-chart" role="img" aria-label="Line chart">
        ${grid}
        <path d="${path}" fill="none" stroke="#1e40af" stroke-width="3"></path>
        ${rows
          .map(
            (row, index) => `
              <circle cx="${x(index)}" cy="${y(row.value)}" r="4" fill="#1e40af"></circle>
              <title>${escapeHtml(row.label)}: ${formatter(row.rawValue)}</title>
            `,
          )
          .join("")}
        ${xLabels}
      </svg>
    </div>
  `;
}

function pieChartHtml(rows, formatter = fmtNumber) {
  if (!rows.length) return '<div class="empty-state">No rows for the current filters.</div>';
  const total = rows.reduce((sum, row) => sum + row.value, 0);
  if (!total) return '<div class="empty-state">No non-zero rows for the current filters.</div>';
  const cx = 150;
  const cy = 150;
  const radius = 108;
  const innerRadius = 58;
  const palette = ["#1e40af", "#3b82f6", "#0f766e", "#14b8a6", "#f59e0b", "#f97316", "#8b5cf6", "#64748b", "#06b6d4", "#84cc16"];
  let startAngle = -Math.PI / 2;
  const arcs = rows
    .map((row, index) => {
      const slice = (row.value / total) * Math.PI * 2;
      const endAngle = startAngle + slice;
      const largeArc = slice > Math.PI ? 1 : 0;
      const x1 = cx + radius * Math.cos(startAngle);
      const y1 = cy + radius * Math.sin(startAngle);
      const x2 = cx + radius * Math.cos(endAngle);
      const y2 = cy + radius * Math.sin(endAngle);
      const ix1 = cx + innerRadius * Math.cos(endAngle);
      const iy1 = cy + innerRadius * Math.sin(endAngle);
      const ix2 = cx + innerRadius * Math.cos(startAngle);
      const iy2 = cy + innerRadius * Math.sin(startAngle);
      const path = [
        `M ${x1} ${y1}`,
        `A ${radius} ${radius} 0 ${largeArc} 1 ${x2} ${y2}`,
        `L ${ix1} ${iy1}`,
        `A ${innerRadius} ${innerRadius} 0 ${largeArc} 0 ${ix2} ${iy2}`,
        "Z",
      ].join(" ");
      startAngle = endAngle;
      return `
        <path d="${path}" fill="${palette[index % palette.length]}" stroke="#ffffff" stroke-width="2">
          <title>${escapeHtml(row.label)}: ${formatter(row.rawValue)} (${chartPercent(row.value, total)})</title>
        </path>
      `;
    })
    .join("");
  return `
    <div class="pie-layout">
      <div class="pie-chart-wrap">
        <svg viewBox="0 0 300 300" class="pie-chart" role="img" aria-label="Pie chart">
          ${arcs}
          <circle cx="${cx}" cy="${cy}" r="${innerRadius - 2}" fill="#ffffff"></circle>
          <text x="${cx}" y="${cy - 6}" text-anchor="middle" class="pie-total-label">Total</text>
          <text x="${cx}" y="${cy + 20}" text-anchor="middle" class="pie-total-value">${formatter(total)}</text>
        </svg>
      </div>
      <div class="pie-legend">
        ${rows
          .map(
            (row, index) => `
              <div class="pie-legend-row">
                <span class="pie-swatch" style="background:${palette[index % palette.length]}"></span>
                <span class="pie-legend-label">${escapeHtml(row.label)}</span>
                <span class="pie-legend-value mono">${formatter(row.rawValue)} <span class="chart-share">${chartPercent(row.value, total)}</span></span>
              </div>
            `,
          )
          .join("")}
      </div>
    </div>
  `;
}

function categoricalChartHtml(panelKey, rows, labelKey, valueKey, formatter = fmtNumber, options = {}) {
  const normalizedRows = normalizedChartRows(rows, labelKey, valueKey);
  if (!normalizedRows.length) return '<div class="empty-state">No rows for the current filters.</div>';
  const mode = state.chartModes[panelKey] || "bar";
  if (mode === "pie") {
    return pieChartHtml(normalizedRows, formatter);
  }
  if (mode === "line") {
    return categoricalLineHtml(normalizedRows, formatter);
  }
  return barListHtml(rows, labelKey, valueKey, formatter, options.green);
}

function cohortHeatmapHtml(rows) {
  if (!rows.length) {
    return '<div class="empty-state">Not enough customer-id data is available for cohorts in this window.</div>';
  }
  const periods = [...new Set(rows.flatMap((row) => Object.keys(row.values)))].sort((a, b) => Number(a) - Number(b));
  const header = periods.map((period) => `<th>M${period}</th>`).join("");
  const heatColor = (value) => {
    if (value == null || value <= 0) return "#eff6ff";
    if (value >= 0.6) return "#1e40af";
    if (value >= 0.45) return "#2563eb";
    if (value >= 0.3) return "#60a5fa";
    if (value >= 0.15) return "#bfdbfe";
    return "#dbeafe";
  };
  const textColor = (value) => (value >= 0.3 ? "#ffffff" : "#10213d");
  const body = rows
    .map((row) => {
      const cells = periods
        .map((period) => {
          const value = row.values[period] ?? 0;
          return `<td class="heat-cell" style="background:${heatColor(value)}; color:${textColor(value)}">${fmtPercent(value)}</td>`;
        })
        .join("");
      return `<tr><td class="cohort-stub">${escapeHtml(row.cohort)}</td>${cells}</tr>`;
    })
    .join("");
  return `
    <div class="cohort-legend">
      <span>Lower retention</span>
      <div class="cohort-legend-bar"></div>
      <span>Higher retention</span>
    </div>
    <table class="cohort-table">
      <thead><tr><th class="cohort-stub">Cohort</th>${header}</tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function tableHtml(rows, columns, caption, options = {}) {
  if (!rows.length) return '<div class="empty-state">No rows for the current filters.</div>';
  const head = columns
    .map((col, index) => {
      const classes = [col.numeric ? "num" : "", options.stickyFirstColumn && index === 0 ? "sticky-first-col" : ""].filter(Boolean).join(" ");
      return `<th class="${classes}">${escapeHtml(col.label)}</th>`;
    })
    .join("");
  const body = rows
    .map(
      (row) =>
        `<tr>${columns
          .map((col, index) => {
            const value = col.format ? col.format(row[col.key]) : escapeHtml(row[col.key]);
            const classes = [
              col.numeric ? "num" : "",
              col.mono ? "mono" : "",
              options.stickyFirstColumn && index === 0 ? "sticky-first-col" : "",
            ]
              .filter(Boolean)
              .join(" ");
            return `<td class="${classes}">${value}</td>`;
          })
          .join("")}</tr>`,
      )
      .join("");
  const wrapClass = ["table-wrap", options.compact ? "table-wrap-compact" : "", options.tall ? "table-wrap-tall" : ""]
    .filter(Boolean)
    .join(" ");
  const tableClass = ["data-table", options.compact ? "compact-table" : ""].filter(Boolean).join(" ");
  return `
      <div class="table-toolbar">
        <div class="table-caption">${escapeHtml(caption)}</div>
        <div>${statChip("Rows", fmtNumber(rows.length), "muted")}</div>
      </div>
    <div class="${wrapClass}">
        <table class="${tableClass}">
          <thead><tr>${head}</tr></thead>
          <tbody>${body}</tbody>
        </table>
      </div>
    `;
}

function subtabButtonsHtml(workspaceKey) {
  const workspace = workspaces.find((item) => item.key === workspaceKey);
  return workspace.subtabs
    .map(
      ([key, label]) =>
        `<button class="subtab-button ${state.activeSubtabs[workspaceKey] === key ? "active" : ""}" data-workspace="${workspaceKey}" data-subtab="${key}">${escapeHtml(label)}</button>`,
    )
    .join("");
}

function workspaceButtonsHtml() {
  return workspaces
    .map(
      (workspace) =>
        `<button class="workspace-button ${state.activeWorkspace === workspace.key ? "active" : ""}" data-workspace="${workspace.key}">${escapeHtml(workspace.label)}</button>`,
    )
    .join("");
}

function metricsStackHtml(items) {
  return `<div class="metrics-stack">${items
    .map(
      (item) => `
        <div class="metric-row">
          <div class="metric-label">${escapeHtml(item.label)}</div>
          <div class="metric-value">${item.value}</div>
        </div>
      `,
    )
    .join("")}</div>`;
}

function hostedFallback(value, label = "Hosted summary only") {
  return value == null ? label : value;
}

function planningLabel(product) {
  return product.replace(" Bomb 2-Pack", "").replace(" Bomb", "");
}

function syncPlanningStateFromDom(requireCustomDates = false) {
  const baselineSelect = document.getElementById("planningTabBaselineSelect");
  const defaultUpliftInput = document.getElementById("planningTabDefaultUplift");
  const baselineStartInput = document.getElementById("planningTabBaselineStart");
  const baselineEndInput = document.getElementById("planningTabBaselineEnd");

  if (baselineSelect) state.planningSettings.baseline = baselineSelect.value || "last_full_month";
  if (defaultUpliftInput) state.planningSettings.defaultUpliftPct = defaultUpliftInput.value || "35";
  state.planningSettings.baselineStart = baselineStartInput?.value || "";
  state.planningSettings.baselineEnd = baselineEndInput?.value || "";

  planningInputs.forEach((item) => {
    const input = document.querySelector(`[data-planning-param="${item.param}"]`);
    state.planningSettings.productForecasts[item.param] = input?.value || state.planningSettings.defaultUpliftPct || "35";
  });

  if (requireCustomDates && state.planningSettings.baseline === "custom_range") {
    if (!state.planningSettings.baselineStart || !state.planningSettings.baselineEnd) {
      state.planningMessage = "Choose both baseline dates before applying custom range.";
      renderWorkspace();
      return false;
    }
  }

  state.planningMessage = "";
  return true;
}

function planningControlsHtml(payload) {
  const config = payload.planningConfig || {};
  const baselineOptions = state.planningSettings.baselineOptions.length
    ? state.planningSettings.baselineOptions
    : config.baselineOptions || defaultPlanningBaselineOptions;
  const disabled = "";
  const customRangeVisible = state.planningSettings.baseline === "custom_range";
  return `
    <div class="filter-group planning-panel">
      <div class="group-title">Demand Planning Inputs</div>
      <div class="control-grid control-grid-planning">
        <label class="field">
          <span>Forecast Baseline</span>
          <select id="planningTabBaselineSelect" ${disabled}>
            ${baselineOptions
              .map(
                (option) =>
                  `<option value="${escapeHtml(option.value)}" ${state.planningSettings.baseline === option.value ? "selected" : ""}>${escapeHtml(option.label)}</option>`,
              )
              .join("")}
          </select>
        </label>
        <label class="field">
          <span>Default Increase %</span>
          <input id="planningTabDefaultUplift" type="number" step="0.1" min="-100" value="${escapeHtml(state.planningSettings.defaultUpliftPct || "35")}" ${disabled}>
        </label>
        <div class="field field-wide planning-note">
          <span>How It Works</span>
          <div>Use the main date range as the planning horizon. Pick a baseline mode here, and if you choose custom range, set the baseline dates below.</div>
        </div>
        <label class="field ${customRangeVisible ? "" : "field-hidden"}">
          <span>Baseline Start</span>
          <input id="planningTabBaselineStart" type="date" value="${escapeHtml(state.planningSettings.baselineStart || "")}" ${disabled}>
        </label>
        <label class="field ${customRangeVisible ? "" : "field-hidden"}">
          <span>Baseline End</span>
          <input id="planningTabBaselineEnd" type="date" value="${escapeHtml(state.planningSettings.baselineEnd || "")}" ${disabled}>
        </label>
        ${planningInputs
          .map(
            (item) => `
              <label class="field">
                <span>${escapeHtml(planningLabel(item.product))} %</span>
                <input
                  id="planning-${escapeHtml(item.param)}"
                  data-planning-param="${escapeHtml(item.param)}"
                  type="number"
                  step="0.1"
                  min="-100"
                  value="${escapeHtml(planningSettingValue(item.param))}"
                  ${disabled}
                >
              </label>
            `,
          )
          .join("")}
      </div>
      <div class="table-toolbar">
        <div class="table-caption">Update the forecast inputs here, then recalculate the planning table below.</div>
        <button id="applyPlanningButton" class="button button-secondary" type="button" ${disabled}>Apply Planning</button>
      </div>
      ${state.planningMessage ? `<div class="planning-message">${escapeHtml(state.planningMessage)}</div>` : ""}
    </div>
  `;
}

function renderSharedTable(tabKey, payload) {
  if (tabKey === "order-daily") {
    return tableHtml(
      payload.orderDailyRows,
      [
        { key: "reporting_date", label: "Date", format: fmtDate, mono: true },
        { key: "gross_product_sales", label: "Gross Product Sales", format: fmtCurrency, numeric: true, mono: true },
        { key: "net_product_sales", label: "Net Product Sales", format: fmtCurrency, numeric: true, mono: true },
        { key: "seller_discount", label: "Seller Discount", format: fmtCurrency, numeric: true, mono: true },
        { key: "export_refund_amount", label: "Export Refund Amount", format: fmtCurrency, numeric: true, mono: true },
        { key: "paid_orders", label: "Paid Orders", format: fmtNumber, numeric: true, mono: true },
        { key: "sales_units", label: "Sales Units", format: fmtNumber, numeric: true, mono: true },
        { key: "sample_units", label: "Sample Units", format: fmtNumber, numeric: true, mono: true },
        { key: "replacement_units", label: "Replacement Units", format: fmtNumber, numeric: true, mono: true },
        { key: "operational_units", label: "Operational Units", format: fmtNumber, numeric: true, mono: true },
      ],
      "Order-export daily output by paid time",
    );
  }

  if (tabKey === "statement-daily") {
    return tableHtml(
      payload.statementDailyRows,
      [
        { key: "reporting_date", label: "Statement Date", format: fmtDate, mono: true },
        { key: "gross_sales", label: "Gross Sales", format: fmtCurrency, numeric: true, mono: true },
        { key: "gross_sales_refund", label: "Gross Sales Refund", format: fmtCurrency, numeric: true, mono: true },
        { key: "seller_discount", label: "Seller Discount", format: fmtCurrency, numeric: true, mono: true },
        { key: "seller_discount_refund", label: "Seller Discount Refund", format: fmtCurrency, numeric: true, mono: true },
        { key: "net_sales", label: "Net Sales", format: fmtCurrency, numeric: true, mono: true },
        { key: "shipping_total", label: "Shipping", format: fmtCurrency, numeric: true, mono: true },
        { key: "fees_total", label: "Fees", format: fmtCurrency, numeric: true, mono: true },
        { key: "adjustments_total", label: "Adjustments", format: fmtCurrency, numeric: true, mono: true },
        { key: "payout_amount", label: "Payout Amount", format: fmtCurrency, numeric: true, mono: true },
      ],
      "Finance-statement daily output by statement date",
    );
  }

  if (tabKey === "expense-structure") {
    return [
      tableHtml(
        payload.expenseStructureRows,
        [
          { key: "category", label: "Category" },
          { key: "amount", label: "Amount", format: fmtCurrency, numeric: true, mono: true },
        ],
        "Expense structure by TikTok-style category",
      ),
      tableHtml(
        payload.expenseStructureDetailRows,
        [
          { key: "category", label: "Category" },
          { key: "line_item", label: "Line Item" },
          { key: "amount", label: "Amount", format: fmtCurrency, numeric: true, mono: true },
        ],
        "Underlying finance statement lines",
      ),
    ].join("");
  }

  if (tabKey === "products") {
    return tableHtml(
      payload.productRows,
      [
        { key: "product_name", label: "Product" },
        { key: "seller_sku_resolved", label: "Seller SKU", mono: true },
        { key: "order_count", label: "Orders", format: fmtNumber, numeric: true, mono: true },
        { key: "units_sold", label: "Units Sold", format: fmtNumber, numeric: true, mono: true },
        { key: "sales_units", label: "Sales Units", format: fmtNumber, numeric: true, mono: true },
        { key: "sample_units", label: "Sample Units", format: fmtNumber, numeric: true, mono: true },
        { key: "replacement_units", label: "Replacement Units", format: fmtNumber, numeric: true, mono: true },
        { key: "net_merchandise_sales", label: "Net Merchandise Sales", format: fmtCurrency, numeric: true, mono: true },
        { key: "returned_units", label: "Returned Units", format: fmtNumber, numeric: true, mono: true },
      ],
      "Normalized product performance",
    );
  }

  if (tabKey === "physical") {
    return tableHtml(
      payload.cogsSummaryRows,
      [
        { key: "product", label: "Product Sent to TikTok" },
        { key: "component_units_sold", label: "Units Sold", format: fmtNumber, numeric: true, mono: true },
        { key: "list_price", label: "List Price", format: fmtCurrency, numeric: true, mono: true },
        { key: "unit_cogs", label: "Unit COGS", format: fmtCurrency, numeric: true, mono: true },
        { key: "estimated_cogs", label: "Estimated COGS", format: fmtCurrency, numeric: true, mono: true },
      ],
      "Expanded physical product rollup",
    );
  }

  if (tabKey === "planning") {
    return `${planningControlsHtml(payload)}${tableHtml(
      payload.inventoryPlanningRows,
      [
        { key: "product", label: "Product" },
        { key: "snapshot_date", label: "Snapshot Date", format: fmtDate, mono: true },
        { key: "baseline_label", label: "Baseline", mono: true },
        { key: "baseline_start", label: "Baseline Start", format: fmtDate, mono: true },
        { key: "baseline_end", label: "Baseline End", format: fmtDate, mono: true },
        { key: "on_hand", label: "On Hand", format: fmtNumber, numeric: true, mono: true },
        { key: "in_transit", label: "In Transit", format: fmtNumber, numeric: true, mono: true },
        { key: "counted_in_transit", label: "In Transit Counted", format: fmtNumber, numeric: true, mono: true },
        { key: "effective_total_supply", label: "Usable Supply", format: fmtNumber, numeric: true, mono: true },
        { key: "units_sold_in_window", label: "Baseline Units", format: fmtNumber, numeric: true, mono: true },
        { key: "avg_daily_demand", label: "Avg Daily Demand", format: (value) => (value == null ? "N/A" : Number(value).toFixed(2)), numeric: true, mono: true },
        { key: "forecast_uplift_pct", label: "Forecast %", format: (value) => (value == null ? "N/A" : `${Number(value).toFixed(1)}%`), numeric: true, mono: true },
        { key: "forecast_daily_demand", label: "Forecast Daily", format: (value) => (value == null ? "N/A" : Number(value).toFixed(2)), numeric: true, mono: true },
        { key: "forecast_units_in_horizon", label: "Forecast Horizon Units", format: fmtNumber, numeric: true, mono: true },
        { key: "safety_stock_weeks", label: "Safety Weeks", format: fmtNumber, numeric: true, mono: true },
        { key: "safety_stock_units", label: "Safety Units", format: fmtNumber, numeric: true, mono: true },
        { key: "weeks_on_hand", label: "Weeks On Hand", format: (value) => (value == null ? "N/A" : Number(value).toFixed(1)), numeric: true, mono: true },
        { key: "weeks_total_supply", label: "Weeks Total", format: (value) => (value == null ? "N/A" : Number(value).toFixed(1)), numeric: true, mono: true },
        { key: "projected_in_transit_arrival_date", label: "Transit ETA", format: fmtDate, mono: true },
        { key: "projected_stockout_date", label: "Projected Stockout", format: fmtDate, mono: true },
        { key: "reorder_date", label: "Reorder Date", format: fmtDate, mono: true },
        { key: "reorder_quantity", label: "Reorder Qty", format: fmtNumber, numeric: true, mono: true },
        { key: "status", label: "Status" },
      ],
      "Demand planning from inventory, selected planning horizon, and forecast uplift by product",
      { compact: true, stickyFirstColumn: true },
    )}`;
  }

  if (tabKey === "inventory-history") {
    return tableHtml(
      payload.inventoryHistoryRows,
      [
        { key: "date", label: "Date", format: fmtDate, mono: true },
        { key: "birria_in_transit", label: "Birria In Transit", format: fmtNumber, numeric: true, mono: true },
        { key: "birria_on_hand", label: "Birria On Hand", format: fmtNumber, numeric: true, mono: true },
        { key: "pozole_in_transit", label: "Pozole In Transit", format: fmtNumber, numeric: true, mono: true },
        { key: "pozole_on_hand", label: "Pozole On Hand", format: fmtNumber, numeric: true, mono: true },
        { key: "tinga_in_transit", label: "Tinga In Transit", format: fmtNumber, numeric: true, mono: true },
        { key: "tinga_on_hand", label: "Tinga On Hand", format: fmtNumber, numeric: true, mono: true },
        { key: "brine_in_transit", label: "Brine In Transit", format: fmtNumber, numeric: true, mono: true },
        { key: "brine_on_hand", label: "Brine On Hand", format: fmtNumber, numeric: true, mono: true },
        { key: "variety_pack_in_transit", label: "Variety In Transit", format: fmtNumber, numeric: true, mono: true },
        { key: "variety_pack_on_hand", label: "Variety On Hand", format: fmtNumber, numeric: true, mono: true },
        { key: "pozole_verde_in_transit", label: "Pozole Verde In Transit", format: fmtNumber, numeric: true, mono: true },
        { key: "pozole_verde_on_hand", label: "Pozole Verde On Hand", format: fmtNumber, numeric: true, mono: true },
      ],
      "TikTok inventory history from the daily Google Sheet",
      { compact: true },
    );
  }

  if (tabKey === "cogs") {
    return [
      panelHtml(
        "COGS per product",
        "Physical-product COGS rollup after bundle expansion.",
        tableHtml(
          payload.cogsSummaryRows,
          [
            { key: "product", label: "Physical Product" },
            { key: "component_units_sold", label: "Units Sold", format: fmtNumber, numeric: true, mono: true },
            { key: "list_price", label: "List Price", format: fmtCurrency, numeric: true, mono: true },
            { key: "unit_cogs", label: "Unit COGS", format: fmtCurrency, numeric: true, mono: true },
            { key: "estimated_cogs", label: "Estimated COGS", format: fmtCurrency, numeric: true, mono: true },
          ],
          "Physical product COGS summary",
          { compact: true },
        ),
        "COGS",
      ),
      panelHtml(
        "COGS by listing",
        "Listing-level COGS allocation and gross-profit proxy.",
        tableHtml(
          payload.cogsListingRows,
          [
            { key: "listing", label: "Listing" },
            { key: "listing_sku", label: "Resolved Listing SKU", mono: true },
            { key: "units_sold", label: "Units Sold", format: fmtNumber, numeric: true, mono: true },
            { key: "mapped_component_units", label: "Mapped Component Units", format: fmtNumber, numeric: true, mono: true },
            { key: "net_merchandise_sales", label: "Net Merchandise Sales", format: fmtCurrency, numeric: true, mono: true },
            { key: "estimated_cogs", label: "Estimated COGS", format: fmtCurrency, numeric: true, mono: true },
            { key: "estimated_gross_profit", label: "Estimated Gross Profit", format: fmtCurrency, numeric: true, mono: true },
            { key: "cogs_assumption", label: "COGS Assumption" },
          ],
          "Listing-level COGS estimate",
          { compact: true },
        ),
        "COGS",
      ),
    ].join("");
  }

  if (tabKey === "raw") {
    return tableHtml(
      payload.rawProductNameRows,
      [
        { key: "product_name", label: "Raw Product Name" },
        { key: "order_count", label: "Orders", format: fmtNumber, numeric: true, mono: true },
        { key: "units_sold", label: "Units Sold", format: fmtNumber, numeric: true, mono: true },
      ],
      "Closest match to a raw pivot by original product name",
    );
  }

  if (tabKey === "reconciliation") {
    return [
        panelHtml(
          "Matched orders",
          "Order export and finance rows joined by Order ID.",
          tableHtml(
          payload.reconciliationRows,
          [
            { key: "order_id", label: "Order ID", mono: true },
            { key: "reporting_date", label: "Order Date", format: fmtDate, mono: true },
            { key: "first_statement_date", label: "First Statement Date", format: fmtDate, mono: true },
            { key: "last_statement_date", label: "Last Statement Date", format: fmtDate, mono: true },
            { key: "gross_sales", label: "Order Gross Sales", format: fmtCurrency, numeric: true, mono: true },
            { key: "order_sales", label: "Order Net Sales", format: fmtCurrency, numeric: true, mono: true },
            { key: "statement_net_sales", label: "Statement Net Sales", format: fmtCurrency, numeric: true, mono: true },
            { key: "statement_amount_total", label: "Statement Amount", format: fmtCurrency, numeric: true, mono: true },
            { key: "shipping_fee_total", label: "Shipping Impact", format: fmtCurrency, numeric: true, mono: true },
            { key: "fulfillment_fee_total", label: "Fulfillment Fees", format: fmtCurrency, numeric: true, mono: true },
            { key: "actual_fee_total", label: "Actual Fee Total", format: fmtCurrency, numeric: true, mono: true },
            ],
            "Matched finance detail",
            { compact: true },
          ),
          "Finance",
        ),
      panelHtml(
        "Unmatched statement rows",
        "Finance rows not yet tied back to an order export row.",
        tableHtml(
          payload.unmatchedStatementRows,
          [
            { key: "statement_date", label: "Statement Date", format: fmtDate, mono: true },
            { key: "order_id", label: "Statement Order ID", mono: true },
            { key: "statement_type", label: "Type" },
            { key: "description", label: "Description" },
            { key: "total_settlement_amount", label: "Statement Amount", format: fmtCurrency, numeric: true, mono: true },
            ],
            "Unmatched finance rows",
            { compact: true },
          ),
          "Finance",
        ),
      panelHtml(
        "Unmatched orders",
        "Orders in the selected basis window without a matched statement row.",
        tableHtml(
          payload.unmatchedOrderRows,
          [
            { key: "order_id", label: "Order ID", mono: true },
            { key: "reporting_date", label: "Order Date", format: fmtDate, mono: true },
            { key: "gross_sales", label: "Gross Sales", format: fmtCurrency, numeric: true, mono: true },
            { key: "order_sales", label: "Net Sales", format: fmtCurrency, numeric: true, mono: true },
            { key: "refund_amount", label: "Refund Amount", format: fmtCurrency, numeric: true, mono: true },
            ],
            "Unmatched order rows",
            { compact: true },
          ),
          "Finance",
        ),
    ].join("");
  }

  if (tabKey === "target-city") {
    const summary = payload.targetCitySummary || {};
    const banner = `
      <div class="stat-banner">
        ${statChip("Target city", summary.target_city || "N/A", "primary")}
        ${summary.target_state ? statChip("State", summary.target_state, "primary") : ""}
        ${statChip("Radius", `${summary.city_radius_miles ?? "N/A"} miles`, "primary")}
        ${statChip("Customers", fmtNumber(summary.customers_in_city), "accent")}
        ${statChip("Orders", fmtNumber(summary.orders_in_city), "accent")}
        ${statChip("ZIP rows", fmtNumber(summary.zip_rows_in_city), "muted")}
      </div>
    `;
    return banner + tableHtml(
      payload.targetCityRows,
      [
        { key: "zipcode", label: "ZIP", mono: true },
        { key: "city", label: "City" },
        { key: "state", label: "State" },
        { key: "unique_customers", label: "Unique Customers", format: fmtNumber, numeric: true, mono: true },
        { key: "orders", label: "Orders", format: fmtNumber, numeric: true, mono: true },
      ],
      "ZIPs within the selected radius of the target city area",
    );
  }

  if (tabKey === "cities") {
    return tableHtml(
      payload.cityRows,
      [
        { key: "city", label: "City" },
        { key: "state", label: "State" },
        { key: "unique_customers", label: "Unique Customers", format: fmtNumber, numeric: true, mono: true },
        { key: "orders", label: "Orders", format: fmtNumber, numeric: true, mono: true },
      ],
      "Top cities by customer count",
    );
  }

  if (tabKey === "zips") {
    return tableHtml(
      payload.zipRows,
      [
        { key: "zipcode", label: "ZIP", mono: true },
        { key: "unique_customers", label: "Unique Customers", format: fmtNumber, numeric: true, mono: true },
        { key: "orders", label: "Orders", format: fmtNumber, numeric: true, mono: true },
      ],
      "Top ZIPs by customer count",
    );
  }

  if (tabKey === "radius") {
    const summary = payload.radiusSummary || {};
    const banner = `
      <div class="stat-banner">
        ${statChip("Target ZIP", summary.target_zip || "N/A", "primary")}
        ${statChip("Radius", `${summary.radius_miles ?? "N/A"} miles`, "primary")}
        ${statChip("Customers", fmtNumber(summary.customers_within_radius), "accent")}
        ${statChip("Orders", fmtNumber(summary.orders_within_radius), "accent")}
        ${statChip("ZIPs matched", fmtNumber(summary.matched_zip_rows), "muted")}
      </div>
    `;
    return banner + tableHtml(
      payload.radiusRows,
      [
        { key: "zipcode", label: "ZIP", mono: true },
        { key: "city", label: "City" },
        { key: "state", label: "State" },
        { key: "distance_miles", label: "Distance", format: fmtDistance, numeric: true, mono: true },
        { key: "unique_customers", label: "Unique Customers", format: fmtNumber, numeric: true, mono: true },
        { key: "orders", label: "Orders", format: fmtNumber, numeric: true, mono: true },
      ],
      "ZIPs within the selected radius",
    );
  }

  if (tabKey === "cohorts") {
    return tableHtml(
      payload.cohortSummaryRows,
      [
        { key: "first_order_month", label: "Cohort Month", format: fmtDate, mono: true },
        { key: "months_since_first", label: "Months Since First Order", format: fmtNumber, numeric: true, mono: true },
        { key: "active_customers", label: "Active Customers", format: fmtNumber, numeric: true, mono: true },
        { key: "cohort_size", label: "Cohort Size", format: fmtNumber, numeric: true, mono: true },
        { key: "retention_rate", label: "Retention Rate", format: fmtPercent, numeric: true, mono: true },
      ],
      "Cohort retention summary",
    );
  }

  if (tabKey === "audit") {
    return tableHtml(
      payload.mathAuditRows,
      [
        { key: "metric", label: "Metric" },
        { key: "value", label: "Value", format: fmtNumber, numeric: true, mono: true },
        { key: "note", label: "What it means" },
      ],
      "Math audit across raw, normalized, and expanded units",
    );
  }

  if (tabKey === "kpis") {
    return tableHtml(
      payload.kpiDefinitionRows,
      [
        { key: "metric", label: "KPI" },
        { key: "category", label: "Category" },
        { key: "formatted_value", label: "Value", mono: true },
        { key: "formula", label: "Formula" },
        { key: "notes", label: "Notes" },
      ],
      "Analyzer KPI definitions",
    );
  }

  return `<div class="report-frame">${escapeHtml(payload.reportMarkdown || "No report available.")}</div>`;
}

function renderOrdersWorkspace(payload) {
  const summary = payload.orderSummary || {};
  const overviewBody = `
    <div class="overview-grid">
      ${panelHtml("Orders revenue trend", "Gross and net product sales by paid time.", financeChartHtml(payload.orderDailyRows, "gross_product_sales", "net_product_sales", "Orders Gross", "Orders Net"), "Orders")}
      ${panelHtml(
        "Orders snapshot",
        "Current paid-time order performance.",
          metricsStackHtml([
            { label: "Net product sales", value: fmtCurrency(summary.orders_net_product_sales) },
            { label: "AOV", value: fmtCurrency(summary.orders_aov) },
            { label: "Paid orders", value: fmtNumber(summary.orders_paid_orders) },
            { label: "Sales units", value: fmtNumber(summary.sales_units) },
            { label: "Units per paid order", value: summary.units_per_paid_order == null ? "N/A" : Number(summary.units_per_paid_order).toFixed(2) },
            { label: "New customers", value: hostedFallback(summary.selected_first_time_buyers == null ? null : fmtNumber(summary.selected_first_time_buyers)) },
            { label: "Repeat customers", value: hostedFallback(summary.selected_repeat_customers == null ? null : fmtNumber(summary.selected_repeat_customers)) },
            { label: "Repeat customer rate", value: hostedFallback(summary.selected_repeat_customer_rate == null ? null : fmtPercent(summary.selected_repeat_customer_rate)) },
          ]),
        "Orders",
      )}
    </div>
    <div class="overview-grid">
      ${chartPanelHtml("orders-status", "Status mix", "Order status composition for the selected slice.", payload.statusRows, "status", "order_count", fmtNumber, "Mix", { green: true })}
      ${panelHtml(
        "Order health",
        "Operational rates for refunds, returns, cancellations, and delivery.",
          metricsStackHtml([
            { label: "Valid orders", value: hostedFallback(summary.valid_orders == null ? null : fmtNumber(summary.valid_orders)) },
            { label: "Cancellation rate", value: hostedFallback(summary.cancellation_rate == null ? null : fmtPercent(summary.cancellation_rate)) },
            { label: "Refund rate", value: hostedFallback(summary.refund_rate == null ? null : fmtPercent(summary.refund_rate)) },
            { label: "Return rate", value: hostedFallback(summary.return_rate == null ? null : fmtPercent(summary.return_rate)) },
            { label: "Delivery rate", value: hostedFallback(summary.delivery_rate == null ? null : fmtPercent(summary.delivery_rate)) },
          ]),
        "Health",
      )}
    </div>
    <div class="overview-grid">
        ${chartPanelHtml("orders-products", "Top products", "Top products in the selected hosted slice.", payload.productRows.slice(0, 8), "product_name", "units_sold", fmtNumber, "Products")}
        ${chartPanelHtml("orders-cities", "Top customer cities", "Top concentration points for unique customers.", payload.cityRows.slice(0, 8), "city", "unique_customers", fmtNumber, "Customers")}
    </div>
    ${panelHtml("Cohort heatmap", "Retention by first observed order month.", cohortHeatmapHtml(payload.cohortHeatmap), "Retention")}
      ${collapsibleDetailPanelHtml(
        "orders",
        state.activeSubtabs.orders,
        state.activeSubtabs.orders === "order-daily" ? "Orders daily output" : "Math audit",
        state.activeSubtabs.orders === "order-daily" ? "Selected-period order export table." : "Cross-check raw units against normalized and expanded units.",
        renderSharedTable(state.activeSubtabs.orders, payload),
        "Details",
      )}
  `;

  return workspaceShell(
    workspaces.find((workspace) => workspace.key === "orders").title,
    workspaces.find((workspace) => workspace.key === "orders").note,
    subtabButtonsHtml("orders"),
    overviewBody,
  );
}

function renderWorkspaceWithTable(workspaceKey, payload) {
  const workspace = workspaces.find((item) => item.key === workspaceKey);
  const activeSubtab = state.activeSubtabs[workspaceKey];
  const orderSummary = payload.orderSummary || {};
  const statementSummary = payload.statementSummary || {};
  const reconciliationSummary = payload.reconciliationSummary || {};
  let introPanels = "";

  if (workspaceKey === "finance") {
    introPanels = `
      <div class="overview-grid">
        ${panelHtml("Statement trend", "Gross sales and net sales by statement date.", financeChartHtml(payload.statementDailyRows, "gross_sales", "net_sales", "Finance Gross", "Finance Net"), "Finance")}
        ${panelHtml(
          "Finance snapshot",
          "Settlement-month totals from the finance statement export.",
          metricsStackHtml([
            { label: "Gross sales", value: fmtCurrency(statementSummary.finance_gross_sales) },
            { label: "Gross sales refund", value: fmtCurrency(statementSummary.finance_gross_sales_refund) },
            { label: "Seller discount", value: fmtCurrency(statementSummary.finance_seller_discount) },
            { label: "Seller discount refund", value: fmtCurrency(statementSummary.finance_seller_discount_refund) },
            { label: "Net sales", value: fmtCurrency(statementSummary.finance_net_sales) },
            { label: "Shipping", value: fmtCurrency(statementSummary.finance_shipping_total) },
            { label: "Fees", value: fmtCurrency(statementSummary.finance_fees_total) },
            { label: "Adjustments", value: fmtCurrency(statementSummary.finance_adjustments_total) },
            { label: "Payout amount", value: fmtCurrency(statementSummary.finance_payout_amount) },
          ]),
            "Finance",
          )}
      </div>
      <div class="overview-grid">
          ${chartPanelHtml("finance-expense", "Expense structure", "Largest finance cost buckets for the selected statement window.", payload.expenseStructureRows || [], "category", "amount", fmtCurrency, "Finance", { green: true })}
        ${panelHtml(
          "Statement coverage",
          "How much of the selected period has joined back to orders by Order ID.",
          metricsStackHtml([
            { label: "Matched orders", value: fmtNumber(reconciliationSummary.matched_orders) },
            { label: "Unmatched statement rows", value: fmtNumber(reconciliationSummary.unmatched_statement_rows) },
            { label: "Matched statement amount", value: fmtCurrency(reconciliationSummary.statement_amount_total) },
            { label: "Actual fee total", value: fmtCurrency(reconciliationSummary.actual_fee_total) },
          ]),
          "Reconciliation",
        )}
      </div>
    `;
  }

  if (workspaceKey === "reconciliation") {
    introPanels = `
      <div class="overview-grid">
        ${panelHtml(
          "Reconciliation summary",
          "Order ID join between paid-time orders and statement-month finance rows.",
          metricsStackHtml([
            { label: "Matched orders", value: fmtNumber(reconciliationSummary.matched_orders) },
            { label: "Unmatched statement rows", value: fmtNumber(reconciliationSummary.unmatched_statement_rows) },
            { label: "Unmatched orders", value: fmtNumber(reconciliationSummary.unmatched_orders) },
            { label: "Matched statement amount", value: fmtCurrency(reconciliationSummary.statement_amount_total) },
            { label: "Matched fee total", value: fmtCurrency(reconciliationSummary.actual_fee_total) },
            { label: "Matched fulfillment fees", value: fmtCurrency(reconciliationSummary.fulfillment_fee_total) },
          ]),
          "Reconciliation",
        )}
        ${panelHtml("Why it differs", "Orders and finance do not align month-for-month because settlement timing lags placement timing.", `<div class="report-frame">Orders workspace = paid-time operational month. Finance workspace = settlement month from statement date. Reconciliation shows where those two systems meet by Order ID.</div>`, "Definitions")}
      </div>
    `;
  }

  if (workspaceKey === "products") {
    const inventorySnapshot = payload.inventorySnapshot || {};
    const planningRows = payload.inventoryPlanningRows || [];
    const urgentRows = planningRows.filter((row) => row.status === "Urgent");
    const watchRows = planningRows.filter((row) => row.status === "Watch");
    const topReorderRow = [...planningRows].sort((a, b) => (b.reorder_quantity || 0) - (a.reorder_quantity || 0))[0];
    const planningConfig = payload.planningConfig || {};
    introPanels = `
      <div class="overview-grid">
          ${chartPanelHtml("products-physical", "Products", "Product unit ranking for the selected slice.", payload.productRows.slice(0, 10), "product_name", "units_sold", fmtNumber, "Products")}
          ${panelHtml(
            "Product summary",
            "High-level operating read for the current slice.",
            metricsStackHtml([
               { label: "Tracked products", value: fmtNumber(payload.productRows.length) },
              { label: "Listing rows", value: fmtNumber(payload.productRows.length) },
              { label: "Sales units", value: fmtNumber(orderSummary.sales_units) },
              { label: "Sample units", value: fmtNumber(orderSummary.sample_units) },
              { label: "Replacement units", value: fmtNumber(orderSummary.replacement_units) },
            ]),
            "Summary",
          )}
      </div>
      <div class="overview-grid">
        ${panelHtml(
          "Inventory snapshot",
          "Latest TikTok inventory row with actual values from the daily Google Sheet.",
          metricsStackHtml([
            { label: "Snapshot date", value: fmtDate(inventorySnapshot.snapshot_date) },
            { label: "Rows with values", value: fmtNumber(inventorySnapshot.valid_metric_count) },
            { label: "Urgent products", value: fmtNumber(urgentRows.length) },
            { label: "Watch products", value: fmtNumber(watchRows.length) },
          ]),
          "Inventory",
        )}
        ${panelHtml(
          "Planning signal",
          "Coverage uses the chosen baseline, forecast uplift, safety stock, and conservative lead time.",
          metricsStackHtml([
            { label: "Forecast baseline", value: planningConfig.baselineLabel || "Last Full Month" },
            { label: "Baseline window", value: planningConfig.baselineStart && planningConfig.baselineEnd ? `${planningConfig.baselineStart} to ${planningConfig.baselineEnd}` : "N/A" },
            { label: "Top reorder product", value: topReorderRow?.product || "None" },
            { label: "Top reorder qty", value: fmtNumber(topReorderRow?.reorder_quantity) },
          ]),
          "Planning",
        )}
      </div>
    `;
  }

  if (workspaceKey === "customers") {
    introPanels = `
      <div class="overview-grid">
        ${chartPanelHtml("customers-cities", "Top cities", "Highest customer concentration by city.", payload.cityRows.slice(0, 10), "city", "unique_customers", fmtNumber, "Cities")}
        ${panelHtml(
          "Targeting snapshot",
          "ZIP radius and city-area radius targeting for the selected slice.",
          metricsStackHtml([
            { label: "Target city radius", value: `${fmtNumber(payload.targetCitySummary?.city_radius_miles)} miles` },
            { label: "Target city customers", value: fmtNumber(payload.targetCitySummary?.customers_in_city) },
            { label: "Target city orders", value: fmtNumber(payload.targetCitySummary?.orders_in_city) },
            { label: "Radius customers", value: fmtNumber(payload.radiusSummary?.customers_within_radius) },
            { label: "Radius orders", value: fmtNumber(payload.radiusSummary?.orders_within_radius) },
            { label: "Unique customers", value: hostedFallback(orderSummary.selected_unique_customers == null ? null : fmtNumber(orderSummary.selected_unique_customers)) },
            { label: "Repeat customers", value: hostedFallback(orderSummary.selected_repeat_customers == null ? null : fmtNumber(orderSummary.selected_repeat_customers)) },
          ]),
          "Targeting",
        )}
      </div>
      ${panelHtml("Cohort heatmap", "Retention view for customer behavior.", cohortHeatmapHtml(payload.cohortHeatmap), "Retention")}
    `;
  }

  if (workspaceKey === "audit") {
    introPanels = `
      <div class="overview-grid">
        ${panelHtml(
          "Audit summary",
          "Use this workspace to trace how metrics were derived and what the exported report says.",
          metricsStackHtml([
            { label: "Math audit rows", value: fmtNumber(payload.mathAuditRows.length) },
            { label: "KPI definitions", value: fmtNumber(payload.kpiDefinitionRows.length) },
            { label: "Raw product rows", value: fmtNumber(payload.rawProductNameRows.length) },
          ]),
          "Audit",
        )}
        ${panelHtml("Source notes", "Analyzer definitions, formulas, and report output live here.", `<div class="report-frame">Switch between Math audit, KPI definitions, and Report using the sub-tabs above.</div>`, "Audit")}
      </div>
    `;
  }

  const detailBlock = collapsibleDetailPanelHtml(
    workspaceKey,
    activeSubtab,
    "Detail view",
    "Focused output for the active subtab.",
    renderSharedTable(activeSubtab, payload),
    "Details",
  );

  return workspaceShell(
    workspace.title,
    workspace.note,
    subtabButtonsHtml(workspaceKey),
    `${introPanels}${detailBlock}`,
  );
}

function renderWorkspace() {
  const root = document.getElementById("workspaceContent");
  const payload = state.payload;
  if (!payload) {
    root.innerHTML = '<div class="empty-state">No data loaded.</div>';
    return;
  }

  if (state.activeWorkspace === "orders") {
    root.innerHTML = renderOrdersWorkspace(payload);
  } else {
    root.innerHTML = renderWorkspaceWithTable(state.activeWorkspace, payload);
  }

  document.querySelectorAll(".workspace-button").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeWorkspace = button.dataset.workspace;
      openDetailPanel(state.activeWorkspace, state.activeSubtabs[state.activeWorkspace]);
      renderWorkspaceNav();
      renderWorkspace();
    });
  });

  document.querySelectorAll(".subtab-button").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeSubtabs[button.dataset.workspace] = button.dataset.subtab;
      openDetailPanel(button.dataset.workspace, button.dataset.subtab);
      renderWorkspace();
    });
  });

  document.querySelectorAll(".chart-mode-button").forEach((button) => {
    button.addEventListener("click", () => {
      state.chartModes[button.dataset.chartPanel] = button.dataset.chartMode;
      renderWorkspace();
    });
  });

  document.querySelectorAll(".detail-toggle-button").forEach((button) => {
    button.addEventListener("click", () => {
      const key = button.dataset.detailToggle;
      const [workspaceKey, subtab] = key.split(":");
      state.detailExpanded[key] = !isDetailExpanded(workspaceKey, subtab);
      renderWorkspace();
    });
  });

  const applyPlanningButton = document.getElementById("applyPlanningButton");
  if (applyPlanningButton) {
    const baselineSelect = document.getElementById("planningTabBaselineSelect");
    if (baselineSelect) {
      baselineSelect.addEventListener("change", () => {
        state.planningSettings.baseline = baselineSelect.value || "last_full_month";
        state.planningMessage = "";
        if (state.planningSettings.baseline === "custom_range") {
          renderWorkspace();
          return;
        }
        if (syncPlanningStateFromDom(false)) {
          loadDashboard();
        }
      });
    }

    const applyPlanning = () => {
      if (syncPlanningStateFromDom(true)) loadDashboard();
    };

    applyPlanningButton.addEventListener("click", applyPlanning);
    document.querySelectorAll("#planningTabBaselineSelect, #planningTabBaselineStart, #planningTabBaselineEnd, #planningTabDefaultUplift, [data-planning-param]").forEach((input) => {
      input.addEventListener("keydown", (event) => {
        if (event.key === "Enter") applyPlanning();
      });
    });
  }
}

function renderWorkspaceNav() {
  document.getElementById("workspaceNav").innerHTML = workspaceButtonsHtml();
}

async function loadDashboard(forceRemote = false) {
  setSubmitState(true);
  setLoading();
  try {
    if (staticModeEnabled() && forceRemote) {
      state.staticChunkCache.clear();
    }
    if (!staticModeEnabled() || forceRemote || !state.basePayload) {
      const payload = await fetchJson(dashboardUrl());
      if (staticModeEnabled()) {
        state.basePayload = payload;
      } else {
        state.payload = payload;
      }
    }
    const payload = staticModeEnabled() ? await buildStaticPayload(state.basePayload) : state.payload;
    state.payload = payload;
    state.planningSettings.baseline = payload.summary?.planning_baseline || state.planningSettings.baseline;
    state.planningSettings.baselineStart = payload.summary?.planning_baseline_start || state.planningSettings.baselineStart;
    state.planningSettings.baselineEnd = payload.summary?.planning_baseline_end || state.planningSettings.baselineEnd;
    state.planningSettings.defaultUpliftPct = String(payload.summary?.planning_default_uplift ?? state.planningSettings.defaultUpliftPct);
    (payload.planningConfig?.productForecasts || []).forEach((item) => {
      const planningInput = planningInputs.find((entry) => entry.product === item.product);
      if (planningInput) {
        state.planningSettings.productForecasts[planningInput.param] = String(item.uplift_pct ?? state.planningSettings.defaultUpliftPct);
      }
    });
    renderSummary(payload.summary);
    renderWorkspaceNav();
    renderWorkspace();
  } catch (error) {
    document.getElementById("workspaceContent").innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
  } finally {
    setSubmitState(false);
  }
}

async function uploadFiles() {
  const fileInput = document.getElementById("uploadFiles");
  const uploadKind = document.getElementById("uploadKind").value;
  const files = [...(fileInput?.files || [])];
  if (!files.length) {
    setUploadState(false, "Choose at least one file first.");
    return;
  }

  const formData = new FormData();
  formData.set("upload_kind", uploadKind);
  files.forEach((file) => formData.append("files", file));

  setUploadState(true, `Uploading ${files.length} file(s)...`);
  try {
    const result = await postForm(uploadUrl(), formData);
    fileInput.value = "";
    state.meta = await fetchJson(metaUrl());
    renderMeta(state.meta);
    await loadDashboard();
    setUploadState(false, result.message || "Upload complete.");
  } catch (error) {
    setUploadState(false, error.message);
  }
}

async function init() {
  state.deploymentMode = resolveDeploymentMode();
  state.meta = await fetchJson(metaUrl());
  renderMeta(state.meta);
  document.getElementById("dateBasisSelect").addEventListener("change", () => {
    syncDateBounds();
  });
  const applyDashboardFilters = () => {
    syncPlanningStateFromDom(false);
    loadDashboard(staticModeEnabled() ? false : true);
  };
  document.getElementById("applyFiltersButton").addEventListener("click", applyDashboardFilters);
  document.getElementById("targetingToggle")?.addEventListener("click", () => {
    state.targetingExpanded = !state.targetingExpanded;
    renderTargetingExpanded();
  });
  document.getElementById("refreshButton").addEventListener("click", () => loadDashboard(true));
  document.getElementById("uploadButton").addEventListener("click", uploadFiles);
  ["startDate", "endDate", "targetZip", "radiusMilesSelect", "targetCity", "cityRadiusMilesSelect", "targetState"].forEach((id) => {
    document.getElementById(id).addEventListener("keydown", (event) => {
      if (event.key === "Enter") applyDashboardFilters();
    });
  });
  renderWorkspaceNav();
  await loadDashboard();
}

init();
