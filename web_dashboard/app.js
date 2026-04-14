const state = {
  meta: null,
  payload: null,
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

function staticModeEnabled() {
  return state.deploymentMode === "static";
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

function planningSettingValue(param) {
  const defaultUplift = state.planningSettings.defaultUpliftPct || "35";
  return state.planningSettings.productForecasts[param] || defaultUplift;
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
  renderSourceToggles(meta.availableSources);
  renderUploadTargets(meta.uploadTargets || []);
  renderPlanningDefaults(meta.planningDefaults || {});
  syncDateBounds();
  applyDeploymentModeUi();
}

function applyDeploymentModeUi() {
  if (!staticModeEnabled()) return;
  document.getElementById("outputSelect").disabled = true;
  document.getElementById("dateBasisSelect").disabled = true;
  document.getElementById("orderBucketModeSelect").disabled = true;
  document.getElementById("startDate").disabled = true;
  document.getElementById("endDate").disabled = true;
  document.getElementById("targetZip").disabled = true;
  document.getElementById("radiusMilesSelect").disabled = true;
  document.getElementById("targetCity").disabled = true;
  document.getElementById("targetState").disabled = true;
  document.getElementById("uploadKind").disabled = true;
  document.getElementById("uploadFiles").disabled = true;
  const submitButton = document.getElementById("applyFiltersButton");
  submitButton.disabled = true;
  submitButton.textContent = "Snapshot mode";
  document.getElementById("refreshButton").textContent = "Reload snapshot";
  const uploadButton = document.getElementById("uploadButton");
  if (uploadButton) {
    uploadButton.disabled = true;
    uploadButton.textContent = "Upload disabled";
  }
  const uploadStatus = document.getElementById("uploadStatus");
  if (uploadStatus) {
    uploadStatus.textContent = "Uploads only work in local mode.";
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
      "Net product sales divided by paid orders in the selected paid-time slice.",
      `
        ${detailFormulaHtml("AOV = Net Product Sales / Paid Orders", ["SKU Subtotal Before Discount", "SKU Seller Discount", "Order Refund Amount", "Order ID"])}
        ${detailListHtml([
          { label: "Net product sales", value: fmtCurrency(orderSummary.orders_net_product_sales) },
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
        ${detailFormulaHtml("Unique Customers = COUNT(DISTINCT customer proxy)", [payload.orderSummary?.customer_id_basis || "Buyer Username -> Buyer Nickname -> Recipient"])}
        ${detailListHtml([
          { label: "Unique customers", value: fmtNumber(orderSummary.selected_unique_customers) },
          { label: "First-time buyers", value: fmtNumber(orderSummary.selected_first_time_buyers) },
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
      "First-Time Buyers",
      "Customer proxies whose first observed valid order in loaded history falls in this slice.",
      `
        ${detailFormulaHtml("First-Time Buyers = COUNT(DISTINCT selected-period customer proxy where first observed order date in available history equals first selected-period order date)", [payload.orderSummary?.customer_id_basis || "Buyer Username -> Buyer Nickname -> Recipient"])}
        ${detailListHtml([
          { label: "First-time buyers", value: fmtNumber(orderSummary.selected_first_time_buyers) },
          { label: "Returning customers", value: fmtNumber(orderSummary.selected_returning_customers) },
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
      "Customer proxies with more than one order in the selected paid-time slice.",
      `
        ${detailFormulaHtml("Repeat Customers = COUNT(DISTINCT customer proxy with more than 1 selected-period order)", [payload.orderSummary?.customer_id_basis || "Buyer Username -> Buyer Nickname -> Recipient"])}
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
        ${detailFormulaHtml("Repeat Customer Rate = Repeat Customers / Unique Customers", [payload.orderSummary?.customer_id_basis || "Buyer Username -> Buyer Nickname -> Recipient"])}
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
    staticModeEnabled() ? statChip("Deploy Mode", "Static snapshot", "accent") : "",
    state.meta?.snapshotGeneratedAt ? statChip("Snapshot", new Date(state.meta.snapshotGeneratedAt).toLocaleString(), "accent") : "",
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
    { key: "orders-aov", label: "AOV", value: fmtCurrency(orderSummary.orders_aov), note: "Net product sales divided by paid orders in the selected paid-time slice" },
    { key: "orders-paid-orders", label: "Paid Orders", value: fmtNumber(orderSummary.orders_paid_orders), note: "Unique paid orders in the selected paid-time slice" },
    { key: "units-sold", label: "Units Sold", value: fmtNumber(orderSummary.sales_units), note: "Sales units in the selected paid-time slice; samples and replacements excluded" },
    { key: "unique-customers", label: "Unique Customers", value: fmtNumber(orderSummary.selected_unique_customers), note: "Distinct customer proxies in the selected paid-time slice" },
    { key: "first-time-buyers", label: "First-Time Buyers", value: fmtNumber(orderSummary.selected_first_time_buyers), note: "Customer proxies whose first observed valid order in loaded history falls in this slice" },
    { key: "repeat-customers", label: "Repeat Customers", value: fmtNumber(orderSummary.selected_repeat_customers), note: "Customer proxies with more than one order in the selected paid-time slice" },
    { key: "repeat-customer-rate", label: "Repeat Customer Rate", value: fmtPercent(orderSummary.selected_repeat_customer_rate), note: "Repeat customers divided by unique customers in the selected paid-time slice" },
  ];

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

function barListHtml(rows, labelKey, valueKey, formatter = fmtNumber, green = false) {
  if (!rows.length) return '<div class="empty-state">No rows for the current filters.</div>';
  const max = Math.max(...rows.map((row) => Math.abs(row[valueKey] || 0)), 1);
  return `
    <div class="bar-list">
      ${rows
        .map(
          (row) => `
            <div class="bar-row">
              <div class="bar-meta">
                <span>${escapeHtml(row[labelKey])}</span>
                <strong class="mono">${formatter(row[valueKey])}</strong>
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
          <title>${escapeHtml(row.label)}: ${formatter(row.rawValue)}</title>
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
                <span class="pie-legend-value mono">${formatter(row.rawValue)}</span>
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
  const disabled = staticModeEnabled() ? "disabled" : "";
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
      "Exact city plus optional state match",
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
          { label: "First-time buyers", value: fmtNumber(summary.selected_first_time_buyers) },
          { label: "Repeat customers", value: fmtNumber(summary.selected_repeat_customers) },
          { label: "Repeat customer rate", value: fmtPercent(summary.selected_repeat_customer_rate) },
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
          { label: "Valid orders", value: fmtNumber(summary.valid_orders) },
          { label: "Cancellation rate", value: fmtPercent(summary.cancellation_rate) },
          { label: "Refund rate", value: fmtPercent(summary.refund_rate) },
          { label: "Return rate", value: fmtPercent(summary.return_rate) },
          { label: "Delivery rate", value: fmtPercent(summary.delivery_rate) },
        ]),
        "Health",
      )}
    </div>
    <div class="overview-grid">
        ${chartPanelHtml("orders-products", "Top products sent", "Physical products after bundle expansion.", payload.cogsSummaryRows.slice(0, 8), "product", "component_units_sold", fmtNumber, "Products")}
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
          ${chartPanelHtml("products-physical", "Products sent to TikTok", "Physical unit ranking after bundle expansion.", payload.cogsSummaryRows.slice(0, 10), "product", "component_units_sold", fmtNumber, "Products")}
          ${panelHtml(
            "Product summary",
            "High-level operating read for the current slice.",
            metricsStackHtml([
              { label: "Physical products tracked", value: fmtNumber(payload.cogsSummaryRows.length) },
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
          "Exact city and ZIP radius targeting for the selected slice.",
          metricsStackHtml([
            { label: "Target city customers", value: fmtNumber(payload.targetCitySummary?.customers_in_city) },
            { label: "Target city orders", value: fmtNumber(payload.targetCitySummary?.orders_in_city) },
            { label: "Radius customers", value: fmtNumber(payload.radiusSummary?.customers_within_radius) },
            { label: "Radius orders", value: fmtNumber(payload.radiusSummary?.orders_within_radius) },
            { label: "Unique customers", value: fmtNumber(orderSummary.selected_unique_customers) },
            { label: "Repeat customers", value: fmtNumber(orderSummary.selected_repeat_customers) },
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
      renderWorkspaceNav();
      renderWorkspace();
    });
  });

  document.querySelectorAll(".subtab-button").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeSubtabs[button.dataset.workspace] = button.dataset.subtab;
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

async function loadDashboard() {
  setSubmitState(true);
  setLoading();
  try {
    const payload = await fetchJson(dashboardUrl());
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
    const result = await postForm("/api/upload", formData);
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
    loadDashboard();
  };
  document.getElementById("applyFiltersButton").addEventListener("click", applyDashboardFilters);
  document.getElementById("refreshButton").addEventListener("click", loadDashboard);
  document.getElementById("uploadButton").addEventListener("click", uploadFiles);
  ["startDate", "endDate", "targetZip", "targetCity", "targetState"].forEach((id) => {
    document.getElementById(id).addEventListener("keydown", (event) => {
      if (event.key === "Enter") applyDashboardFilters();
    });
  });
  renderWorkspaceNav();
  await loadDashboard();
}

init();
