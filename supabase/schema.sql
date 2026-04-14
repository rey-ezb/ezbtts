create table if not exists public.upload_batches (
  id uuid primary key default gen_random_uuid(),
  upload_type text not null,
  original_filename text not null,
  stored_filename text not null,
  storage_path text,
  uploaded_at timestamptz not null default now(),
  file_size_bytes bigint,
  notes text
);

create table if not exists public.upload_coverage (
  id uuid primary key default gen_random_uuid(),
  upload_batch_id uuid not null references public.upload_batches(id) on delete cascade,
  source_label text not null,
  start_date date,
  end_date date,
  replaced_by_batch_id uuid references public.upload_batches(id) on delete set null,
  is_active boolean not null default true
);

create index if not exists upload_batches_uploaded_at_idx on public.upload_batches (uploaded_at desc);
create index if not exists upload_coverage_source_dates_idx on public.upload_coverage (source_label, start_date, end_date);

create table if not exists public.dashboard_snapshots (
  id uuid primary key default gen_random_uuid(),
  snapshot_label text not null default 'latest',
  selected_output_dir text,
  generated_at timestamptz not null default now(),
  inserted_at timestamptz not null default now(),
  meta jsonb not null,
  summary jsonb not null,
  planning_config jsonb not null default '{}'::jsonb,
  order_summary jsonb not null default '{}'::jsonb,
  statement_summary jsonb not null default '{}'::jsonb,
  reconciliation_summary jsonb not null default '{}'::jsonb,
  data_quality_summary jsonb not null default '{}'::jsonb,
  report_markdown text not null default ''
);

create index if not exists dashboard_snapshots_label_generated_idx
  on public.dashboard_snapshots (snapshot_label, generated_at desc);

create table if not exists public.inventory_planning_snapshots (
  id uuid primary key default gen_random_uuid(),
  snapshot_id uuid not null references public.dashboard_snapshots(id) on delete cascade,
  product text not null,
  inventory_product text,
  snapshot_date date,
  baseline_label text,
  baseline_start date,
  baseline_end date,
  on_hand numeric,
  in_transit numeric,
  counted_in_transit numeric,
  effective_total_supply numeric,
  units_sold_in_window numeric,
  avg_daily_demand numeric,
  forecast_uplift_pct numeric,
  forecast_daily_demand numeric,
  forecast_units_in_horizon numeric,
  safety_stock_weeks numeric,
  safety_stock_units numeric,
  projected_in_transit_arrival_date date,
  days_on_hand numeric,
  days_total_supply numeric,
  weeks_on_hand numeric,
  weeks_total_supply numeric,
  projected_stockout_date date,
  reorder_date date,
  reorder_quantity numeric,
  status text
);

create index if not exists inventory_planning_snapshots_snapshot_idx
  on public.inventory_planning_snapshots (snapshot_id, product);
