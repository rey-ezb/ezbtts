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
