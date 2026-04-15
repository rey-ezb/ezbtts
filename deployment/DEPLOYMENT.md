# Deployment Notes

## Current Production Shape

- GitHub: source control and deploy trigger
- Netlify: static frontend hosting
- Python: snapshot generation
- Supabase: shared hosted storage and database for snapshots and planning rows
- Supabase Storage raw uploads: shared source files for hosted rebuilds

## Why This Shape

The analytics logic is still Python and pandas heavy. Rebuilding it as always-on hosted compute would cost more and add more moving parts.

This shape keeps:

- the current business logic
- fast page loads on Netlify
- free-tier compatibility
- a hosted data copy that does not depend on your laptop being online

## Required Accounts

- GitHub free account
- Netlify free account
- Supabase free account

## Supabase Setup

1. Create a Supabase project.
2. Open the SQL editor and run `supabase/schema.sql`.
3. Create a Storage bucket named `dashboard-snapshots`.
4. Create a Storage bucket named `dashboard-uploads`.
4. Set these environment variables locally when you want to sync hosted data:

```bash
SUPABASE_URL=...
SUPABASE_SERVICE_ROLE_KEY=...
SUPABASE_STORAGE_BUCKET=dashboard-snapshots
SUPABASE_STORAGE_PREFIX=latest
SUPABASE_SNAPSHOT_LABEL=latest
SUPABASE_UPLOAD_BUCKET=dashboard-uploads
SUPABASE_UPLOAD_PREFIX=uploads
```

## Netlify Runtime Config

To make the deployed frontend read hosted data from Supabase Storage and accept hosted uploads, add these Netlify environment variables in the site settings:

```bash
SUPABASE_URL=...
SUPABASE_SERVICE_ROLE_KEY=...
SUPABASE_STORAGE_BUCKET=dashboard-snapshots
SUPABASE_STORAGE_PREFIX=latest
SUPABASE_UPLOAD_BUCKET=dashboard-uploads
SUPABASE_UPLOAD_PREFIX=uploads
NETLIFY_BUILD_HOOK_URL=...
```

The Netlify build command should be:

```bash
python deployment/sync_dashboard_to_supabase.py
```

That build will:

- materialize raw uploads from `dashboard-uploads`
- rebuild the dashboard snapshot
- upload the fresh snapshot into `dashboard-snapshots`
- publish the site

## Commands

Upload snapshot files to Supabase Storage only:

```bash
python deployment/sync_snapshot_to_supabase.py
```

Upload the snapshot files and write the dashboard/planning rows into Supabase tables:

```bash
python deployment/sync_dashboard_to_supabase.py
```

Run tests:

```bash
python -m unittest tests.test_export_dashboard_snapshot tests.test_sync_dashboard_to_supabase tests.test_hosted_uploads -v
```
