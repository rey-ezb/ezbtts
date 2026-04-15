# Netlify Setup

## What you need

- A free Netlify account
- The GitHub repo already connected

## Recommended settings

- Base directory: leave blank
- Build command: `python deployment/sync_dashboard_to_supabase.py`
- Publish directory: `web_dashboard`

Netlify will read `netlify.toml`, install the Python dependencies from `requirements.txt`, rebuild the snapshot from hosted raw uploads, sync the latest snapshot to Supabase, and publish the static dashboard.

## Required environment variables

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_STORAGE_BUCKET=dashboard-snapshots`
- `SUPABASE_STORAGE_PREFIX=latest`
- `SUPABASE_UPLOAD_BUCKET=dashboard-uploads`
- `SUPABASE_UPLOAD_PREFIX=uploads`
- `NETLIFY_BUILD_HOOK_URL`

## What happens after deploy

- The live site reads the hosted snapshot from Supabase
- The upload button stores raw files in Supabase and triggers a rebuild
- The rebuild regenerates the dashboard snapshot from hosted raw files
- Teammates can view the refreshed dashboard without your laptop being online
