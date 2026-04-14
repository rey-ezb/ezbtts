# Netlify Setup

## What you need

- A free Netlify account
- The GitHub repo already connected

## Recommended settings

- Base directory: leave blank
- Build command: `python deployment/export_dashboard_snapshot.py`
- Publish directory: `web_dashboard`

Netlify will read `netlify.toml`, install the Python dependencies from `requirements.txt`, build the snapshot, and publish the static dashboard.

## What happens after deploy

- The live site will show the latest generated snapshot
- Local uploads still only work in local mode
- If you want hosted upload history later, use Supabase
