# Deployment Notes

## Recommended Free-Tier Architecture

- GitHub: source control and deploy trigger
- Netlify: static frontend hosting
- Python: snapshot generation
- Supabase: optional snapshot storage only

## Why This Shape

The current analytics stack is Python and pandas heavy. Rebuilding that as live serverless compute would be slower, more brittle, and more likely to exceed free-tier limits.

Static snapshot deployment keeps:

- the current business logic
- fast page loads
- zero paid infrastructure

## Required Accounts

- GitHub free account
- Netlify free account
- Supabase free account only if you want remote snapshot storage

## Commands

Generate the deploy snapshot:

```bash
python deployment/export_dashboard_snapshot.py
```

Run tests:

```bash
python -m unittest tests.test_export_dashboard_snapshot -v
```

Optional upload to Supabase Storage:

```bash
python deployment/sync_snapshot_to_supabase.py
```
