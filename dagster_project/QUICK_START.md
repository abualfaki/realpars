# Quick Start Guide - Dagster Pipeline

Get the RealPars weekly report pipeline running in 10 minutes.

## Step 1: Install Dependencies (1 min)

```bash
cd "/Users/abubakaral-faki/Projects/Client Projects/01-RealPars"

# Install Python packages (if not already installed)
pip install dagster dagster-webserver dagster-airbyte dagster-dbt
```

## Step 2: Add Environment Variables (2 min)

Add these to your `.env` file in the project root:

```bash
# Airbyte Configuration
AIRBYTE_API_TOKEN=your_token_here
AIRBYTE_WORKSPACE_ID=your_workspace_id

# Make.com Webhook
MAKE_WEBHOOK_URL=https://hook.eu1.make.com/your-webhook-id
```

**Where to find these:**
- **Airbyte Token**: [Airbyte Cloud → Settings → Applications](https://cloud.airbyte.com/settings/applications)
- **Connection IDs**: Airbyte Cloud → Connections → Click connection → Copy ID from URL
- **Make.com Webhook**: Make.com → Your Scenario → Add Webhook module → Copy URL

## Step 3: Prepare dbt (1 min)

```bash
cd dbt/realpars_community
dbt compile
cd ../..
```

## Step 4: Start Dagster (1 min)

```bash
# Start Dagster UI
dagster dev -f dagster_project/definitions.py
```

Open your browser: **http://localhost:3000**

## Step 5: Test the Pipeline (5 min)

### Option A: Run Complete Pipeline

1. Go to **Assets** tab
2. Select all assets (or just click **trigger_make_weekly_reports**)
3. Click **Materialize**
4. Watch it run:
   - ⏳ Airbyte syncs (5-10 min)
   - ⏳ dbt models (2-3 min)
   - ⏳ Make.com trigger (<1 min)

### Option B: Test Individual Steps

**Just sync data:**
- Jobs → `airbyte_sync_only` → Launch Run

**Just run dbt:**
- Jobs → `dbt_transform_only` → Launch Run

**Just trigger emails:**
- Jobs → `email_trigger_only` → Launch Run

## Step 6: Enable Scheduling (Optional)

1. Go to **Schedules** tab
2. Find **weekly_report_schedule**
3. Click **Turn On**

Your pipeline will now run automatically every Monday at 2 AM!

## Troubleshooting

**"Asset not found" error:**
- Check that `.env` has all required Airbyte connection IDs

**"dbt compilation failed":**
```bash
cd dbt/realpars_community
dbt debug
```

**"Airbyte connection failed":**
- Verify `AIRBYTE_API_TOKEN` is correct
- Check token hasn't expired

**Make.com not receiving webhook:**
- Test webhook manually: `curl -X POST $MAKE_WEBHOOK_URL -d '{"test": true}'`
- Check webhook is "On" in Make.com

## What's Next?

- View [README.md](README.md) for full documentation
- Explore asset lineage in Dagster UI
- Customize schedules in [weekly_report_job.py](jobs/weekly_report_job.py)
- Add sensors for event-driven triggers

## Architecture Diagram

```
Circle.so API
     ↓
[Airbyte] → BigQuery (raw tables)
     ↓
[dbt] → BigQuery (analytics tables)
     ↓
[Make.com] → Email Reports
```

All orchestrated by Dagster! 🚀
