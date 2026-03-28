# RealPars Weekly Report Dagster Pipeline

Complete data orchestration pipeline for syncing Circle.so community data, transforming it with dbt, and triggering email automation via Make.com.

## Pipeline Overview

```
┌──────────────────────────────────────────────────────────────┐
│                    DAGSTER PIPELINE                           │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  Step 1: Airbyte Sync (Circle.so → BigQuery)                │
│  ├─ community_members                                         │
│  ├─ posts & comments                                          │
│  ├─ events & attendees                                        │
│  └─ course completions                                        │
│                    ↓                                          │
│  Step 2: dbt Transformations                                  │
│  ├─ Staging: Clean raw data                                   │
│  ├─ Intermediate: Calculate engagement metrics               │
│  └─ Marts: Build reporting tables                            │
│                    ↓                                          │
│  Step 3: Make.com Trigger                                     │
│  └─ Send weekly PDF reports via email                        │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

## Project Structure

```
dagster_project/
├── definitions.py              # Main Dagster entry point
├── assets/
│   ├── airbyte_assets.py      # Step 1: Airbyte syncs
│   ├── dbt_assets.py          # Step 2: dbt transformations
│   └── make_dot_com_trigger.py # Step 3: Email automation
├── resources/
│   ├── airbyte_resources.py   # Airbyte connection config
│   └── dbt_resource.py        # dbt CLI config
└── jobs/
    └── weekly_report_job.py   # Job definitions & schedules
```

## Prerequisites

1. **Environment Variables** - Add to your `.env` file:
   ```bash
   # Airbyte Configuration
   AIRBYTE_API_TOKEN=your_airbyte_cloud_token
   AIRBYTE_WORKSPACE_ID=your_workspace_id
   
   # Airbyte Connection IDs (from Airbyte Cloud)
   AIRBYTE_CONNECTION_ID_COMMUNITY_MEMBERS_TABLES=xxx-xxx-xxx
   AIRBYTE_CONNECTION_ID_COMMENTS_TABLE=xxx-xxx-xxx
   AIRBYTE_CONNECTION_ID_EVENTS_LIST_&_ATTENDEES_TABLES=xxx-xxx-xxx
   AIRBYTE_CONNECTION_ID_MEMEBER_TAGS_TABLE=xxx-xxx-xxx
   AIRBYTE_CONNECTION_ID_POSTS_TABLE=xxx-xxx-xxx
   
   # Make.com Webhook
   MAKE_WEBHOOK_URL=https://hook.eu1.make.com/your-webhook-id
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Prepare dbt Project**:
   ```bash
   cd dbt/realpars_community
   dbt deps
   dbt compile
   ```

## How to Run

### Start Dagster UI (Development)

```bash
# From project root
cd /Users/abubakaral-faki/Projects/Client\ Projects/01-RealPars

# Start Dagster development server
dagster dev -f dagster_project/definitions.py
```

Then open: **http://localhost:3000**

### Run Complete Pipeline

1. Go to **Assets** tab
2. Select **trigger_make_weekly_reports** (this will auto-select all dependencies)
3. Click **Materialize**
4. Watch the pipeline execute:
   - ✓ Airbyte syncs complete (5-10 minutes)
   - ✓ dbt models run (2-3 minutes)
   - ✓ Make.com triggered (instant)

### Run Individual Steps

**Sync data only (without sending emails):**
```bash
# Via UI: Go to Jobs → "airbyte_sync_only" → Launch Run
```

**Run dbt transformations only:**
```bash
# Via UI: Go to Jobs → "dbt_transform_only" → Launch Run
```

**Trigger emails only (assumes data is ready):**
```bash
# Via UI: Go to Jobs → "email_trigger_only" → Launch Run
```

## Schedules

The pipeline includes two automated schedules:

### 1. Weekly Report (Full Pipeline)
- **Schedule**: Every Monday at 2:00 AM
- **Runs**: Complete pipeline (sync → transform → email)
- **Enable**: Go to Schedules → "weekly_report_schedule" → Turn On

### 2. Daily Data Refresh
- **Schedule**: Every day at 3:00 AM  
- **Runs**: Only Airbyte syncs (no emails)
- **Enable**: Go to Schedules → "daily_data_refresh" → Turn On

## Make.com Setup

1. In your Make.com scenario, **replace the Schedule trigger** with:
   - Module: **Webhooks → Custom Webhook**
   - Copy the webhook URL

2. Add webhook URL to `.env`:
   ```bash
   MAKE_WEBHOOK_URL=https://hook.eu1.make.com/xxxxx
   ```

3. Rest of Make.com workflow stays the same:
   - Query BigQuery for manager/team data
   - Generate PDF reports
   - Send emails

## Monitoring & Debugging

### View Data Lineage
- **Assets Tab** → Click any asset → See upstream/downstream dependencies
- Example: `trigger_make_weekly_reports` depends on `realpars_dbt_models`

### Check Run Status
- **Runs Tab** → See all pipeline executions
- Green = Success, Red = Failed, Yellow = In Progress

### View Logs
- Click any asset during/after run → **View Logs**
- See detailed output from Airbyte, dbt, or Make.com trigger

### Common Issues

**Airbyte sync fails:**
- Check connection IDs in `.env` match Airbyte Cloud
- Verify `AIRBYTE_API_TOKEN` is valid
- Check Airbyte Cloud UI for connection status

**dbt models fail:**
- Ensure raw tables exist in BigQuery
- Check `GOOGLE_APPLICATION_CREDENTIALS` is set
- Run `dbt debug` to verify connection

**Make.com not triggered:**
- Verify `MAKE_WEBHOOK_URL` is set correctly
- Check webhook is active in Make.com
- View logs for HTTP response code

## Testing

Test the pipeline locally before scheduling:

```bash
# 1. Start Dagster UI
dagster dev -f dagster_project/definitions.py

# 2. Materialize assets one by one:
#    - First: Airbyte assets (check BigQuery for data)
#    - Second: dbt models (check reporting tables)
#    - Third: Make.com trigger (check email received)
```

## Production Deployment

For production deployment, consider:

1. **Dagster Cloud**: Deploy to Dagster Cloud for managed hosting
2. **Docker**: Use the included `DockerFile` to containerize
3. **Cloud Run**: Deploy to Google Cloud Run with scheduled triggers
4. **VM**: Run on a VM with systemd service

See [Dagster Deployment Docs](https://docs.dagster.io/deployment) for details.

## Benefits Over Previous Approach

✅ **Single orchestration** - One UI instead of Cloud Scheduler + manual scripts  
✅ **Data lineage** - See which dbt models depend on which Airbyte sources  
✅ **Retry logic** - Auto-retry failed steps without manual intervention  
✅ **Better monitoring** - Real-time logs and status tracking  
✅ **Easier testing** - Test full pipeline locally before deploying  
✅ **Scheduling** - Built-in cron scheduler with UI controls  

## Support

For issues:
1. Check logs in Dagster UI
2. Review error messages in terminal
3. Verify environment variables are set correctly
4. Test individual components (Airbyte, dbt, Make.com) separately
