# Incremental Weekly Member Engagement - Implementation Guide

## Overview

This guide explains the new incremental implementation of `int_weekly_member_engagement` and how to migrate from the full-refresh table approach.

## Files

- **Current (Table):** `int_weekly_member_engagement.sql` - Full refresh on every run
- **New (Incremental):** `int_weekly_member_engagement_incremental.sql` - Efficient incremental updates

## Key Changes

### Configuration

```sql
-- OLD (Table)
materialized='table'

-- NEW (Incremental)
materialized='incremental'
incremental_strategy='merge'
unique_key=['community_member_id', 'week_start_date']
partition_by='week_start_date' (monthly)
cluster_by=['community_member_id', 'week_start_date']
```

### How Incremental Logic Works

#### Full Refresh Mode (`--full-refresh` flag)
- Builds complete history for all members from their `user_profile_created_at` date
- Uses existing logic: cross join members with all activity weeks
- Sets `is_backfill=false` for all rows

#### Incremental Mode (default)
Processes two groups separately then merges:

**1. New Members (Full Backfill)**
- Detects members not in existing table OR who joined in last 4 weeks
- Generates complete weekly spine from their join date to current week
- Cross joins new members with their historical weeks
- Sets `is_backfill=true` for these rows

**2. Existing Members (Recent Weeks Only)**
- Processes only last 4 weeks + current week
- Updates recent weeks to catch late-arriving activity data
- Sets `is_backfill=false` for these rows

**Why 4-week lookback?**
- Airbyte syncs may be delayed
- Activity data (comments, likes, completions) can arrive late
- Ensures data quality without full recalculation

### New Columns

| Column | Type | Description |
|--------|------|-------------|
| `is_backfill` | BOOLEAN | `true` for new member historical rows, `false` for normal updates |
| `dbt_updated_at` | TIMESTAMP | When this row was last processed by dbt |

## Migration Steps

### Step 1: Test the New Model

Run as a separate model first to validate:

```bash
# Full refresh test
source .venv/bin/activate
cd dbt/realpars_community
python3 run_dbt.py run --select int_weekly_member_engagement_incremental --full-refresh
```

**Validate results:**
```sql
-- Check row counts match
SELECT COUNT(*) FROM cc_intermediate_transformations.int_weekly_member_engagement;
SELECT COUNT(*) FROM cc_intermediate_transformations.int_weekly_member_engagement_incremental;

-- Verify no gaps in member weeks
SELECT 
    community_member_id,
    MIN(week_start_date) as first_week,
    MAX(week_start_date) as last_week,
    COUNT(*) as total_weeks,
    COUNT(DISTINCT week_start_date) as distinct_weeks
FROM cc_intermediate_transformations.int_weekly_member_engagement_incremental
GROUP BY community_member_id
HAVING COUNT(*) != COUNT(DISTINCT week_start_date);  -- Should return 0 rows
```

### Step 2: Test Incremental Behavior

```bash
# Run incremental (simulates weekly pipeline run)
python3 run_dbt.py run --select int_weekly_member_engagement_incremental

# Check execution time (should be significantly faster)
```

**Validate incremental logic:**
```sql
-- Check recent weeks were reprocessed
SELECT 
    week_start_date,
    COUNT(*) as member_count,
    MAX(dbt_updated_at) as last_updated
FROM cc_intermediate_transformations.int_weekly_member_engagement_incremental
WHERE week_start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
GROUP BY week_start_date
ORDER BY week_start_date DESC;

-- Verify new members get is_backfill=true
SELECT 
    community_member_id,
    MIN(week_start_date) as first_week,
    COUNT(*) as total_weeks,
    COUNT(CASE WHEN is_backfill THEN 1 END) as backfill_weeks
FROM cc_intermediate_transformations.int_weekly_member_engagement_incremental
GROUP BY community_member_id
ORDER BY MIN(user_profile_created_at) DESC
LIMIT 10;
```

### Step 3: Test New Member Scenario

**Option A: Use existing recent member**
```sql
-- Find a member who joined recently
SELECT community_member_id, email, user_profile_created_at
FROM circle_community_clean_staging.clean_communtity_members_table
WHERE user_profile_created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY user_profile_created_at DESC
LIMIT 5;

-- Delete them from incremental table
DELETE FROM cc_intermediate_transformations.int_weekly_member_engagement_incremental
WHERE community_member_id = 'MEMBER_ID_HERE';

-- Run incremental
python3 run_dbt.py run --select int_weekly_member_engagement_incremental

-- Verify they have all historical weeks
SELECT week_start_date, is_backfill, dbt_updated_at
FROM cc_intermediate_transformations.int_weekly_member_engagement_incremental
WHERE community_member_id = 'MEMBER_ID_HERE'
ORDER BY week_start_date;
```

### Step 4: Cutover to Production

Once validated:

```bash
# Backup current table (optional)
bq cp \
  circle-analytics-468017:cc_intermediate_transformations.int_weekly_member_engagement \
  circle-analytics-468017:cc_intermediate_transformations.int_weekly_member_engagement_backup_$(date +%Y%m%d)

# Option 1: Rename files (recommended)
cd models/intermediate
mv int_weekly_member_engagement.sql int_weekly_member_engagement_old.sql
mv int_weekly_member_engagement_incremental.sql int_weekly_member_engagement.sql

# Option 2: Replace content
# Copy content from _incremental.sql to the original file

# Run full refresh to build initial incremental table
python3 run_dbt.py run --select int_weekly_member_engagement --full-refresh
```

### Step 5: Update Downstream Dependencies

Check what depends on this model:

```bash
dbt ls --select int_weekly_member_engagement+ --exclude int_weekly_member_engagement
```

Verify downstream models handle new columns (`is_backfill`, `dbt_updated_at`). Most should work without changes.

## Weekly Pipeline Integration

### Recommended Run Command

```bash
# In your ETL pipeline
python3 run_dbt.py run --select intermediate
```

This will:
1. Update individual metric tables (int_weekly_classes_attended, etc.)
2. Add new week to engagement table for all existing members
3. Backfill any new members discovered in community members table
4. Reprocess last 4 weeks for data quality

### For Weekly Email Campaign

Query the most recent completed week:

```sql
SELECT 
    community_member_id,
    email,
    first_name,
    last_name,
    week_start_date,
    week_end_date,
    classes_attended,
    courses_completed,
    lessons_completed,
    has_activity
FROM cc_intermediate_transformations.int_weekly_member_engagement
WHERE week_start_date = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY))
  AND has_activity = true  -- Only members with engagement
ORDER BY community_member_id;
```

## Performance Expectations

### Full Refresh
- **Initial run:** 30-60 seconds (depends on data volume)
- **Frequency:** Only needed for schema changes or data corrections

### Incremental Run
- **Weekly run:** 5-15 seconds
- **Processing:** ~4 weeks × all members + new member backfills
- **Data scanned:** 80-90% reduction vs full refresh

### Cost Optimization
- Partitioned by `week_start_date` (monthly partitions)
- Clustered by `community_member_id, week_start_date`
- Queries filtering by week or member will be very efficient

## Troubleshooting

### Issue: New member missing historical weeks

**Diagnosis:**
```sql
SELECT * FROM cc_intermediate_transformations.int_weekly_member_engagement
WHERE community_member_id = 'MEMBER_ID'
ORDER BY week_start_date;
```

**Fix:** Run with full-refresh or check if member's `user_profile_created_at` is properly set.

### Issue: Incremental run very slow

**Possible causes:**
1. Too many new members (lots of backfills)
2. Partition pruning not working

**Check:**
```sql
-- See how many new members being processed
SELECT COUNT(DISTINCT community_member_id) 
FROM circle_community_clean_staging.clean_communtity_members_table
WHERE user_profile_created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK);
```

**Adjust:** Change lookback window from 4 weeks to 2 weeks in the model if needed.

### Issue: Gaps in week coverage

**Diagnosis:**
```sql
-- Find members with week gaps
WITH member_weeks AS (
    SELECT 
        community_member_id,
        week_start_date,
        LAG(week_start_date) OVER (PARTITION BY community_member_id ORDER BY week_start_date) as prev_week
    FROM cc_intermediate_transformations.int_weekly_member_engagement
)
SELECT 
    community_member_id,
    prev_week,
    week_start_date,
    DATE_DIFF(week_start_date, prev_week, DAY) as gap_days
FROM member_weeks
WHERE DATE_DIFF(week_start_date, prev_week, DAY) > 7
ORDER BY community_member_id, week_start_date;
```

**Fix:** Run full-refresh to rebuild spine consistently.

## Monitoring Queries

### Check incremental run health
```sql
SELECT 
    DATE(dbt_updated_at) as run_date,
    COUNT(*) as rows_updated,
    COUNT(DISTINCT community_member_id) as members_updated,
    COUNT(CASE WHEN is_backfill THEN 1 END) as backfill_rows,
    MIN(week_start_date) as earliest_week,
    MAX(week_start_date) as latest_week
FROM cc_intermediate_transformations.int_weekly_member_engagement
WHERE DATE(dbt_updated_at) = CURRENT_DATE()
GROUP BY DATE(dbt_updated_at);
```

### Track new member backfills
```sql
SELECT 
    DATE(dbt_updated_at) as backfill_date,
    community_member_id,
    email,
    COUNT(*) as weeks_backfilled,
    MIN(week_start_date) as from_week,
    MAX(week_start_date) as to_week
FROM cc_intermediate_transformations.int_weekly_member_engagement
WHERE is_backfill = true
  AND DATE(dbt_updated_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY DATE(dbt_updated_at), community_member_id, email
ORDER BY backfill_date DESC, weeks_backfilled DESC;
```

## Rollback Plan

If issues arise:

```bash
# Restore backup
bq cp \
  circle-analytics-468017:cc_intermediate_transformations.int_weekly_member_engagement_backup_YYYYMMDD \
  circle-analytics-468017:cc_intermediate_transformations.int_weekly_member_engagement

# Revert code
cd models/intermediate
mv int_weekly_member_engagement.sql int_weekly_member_engagement_incremental.sql
mv int_weekly_member_engagement_old.sql int_weekly_member_engagement.sql

# Run full refresh
python3 run_dbt.py run --select int_weekly_member_engagement --full-refresh
```

## Questions?

- Check dbt logs: `dbt/realpars_community/logs/dbt.log`
- Review compiled SQL: `dbt/realpars_community/target/compiled/realpars/models/intermediate/int_weekly_member_engagement.sql`
- Test in dev: Add `--target dev` to run commands
