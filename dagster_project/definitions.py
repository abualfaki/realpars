"""
Dagster Definitions - RealPars Community Analytics Pipeline

This is the main entry point for the Dagster code location.
It brings together all assets, jobs, schedules, and resources.

Pipeline Orchestration:
- Weekly Reports: Airbyte → dbt → Email (Monday 8 AM EU time)
- Monthly Course Completion: Airbyte → dbt → Email (1st of month, 9 AM EU time)
- Daily Data Refresh: Airbyte sync only (Midnight EU time daily)

To run:
    dagster dev -f dagster_project/definitions.py
"""

from dagster import Definitions, ScheduleDefinition
import logging

# Import all modules
from dagster_project.assets import airbyte_assets, dbt_assets, make_dot_com_trigger
from dagster_project.resources.airbyte_resources import airbyte_resource
from dagster_project.resources.dbt_resource import dbt_resource
from dagster_project.jobs.weekly_and_monthly_email_report_job import (
    weekly_report_job,
    monthly_course_completion_job,
    airbyte_sync_job,
    dbt_transform_job,
    email_trigger_weekly_job,
    email_trigger_monthly_job,
)

logger = logging.getLogger(__name__)

# Load all assets from modules
all_assets = [
    *airbyte_assets.airbyte_assets,  # Airbyte sync assets (Circle.so → BigQuery)
    dbt_assets.realpars_dbt_models,   # dbt transformation asset (Raw → Analytics)
    make_dot_com_trigger.trigger_make_weekly_reports,  # Weekly email trigger
    make_dot_com_trigger.trigger_make_monthly_course_completion,  # Monthly email trigger
]

# Schedule 1: Weekly report pipeline - Monday 8 AM EU time (7 AM UTC)
weekly_report_schedule = ScheduleDefinition(
    name="weekly_report_schedule",
    job=weekly_report_job,
    cron_schedule="0 7 * * 1",  # Monday 7 AM UTC = 8 AM CET
    description="Run weekly report pipeline every Monday at 8 AM EU time",
)

# Schedule 2: Monthly course completion - 1st of each month at 9 AM EU time (8 AM UTC)
monthly_course_completion_schedule = ScheduleDefinition(
    name="monthly_course_completion_schedule",
    job=monthly_course_completion_job,
    cron_schedule="0 8 1 * *",  # 1st of month at 8 AM UTC = 9 AM CET
    description="Run monthly course completion pipeline on 1st of each month at 9 AM EU time",
)

# Schedule 3: Weekly data refresh - Friday at midnight EU time (11 PM UTC Thursday)
daily_refresh_schedule = ScheduleDefinition(
    name="weekly_data_refresh",
    job=airbyte_sync_job,
    cron_schedule="0 23 * * 4",  # Thursday 11 PM UTC = Friday midnight CET
    description="Sync Circle.so data every Friday at midnight EU time",
)

# Main Dagster definitions
defs = Definitions(
    assets=all_assets,
    jobs=[
        weekly_report_job,
        monthly_course_completion_job,
        airbyte_sync_job,
        dbt_transform_job,
        email_trigger_weekly_job,
        email_trigger_monthly_job,
    ],
    schedules=[
        weekly_report_schedule,
        monthly_course_completion_schedule,
        daily_refresh_schedule,
    ],
    resources={
        "airbyte": airbyte_resource,
        "dbt": dbt_resource,
    },
)

logger.info("✓ Dagster definitions loaded successfully")
logger.info(f"  - {len(all_assets)} assets")
logger.info(f"  - 6 jobs (2 pipelines, 4 components)")
logger.info(f"  - 3 schedules (weekly, monthly, daily)")
