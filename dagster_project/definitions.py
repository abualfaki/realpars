"""
Dagster Definitions - RealPars Community Analytics Pipeline

This is the main entry point for the Dagster code location.
It brings together all assets, jobs, schedules, and resources.

Pipeline Orchestration:
- Weekly data refresh: Airbyte → dbt (Sunday midnight EU time)
- Weekly email: Make.com weekly reports (Monday 7 AM EU time)
- Monthly data refresh: Airbyte → dbt (last day of the month at midnight EU time)
- Monthly email: Make.com course completion (1st day of the month)

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
    airbyte_and_dbt_refresh_job,
    airbyte_sync_job,
    dbt_transform_job,
    email_trigger_weekly_job,
    email_trigger_monthly_job,
)

logger = logging.getLogger(__name__)

EU_TIMEZONE = "Europe/Brussels"

# Load all assets from modules
all_assets = [
    *airbyte_assets.airbyte_assets,  # Airbyte sync assets (Circle.so → BigQuery)
    dbt_assets.realpars_dbt_models,   # dbt transformation asset (Raw → Analytics)
    make_dot_com_trigger.trigger_make_weekly_reports,  # Weekly email trigger
    make_dot_com_trigger.trigger_make_monthly_course_completion,  # Monthly email trigger
]

# Schedule 1: Airbyte + dbt refresh every Sunday at 00:00 EU time
weekly_data_refresh_schedule = ScheduleDefinition(
    name="weekly_data_refresh_schedule",
    job=airbyte_and_dbt_refresh_job,
    cron_schedule="0 0 * * 0",  # Sunday midnight
    execution_timezone=EU_TIMEZONE,
    description="Run Airbyte syncs and dbt transforms every Sunday at 00:00 EU time",
)

# Schedule 2: Weekly email trigger every Monday at 07:00 EU time
weekly_make_reports_schedule = ScheduleDefinition(
    name="weekly_make_reports_schedule",
    job=email_trigger_weekly_job,
    cron_schedule="0 7 * * 1",
    execution_timezone=EU_TIMEZONE,
    description="Trigger Make.com weekly reports every Monday at 07:00 EU time",
)

# Schedule 3: Airbyte + dbt refresh on the last day of each month at 00:00 EU time
monthly_data_refresh_schedule = ScheduleDefinition(
    name="monthly_data_refresh_schedule",
    job=airbyte_and_dbt_refresh_job,
    cron_schedule="0 0 L * *",
    execution_timezone=EU_TIMEZONE,
    description="Run Airbyte syncs and dbt transforms on the last day of the month at 00:00 EU time",
)

# Schedule 4: Monthly email trigger on the 1st day of each month at 07:00 EU time
monthly_make_reports_schedule = ScheduleDefinition(
    name="monthly_make_reports_schedule",
    job=email_trigger_monthly_job,
    cron_schedule="0 7 1 * *",
    execution_timezone=EU_TIMEZONE,
    description="Trigger Make.com monthly course completion reports on the 1st of each month at 07:00 EU time",
)

# Main Dagster definitions
job_list = [
    weekly_report_job,
    monthly_course_completion_job,
    airbyte_and_dbt_refresh_job,
    airbyte_sync_job,
    dbt_transform_job,
    email_trigger_weekly_job,
    email_trigger_monthly_job,
]

schedule_list = [
    weekly_data_refresh_schedule,
    weekly_make_reports_schedule,
    monthly_data_refresh_schedule,
    monthly_make_reports_schedule,
]

defs = Definitions(
    assets=all_assets,
    jobs=job_list,
    schedules=schedule_list,
    resources={
        "airbyte": airbyte_resource,
        "dbt": dbt_resource,
    },
)

logger.info("✓ Dagster definitions loaded successfully")
logger.info("  - %s assets", len(all_assets))
logger.info("  - %s jobs", len(job_list))
logger.info("  - %s schedules", len(schedule_list))
