"""
Report Job Definition

This module defines the pipeline jobs that orchestrate:
1. Airbyte syncs (Circle.so → BigQuery)
2. dbt transformations (Raw → Analytics tables)
3. Make.com triggers (Send emails)
   - Weekly reports (Monday 8 AM EU time)
   - Monthly course completion (1st of month)
"""

from dagster import define_asset_job, AssetSelection
import logging

logger = logging.getLogger(__name__)

# Define the complete weekly report pipeline job
weekly_report_job = define_asset_job(
    name="weekly_report_pipeline",
    description="Complete weekly pipeline: Airbyte sync → dbt transform → Make.com weekly email automation",
    selection=AssetSelection.all() - AssetSelection.keys("trigger_make_monthly_course_completion"),
)

# Define the complete monthly course completion pipeline job
monthly_course_completion_job = define_asset_job(
    name="monthly_course_completion_pipeline",
    description="Complete monthly pipeline: Airbyte sync → dbt transform → Make.com monthly course completion report email automation",
    selection=AssetSelection.all() - AssetSelection.keys("trigger_make_weekly_reports"),
)

# Shared job for running Airbyte syncs followed by dbt transformations
airbyte_and_dbt_refresh_job = define_asset_job(
    name="airbyte_and_dbt_refresh",
    description="Run all Airbyte sync assets and then dbt transformations",
    selection=AssetSelection.kind("airbyte") | AssetSelection.assets("realpars_dbt_models"),
)

# Define individual job components (optional - for running steps separately)
airbyte_sync_job = define_asset_job(
    name="airbyte_sync_only",
    description="Run only Airbyte syncs to refresh raw data",
    selection=AssetSelection.groups("airbyte"),  # Only Airbyte assets
)

dbt_transform_job = define_asset_job(
    name="dbt_transform_only", 
    description="Run only dbt transformations (assumes raw data exists)",
    selection=AssetSelection.keys("realpars_dbt_models"),
)

email_trigger_weekly_job = define_asset_job(
    name="email_trigger_weekly_only",
    description="Trigger Make.com weekly report workflow only",
    selection=AssetSelection.keys("trigger_make_weekly_reports"),
)

email_trigger_monthly_job = define_asset_job(
    name="email_trigger_monthly_only",
    description="Trigger Make.com monthly course completion workflow only",
    selection=AssetSelection.keys("trigger_make_monthly_course_completion"),
)
