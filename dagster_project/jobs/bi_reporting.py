"""
BI Reporting Job Definitions

This module defines the pipeline jobs that orchestrate:
1. Airbyte syncs (Circle.so → BigQuery)
2. Core dbt transformations (Raw → Analytics tables)
3. BI reporting dbt assets for weekly and monthly downstream reporting
4. Make.com email and Slack triggers
"""

from dagster import define_asset_job, AssetSelection
import logging

logger = logging.getLogger(__name__)


weekly_report_job = define_asset_job(
    name="weekly_report_pipeline",
    description="Weekly pipeline: Airbyte sync, core dbt, BI weekly activity, weekly email, and Slack inactivity automation.",
    selection=(
        AssetSelection.groups("airbyte_to_bigquery_sync")
        | AssetSelection.keys("realpars_dbt_models")
        | AssetSelection.keys("team_member_weekly_activity_bi_report")
        | AssetSelection.keys("slack_message_report_models")
        | AssetSelection.keys("trigger_make_weekly_reports")
        | AssetSelection.keys("trigger_make_weekly_business_inactivity_report")
    ),
)


monthly_course_completion_job = define_asset_job(
    name="monthly_course_completion_pipeline",
    description="Monthly pipeline: Airbyte sync, core dbt, BI course completion detail, and monthly email automation.",
    selection=(
        AssetSelection.groups("airbyte_to_bigquery_sync")
        | AssetSelection.keys("realpars_dbt_models")
        | AssetSelection.keys("team_member_course_completion_dates_bi_report")
        | AssetSelection.keys("trigger_make_monthly_course_completion")
    ),
)


airbyte_and_dbt_refresh_job = define_asset_job(
    name="airbyte_and_dbt_refresh",
    description="Run all Airbyte sync assets and then the core dbt transformations.",
    selection=AssetSelection.groups("airbyte_to_bigquery_sync") | AssetSelection.keys("realpars_dbt_models"),
)


airbyte_sync_job = define_asset_job(
    name="airbyte_sync_only",
    description="Run only Airbyte syncs to refresh raw data.",
    selection=AssetSelection.groups("airbyte_to_bigquery_sync"),
)


dbt_transform_job = define_asset_job(
    name="dbt_transform_only",
    description="Run only the core dbt transformations.",
    selection=AssetSelection.keys("realpars_dbt_models"),
)


email_trigger_weekly_job = define_asset_job(
    name="email_trigger_weekly_only",
    description="Trigger the weekly Make.com email workflow only.",
    selection=AssetSelection.keys("trigger_make_weekly_reports"),
)


email_trigger_monthly_job = define_asset_job(
    name="email_trigger_monthly_only",
    description="Trigger the monthly Make.com course completion email workflow only.",
    selection=AssetSelection.keys("trigger_make_monthly_course_completion"),
)


bi_reporting_weekly_job = define_asset_job(
    name="bi_reporting_weekly_only",
    description="Run the weekly BI reporting asset alongside its upstream core dbt dependency.",
    selection=AssetSelection.keys("realpars_dbt_models") | AssetSelection.keys("team_member_weekly_activity_bi_report"),
)


bi_reporting_monthly_job = define_asset_job(
    name="bi_reporting_monthly_only",
    description="Run the monthly BI reporting asset alongside its upstream core dbt dependency.",
    selection=AssetSelection.keys("realpars_dbt_models") | AssetSelection.keys("team_member_course_completion_dates_bi_report"),
)


slack_message_job = define_asset_job(
    name="slack_message_only",
    description="Run the Slack inactivity report model and trigger the Slack Make.com webhook.",
    selection=(
        AssetSelection.keys("realpars_dbt_models")
        | AssetSelection.keys("slack_message_report_models")
        | AssetSelection.keys("trigger_make_weekly_business_inactivity_report")
    ),
)