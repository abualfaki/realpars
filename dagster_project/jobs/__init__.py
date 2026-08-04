"""
Jobs Module

Contains all Dagster job definitions for pipeline, BI reporting, and automation runs.
"""

from .bi_reporting import (
    weekly_report_job,
    monthly_course_completion_job,
    airbyte_and_dbt_refresh_job,
    airbyte_sync_job,
    dbt_transform_job,
    email_trigger_weekly_job,
    email_trigger_monthly_job,
    bi_reporting_weekly_job,
    bi_reporting_monthly_job,
    slack_message_job,
)

__all__ = [
    "weekly_report_job",
    "monthly_course_completion_job",
    "airbyte_and_dbt_refresh_job",
    "airbyte_sync_job",
    "dbt_transform_job",
    "email_trigger_weekly_job",
    "email_trigger_monthly_job",
    "bi_reporting_weekly_job",
    "bi_reporting_monthly_job",
    "slack_message_job",
]
