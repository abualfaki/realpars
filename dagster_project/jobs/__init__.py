"""
Jobs Module

Contains all Dagster job definitions:
- weekly_report_job: Complete weekly pipeline
- monthly_course_completion_job: Complete monthly pipeline
- airbyte_sync_job: Data sync only
- dbt_transform_job: Transformation only
- email_trigger_weekly_job: Weekly email trigger only
- email_trigger_monthly_job: Monthly email trigger only
"""

from .weekly_and_monthly_email_report_job import (
    weekly_report_job,
    monthly_course_completion_job,
    airbyte_sync_job,
    dbt_transform_job,
    email_trigger_weekly_job,
    email_trigger_monthly_job,
)

__all__ = [
    "weekly_report_job",
    "monthly_course_completion_job",
    "airbyte_sync_job",
    "dbt_transform_job",
    "email_trigger_weekly_job",
    "email_trigger_monthly_job",
]
