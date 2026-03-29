"""
Make.com Trigger Assets

This module defines Dagster assets that trigger Make.com webhooks for:
1. Weekly team engagement reports (sent Monday 8 AM EU time)
2. Monthly course completion reports (sent at beginning of each month)

The Make.com workflows:
- Query BigQuery for manager and team data
- Generate PDF reports with engagement/completion metrics
- Send personalized emails to each manager
"""

import requests
from dagster import asset, AssetExecutionContext
from typing import Dict, Any
import logging
from datetime import datetime

from configs.config import MAKE_WEBHOOK_WEEKLY_REPORTS, MAKE_WEBHOOK_MONTHLY_COURSE_COMPLETION

logger = logging.getLogger(__name__)


@asset(
    name="trigger_make_weekly_reports",
    deps=["realpars_dbt_models"],
    description="Trigger Make.com webhook to send weekly team engagement reports via email (Monday 8 AM EU time)",
    group_name="email_automation",
)
def trigger_make_weekly_reports(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Trigger Make.com workflow to send weekly team engagement reports.
    
    This runs after dbt models complete and populate the reporting tables
    that Make.com queries (manager_team_detail_pdf_makecom, etc.).
    
    Scheduled to run: Monday 8 AM EU time
    
    Returns:
        Dict with status and response details
    """
    
    if not MAKE_WEBHOOK_WEEKLY_REPORTS:
        context.log.warning("MAKE_WEBHOOK_WEEKLY_REPORTS not configured, skipping email trigger")
        return {
            "status": "skipped",
            "reason": "No webhook URL configured"
        }
    
    context.log.info("Triggering Make.com weekly report workflow...")
    context.log.info(f"Webhook URL: {MAKE_WEBHOOK_WEEKLY_REPORTS[:50]}...")
    
    try:
        payload = {
            "trigger_source": "dagster_pipeline",
            "pipeline_run_id": context.run.run_id,
            "timestamp": datetime.now().isoformat(),
            "action": "send_weekly_reports",
            "report_type": "weekly_engagement",
        }
        
        context.log.info(f"Sending webhook request with payload: {payload}")
        
        response = requests.post(
            MAKE_WEBHOOK_WEEKLY_REPORTS,
            json=payload,
            timeout=30,
            headers={"Content-Type": "application/json"}
        )
        
        response.raise_for_status()
        
        context.log.info(f"✓ Make.com weekly reports triggered successfully: HTTP {response.status_code}")
        
        response_data = None
        if response.text:
            try:
                response_data = response.json()
                context.log.info(f"Response: {response_data}")
            except Exception:
                context.log.info(f"Response text: {response.text[:200]}")
        
        return {
            "status": "success",
            "status_code": response.status_code,
            "response": response_data,
            "timestamp": payload["timestamp"],
            "report_type": "weekly_engagement",
        }
        
    except requests.exceptions.Timeout:
        context.log.error("❌ Make.com webhook request timed out after 30 seconds")
        raise
        
    except requests.exceptions.HTTPError as e:
        context.log.error(f"❌ Make.com webhook returned error: {e}")
        context.log.error(f"Response: {e.response.text if e.response else 'No response'}")
        raise
        
    except Exception as e:
        context.log.error(f"❌ Failed to trigger Make.com webhook: {e}")
        raise


@asset(
    name="trigger_make_monthly_course_completion",
    deps=["realpars_dbt_models"],
    description="Trigger Make.com webhook to send monthly course completion reports via email (1st of each month)",
    group_name="email_automation",
)
def trigger_make_monthly_course_completion(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Trigger Make.com workflow to send monthly course completion reports.
    
    This runs after dbt models complete and populate the course completion tables
    that Make.com queries.
    
    Scheduled to run: 1st of each month
    
    Returns:
        Dict with status and response details
    """
    
    if not MAKE_WEBHOOK_MONTHLY_COURSE_COMPLETION:
        context.log.warning("MAKE_WEBHOOK_MONTHLY_COURSE_COMPLETION not configured, skipping email trigger")
        return {
            "status": "skipped",
            "reason": "No webhook URL configured"
        }
    
    context.log.info("Triggering Make.com monthly course completion workflow...")
    context.log.info(f"Webhook URL: {MAKE_WEBHOOK_MONTHLY_COURSE_COMPLETION[:50]}...")
    
    try:
        payload = {
            "trigger_source": "dagster_pipeline",
            "pipeline_run_id": context.run.run_id,
            "timestamp": datetime.now().isoformat(),
            "action": "send_monthly_course_completion",
            "report_type": "monthly_course_completion",
        }
        
        context.log.info(f"Sending webhook request with payload: {payload}")
        
        response = requests.post(
            MAKE_WEBHOOK_MONTHLY_COURSE_COMPLETION,
            json=payload,
            timeout=30,
            headers={"Content-Type": "application/json"}
        )
        
        response.raise_for_status()
        
        context.log.info(f"✓ Make.com monthly course completion triggered successfully: HTTP {response.status_code}")
        
        response_data = None
        if response.text:
            try:
                response_data = response.json()
                context.log.info(f"Response: {response_data}")
            except Exception:
                context.log.info(f"Response text: {response.text[:200]}")
        
        return {
            "status": "success",
            "status_code": response.status_code,
            "response": response_data,
            "timestamp": payload["timestamp"],
            "report_type": "monthly_course_completion",
        }
        
    except requests.exceptions.Timeout:
        context.log.error("❌ Make.com webhook request timed out after 30 seconds")
        raise
        
    except requests.exceptions.HTTPError as e:
        context.log.error(f"❌ Make.com webhook returned error: {e}")
        context.log.error(f"Response: {e.response.text if e.response else 'No response'}")
        raise
        
    except Exception as e:
        context.log.error(f"❌ Failed to trigger Make.com webhook: {e}")
        raise
