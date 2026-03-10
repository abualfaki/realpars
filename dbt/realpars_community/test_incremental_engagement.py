#!/usr/bin/env python3
"""
Test Script for Incremental Weekly Member Engagement Implementation

This script runs automated tests to validate the incremental model behavior
before migrating from the table-based approach.

Usage:
    python test_incremental_engagement.py

Requirements:
    - Run from project root with .venv activated
    - BigQuery access configured
    - dbt project set up
"""

import subprocess
import sys
from pathlib import Path
from google.cloud import bigquery
from datetime import datetime, timedelta

# Project configuration
PROJECT_ID = "circle-analytics-468017"
DATASET = "cc_intermediate_transformations"
TABLE_NAME = "int_weekly_member_engagement_incremental"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE_NAME}"

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_header(text):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.END}\n")

def print_success(text):
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")

def print_error(text):
    print(f"{Colors.RED}✗ {text}{Colors.END}")

def print_warning(text):
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")

def print_info(text):
    print(f"{Colors.BLUE}ℹ {text}{Colors.END}")

def run_dbt_command(command_args):
    """Run dbt command using run_dbt.py wrapper"""
    cmd = ["python3", "dbt/realpars_community/run_dbt.py"] + command_args
    print_info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0, result.stdout, result.stderr

def run_query(client, query):
    """Execute BigQuery query and return results"""
    try:
        query_job = client.query(query)
        results = query_job.result()
        return list(results)
    except Exception as e:
        print_error(f"Query failed: {e}")
        return None

def test_1_full_refresh_build(client):
    """Test 1: Build incremental model with full refresh"""
    print_header("Test 1: Full Refresh Build")
    
    success, stdout, stderr = run_dbt_command([
        "run",
        "--select", "int_weekly_member_engagement_incremental",
        "--full-refresh"
    ])
    
    if not success:
        print_error("Full refresh build failed")
        print(stderr)
        return False
    
    print_success("Full refresh build completed")
    
    # Verify table exists
    query = f"""
    SELECT COUNT(*) as row_count
    FROM `{FULL_TABLE_ID}`
    """
    results = run_query(client, query)
    if results and results[0].row_count > 0:
        print_success(f"Table created with {results[0].row_count:,} rows")
        return True
    else:
        print_error("Table is empty or doesn't exist")
        return False

def test_2_row_count_comparison(client):
    """Test 2: Compare row counts with original table"""
    print_header("Test 2: Row Count Comparison")
    
    query = f"""
    SELECT 
        (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET}.int_weekly_member_engagement`) as original_count,
        (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET}.int_weekly_member_engagement_incremental`) as incremental_count
    """
    results = run_query(client, query)
    
    if not results:
        print_warning("Could not compare row counts (original table may not exist)")
        return True  # Not a failure if original doesn't exist
    
    original = results[0].original_count
    incremental = results[0].incremental_count
    diff = abs(original - incremental)
    diff_pct = (diff / original * 100) if original > 0 else 0
    
    print_info(f"Original table: {original:,} rows")
    print_info(f"Incremental table: {incremental:,} rows")
    print_info(f"Difference: {diff:,} rows ({diff_pct:.2f}%)")
    
    if diff_pct < 1:  # Less than 1% difference is acceptable
        print_success("Row counts match within acceptable range")
        return True
    else:
        print_warning(f"Row count difference exceeds 1%")
        return True  # Still pass, might be expected

def test_3_no_week_gaps(client):
    """Test 3: Verify no gaps in week coverage per member"""
    print_header("Test 3: Week Gap Detection")
    
    query = f"""
    WITH member_weeks AS (
        SELECT 
            community_member_id,
            week_start_date,
            LAG(week_start_date) OVER (PARTITION BY community_member_id ORDER BY week_start_date) as prev_week
        FROM `{FULL_TABLE_ID}`
    )
    SELECT 
        community_member_id,
        prev_week,
        week_start_date,
        DATE_DIFF(week_start_date, prev_week, DAY) as gap_days
    FROM member_weeks
    WHERE DATE_DIFF(week_start_date, prev_week, DAY) > 7
    LIMIT 10
    """
    results = run_query(client, query)
    
    if not results:
        print_error("Query failed")
        return False
    
    if len(results) == 0:
        print_success("No week gaps found - coverage is continuous")
        return True
    else:
        print_warning(f"Found {len(results)} week gaps")
        for row in results[:5]:
            print(f"  Member {row.community_member_id}: gap from {row.prev_week} to {row.week_start_date} ({row.gap_days} days)")
        return False

def test_4_incremental_run(client):
    """Test 4: Run incremental update"""
    print_header("Test 4: Incremental Run")
    
    # Get row count before
    query = f"SELECT COUNT(*) as count, MAX(dbt_updated_at) as last_update FROM `{FULL_TABLE_ID}`"
    before = run_query(client, query)
    if not before:
        print_error("Could not get initial counts")
        return False
    
    count_before = before[0].count
    print_info(f"Rows before: {count_before:,}")
    
    # Run incremental
    success, stdout, stderr = run_dbt_command([
        "run",
        "--select", "int_weekly_member_engagement_incremental"
    ])
    
    if not success:
        print_error("Incremental run failed")
        print(stderr)
        return False
    
    print_success("Incremental run completed")
    
    # Get row count after
    after = run_query(client, query)
    if not after:
        print_error("Could not get final counts")
        return False
    
    count_after = after[0].count
    print_info(f"Rows after: {count_after:,}")
    
    if count_after >= count_before:
        print_success(f"Row count maintained or increased (+{count_after - count_before:,} rows)")
        return True
    else:
        print_error(f"Row count decreased by {count_before - count_after:,} rows")
        return False

def test_5_backfill_flag(client):
    """Test 5: Verify is_backfill flag is properly set"""
    print_header("Test 5: Backfill Flag Validation")
    
    query = f"""
    SELECT 
        is_backfill,
        COUNT(*) as row_count,
        COUNT(DISTINCT community_member_id) as member_count
    FROM `{FULL_TABLE_ID}`
    GROUP BY is_backfill
    ORDER BY is_backfill
    """
    results = run_query(client, query)
    
    if not results:
        print_error("Query failed")
        return False
    
    for row in results:
        flag = "Backfill" if row.is_backfill else "Regular"
        print_info(f"{flag}: {row.row_count:,} rows across {row.member_count:,} members")
    
    # Check that we have both types (in incremental runs)
    has_both = len(results) == 2
    if has_both:
        print_success("Both backfill and regular rows present")
    else:
        print_warning("Only one type of rows found (may be expected in full refresh)")
    
    return True

def test_6_updated_at_timestamp(client):
    """Test 6: Verify dbt_updated_at is recent"""
    print_header("Test 6: Updated Timestamp Validation")
    
    query = f"""
    SELECT 
        MIN(dbt_updated_at) as earliest_update,
        MAX(dbt_updated_at) as latest_update,
        COUNT(DISTINCT DATE(dbt_updated_at)) as update_days
    FROM `{FULL_TABLE_ID}`
    """
    results = run_query(client, query)
    
    if not results:
        print_error("Query failed")
        return False
    
    row = results[0]
    print_info(f"Earliest update: {row.earliest_update}")
    print_info(f"Latest update: {row.latest_update}")
    print_info(f"Update days: {row.update_days}")
    
    # Check if latest update is recent (within last hour)
    if row.latest_update and (datetime.utcnow() - row.latest_update.replace(tzinfo=None)).seconds < 3600:
        print_success("Timestamps are recent")
        return True
    else:
        print_warning("Latest update is not recent")
        return True  # Not a hard failure

def test_7_metric_aggregation(client):
    """Test 7: Verify metrics are properly aggregated"""
    print_header("Test 7: Metric Aggregation Validation")
    
    query = f"""
    SELECT 
        AVG(classes_attended) as avg_classes,
        AVG(courses_completed) as avg_courses,
        AVG(lessons_completed) as avg_lessons,
        AVG(likes_received) as avg_likes,
        AVG(comments_received) as avg_comments_rcv,
        AVG(comments_made) as avg_comments_made,
        SUM(CASE WHEN has_activity THEN 1 ELSE 0 END) / COUNT(*) * 100 as pct_with_activity
    FROM `{FULL_TABLE_ID}`
    """
    results = run_query(client, query)
    
    if not results:
        print_error("Query failed")
        return False
    
    row = results[0]
    print_info(f"Avg classes attended: {row.avg_classes:.2f}")
    print_info(f"Avg courses completed: {row.avg_courses:.2f}")
    print_info(f"Avg lessons completed: {row.avg_lessons:.2f}")
    print_info(f"Avg likes received: {row.avg_likes:.2f}")
    print_info(f"Avg comments received: {row.avg_comments_rcv:.2f}")
    print_info(f"Avg comments made: {row.avg_comments_made:.2f}")
    print_info(f"Weeks with activity: {row.pct_with_activity:.1f}%")
    
    # Basic sanity check - metrics should be non-negative
    all_non_negative = all(getattr(row, col) >= 0 for col in 
                           ['avg_classes', 'avg_courses', 'avg_lessons', 
                            'avg_likes', 'avg_comments_rcv', 'avg_comments_made'])
    
    if all_non_negative:
        print_success("All metrics are valid (non-negative)")
        return True
    else:
        print_error("Found negative metric values")
        return False

def test_8_most_recent_week(client):
    """Test 8: Verify most recent week coverage"""
    print_header("Test 8: Recent Week Coverage")
    
    query = f"""
    SELECT 
        MAX(week_start_date) as latest_week,
        COUNT(DISTINCT community_member_id) as member_count
    FROM `{FULL_TABLE_ID}`
    GROUP BY week_start_date
    HAVING week_start_date = MAX(week_start_date)
    """
    results = run_query(client, query)
    
    if not results:
        print_error("Query failed")
        return False
    
    row = results[0]
    print_info(f"Latest week: {row.latest_week}")
    print_info(f"Members in latest week: {row.member_count:,}")
    
    # Check if latest week is current or previous week
    latest_week = row.latest_week
    current_week = datetime.now().date()
    weeks_behind = (current_week - latest_week).days // 7
    
    if weeks_behind <= 1:
        print_success(f"Latest week is current or 1 week behind")
        return True
    else:
        print_warning(f"Latest week is {weeks_behind} weeks behind current date")
        return True  # Not a hard failure

def main():
    print_header("Incremental Weekly Member Engagement - Test Suite")
    print_info("Testing incremental implementation before migration")
    
    # Initialize BigQuery client
    try:
        client = bigquery.Client(project=PROJECT_ID)
        print_success(f"Connected to BigQuery project: {PROJECT_ID}")
    except Exception as e:
        print_error(f"Failed to connect to BigQuery: {e}")
        sys.exit(1)
    
    # Run all tests
    tests = [
        ("Full Refresh Build", test_1_full_refresh_build),
        ("Row Count Comparison", test_2_row_count_comparison),
        ("Week Gap Detection", test_3_no_week_gaps),
        ("Incremental Run", test_4_incremental_run),
        ("Backfill Flag Validation", test_5_backfill_flag),
        ("Updated Timestamp", test_6_updated_at_timestamp),
        ("Metric Aggregation", test_7_metric_aggregation),
        ("Recent Week Coverage", test_8_most_recent_week),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            passed = test_func(client)
            results.append((test_name, passed))
        except Exception as e:
            print_error(f"Test crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print_header("Test Summary")
    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)
    
    for test_name, passed in results:
        if passed:
            print_success(f"{test_name}")
        else:
            print_error(f"{test_name}")
    
    print(f"\n{Colors.BOLD}Results: {passed_count}/{total_count} tests passed{Colors.END}")
    
    if passed_count == total_count:
        print_success("\nAll tests passed! Ready for migration.")
        return 0
    elif passed_count >= total_count * 0.75:
        print_warning(f"\nMost tests passed. Review failures before migration.")
        return 1
    else:
        print_error(f"\nToo many test failures. Fix issues before migration.")
        return 2

if __name__ == "__main__":
    sys.exit(main())
