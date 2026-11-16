{{
    config(
        materialized='view',
        schema='bi_reports'
    )
}}

/*
    Team member weekly activity report for BI consumption.

    This model is intentionally a thin downstream projection of
    manager_weekly_team_detail_report_all_weeks so the dependency stays explicit.
*/

select
    coalesce(nullif(trim(team_member), ''), 'Has only a Manager') as team_member_name,
    live_classes_attended,
    lessons_completed,
    comments_made,
    comments_received,
    likes_received,
    community_interactions,
    week_total_engagement as total_points_this_week,
    week_start_date,
    week_end_date,
    cast(report_week as string) as report_week,
    business_name
from {{ ref('manager_weekly_team_detail_report_all_weeks') }}