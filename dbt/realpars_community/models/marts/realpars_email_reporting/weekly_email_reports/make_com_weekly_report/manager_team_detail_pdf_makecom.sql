{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: Full Team Detail for PDF Generation
    
    Returns complete team breakdown with all metrics (no points) for PDF export.
    Sorted by engagement level to show most active members first.
    
    Example usage in Make.com:
    SELECT * FROM manager_team_detail_pdf_makecom
    WHERE manager_email = '{{manager_email}}'
    AND week_start_date = (SELECT MAX(week_start_date) FROM manager_weekly_team_detail_report)
    ORDER BY week_total_engagement DESC, team_member
*/

SELECT 
    -- Manager & Business Info
    manager_email,
    manager_full_name,
    business_name,
    
    -- Week Info
    week_start_date,
    week_end_date,
    week_start_formatted,
    week_end_formatted,
    
    -- Team Member
    team_member,
    member_email,
    
    -- Current Week Activity
    live_classes_attended,
    lessons_completed,
    likes_received,
    comments_received,
    comments_made,
    week_total_engagement,
    
    -- Previous Week Activity
    previous_week_engagement,
    
    -- Change
    total_engagement_change,
    
    -- Status
    activity_status,
    
    -- Team Summary (for report header/footer)
    total_team_members,
    active_members,
    inactive_members,
    team_total_engagement,
    team_previous_engagement,
    avg_engagement_per_member,
    
    report_generated_at

FROM {{ ref('manager_weekly_team_detail_report') }}
ORDER BY manager_email, week_start_date DESC, week_total_engagement DESC, team_member
