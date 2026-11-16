{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: Manager List for Automation
    
    Returns one row per manager with their latest week's summary.
    Use this as the trigger/iterator in Make.com to send one email per manager.
    
    Example usage in Make.com:
    1. Query this view to get list of managers
    2. For each manager_email, query the detail views (top performers, movers, actions)
    3. Populate email template and send
    
    SELECT * FROM manager_email_list_makecom
    WHERE week_start_date = (SELECT MAX(week_start_date) FROM manager_email_report_with_rankings)
*/

SELECT DISTINCT
    manager_email,
    manager_full_name,
    manager_first_name,
    manager_last_name,
    business_name,
    week_start_date,
    week_end_date,
    week_start_formatted,
    week_end_formatted,
    
    -- Team summary
    team_size,
    active_members,
    inactive_members,
    team_total_points,
    team_avg_points,
    team_max_points,
    team_points_change,
    team_avg_points_change,
    
    -- Engagement breakdown
    team_classes_attended,
    team_lessons_completed,
    team_likes_received,
    team_comments_received,
    team_comments_made,
    
    -- Counts for email sections
    (SELECT COUNT(*) FROM {{ ref('manager_email_report_with_rankings') }} sub
     WHERE sub.manager_email = main.manager_email 
     AND sub.week_start_date = main.week_start_date
     AND sub.is_top_performer = TRUE) AS count_top_performers,
     
    (SELECT COUNT(*) FROM {{ ref('manager_email_report_with_rankings') }} sub
     WHERE sub.manager_email = main.manager_email 
     AND sub.week_start_date = main.week_start_date
     AND sub.is_top_mover = TRUE) AS count_top_movers,
     
    (SELECT COUNT(*) FROM {{ ref('manager_email_report_with_rankings') }} sub
     WHERE sub.manager_email = main.manager_email 
     AND sub.week_start_date = main.week_start_date
     AND sub.suggested_action IS NOT NULL) AS count_action_items,
    
    report_generated_at

FROM {{ ref('manager_email_report_with_rankings') }} main
ORDER BY manager_email, week_start_date DESC
