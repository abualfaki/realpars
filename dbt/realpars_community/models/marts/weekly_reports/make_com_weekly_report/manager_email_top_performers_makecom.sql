{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: Top Performers Summary
    
    Simplified view showing only the top 3 performers per manager for easy email formatting.
    Use this in Make.com to populate the "Top Performers" section of the email.
    
    Example usage in Make.com:
    SELECT * FROM manager_email_top_performers_makecom
    WHERE manager_email = '{{manager_email}}'
    AND week_start_date = (SELECT MAX(week_start_date) FROM manager_email_report_with_rankings)
    ORDER BY performance_rank
*/

SELECT 
    manager_email,
    manager_full_name,
    business_name,
    week_start_date,
    week_end_date,
    week_start_formatted,
    week_end_formatted,
    
    -- Top performer details
    member_first_name,
    member_full_name,
    total_points,
    performance_rank,
    
    -- Activity breakdown for context
    classes_attended,
    lessons_completed,
    likes_received,
    comments_received,
    comments_made,
    
    -- Change from previous week
    previous_week_points,
    points_change,
    
    report_generated_at

FROM {{ ref('manager_email_report_with_rankings') }}
WHERE is_top_performer = TRUE
ORDER BY manager_email, week_start_date DESC, performance_rank
