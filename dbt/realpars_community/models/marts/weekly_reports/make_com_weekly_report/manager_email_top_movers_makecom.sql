{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: Top Movers Summary
    
    Simplified view showing only the top movers (biggest weekly gainers) per manager.
    Use this in Make.com to populate the "Top Movers" section of the email.
    
    Example usage in Make.com:
    SELECT * FROM manager_email_top_movers_makecom
    WHERE manager_email = '{{manager_email}}'
    AND week_start_date = (SELECT MAX(week_start_date) FROM manager_email_report_with_rankings)
    ORDER BY mover_rank
*/

SELECT 
    manager_email,
    manager_full_name,
    business_name,
    week_start_date,
    week_end_date,
    
    -- Top mover details
    member_first_name,
    member_full_name,
    total_points,
    previous_week_points,
    points_change,
    mover_rank,
    
    -- Activity breakdown
    classes_attended,
    lessons_completed,
    
    report_generated_at

FROM {{ ref('manager_email_report_with_rankings') }}
WHERE is_top_mover = TRUE
  AND points_change > 0  -- Only show positive movers
ORDER BY manager_email, week_start_date DESC, mover_rank
