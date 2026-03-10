{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: Action Points
    
    Simplified view showing generated action items for each manager.
    Use this in Make.com to populate the "Action Points" section of the email.
    
    Example usage in Make.com:
    SELECT * FROM manager_email_action_points_makecom
    WHERE manager_email = '{{manager_email}}'
    AND week_start_date = (SELECT MAX(week_start_date) FROM manager_email_report_with_rankings)
    AND suggested_action IS NOT NULL
    ORDER BY action_priority
*/

SELECT 
    manager_email,
    manager_full_name,
    business_name,
    week_start_date,
    week_end_date,
    
    -- Team member and action
    member_first_name,
    member_full_name,
    member_email,
    suggested_action,
    
    -- Context for the action
    performance_category,
    momentum_category,
    total_points,
    points_change,
    
    -- Priority sorting (most important actions first)
    CASE 
        WHEN performance_rank = 1 THEN 1  -- Celebrate top performer
        WHEN is_top_mover THEN 2  -- Encourage top movers
        WHEN needs_reengagement AND previous_week_points > 0 THEN 3  -- Re-engage former active members
        WHEN needs_reengagement THEN 4  -- Check in with inactive members
        WHEN points_change <= -10 THEN 5  -- Follow up on declining members
        ELSE 6  -- General acknowledgments
    END AS action_priority,
    
    report_generated_at

FROM {{ ref('manager_email_report_with_rankings') }}
WHERE suggested_action IS NOT NULL
ORDER BY manager_email, week_start_date DESC, action_priority, member_full_name
