{{
    config(
        materialized='table',
        schema='cc_stg_weekly_reports',
        partition_by={
            "field": "week_start_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['manager_email', 'week_start_date']
    )
}}

/*
    Manager Email Report with Rankings & Week-over-Week Comparison
    
    Purpose: Generate structured data for Make.com automation to send weekly manager emails
    
    Features:
    - Calculates weighted engagement points (5pts for live classes, 1pt for other activities)
    - Ranks team members by total points (Top Performers)
    - Identifies biggest weekly gainers (Top Movers)
    - Generates automated action items
    - Includes both current and previous week data for comparison
    
    Point System:
    - Live classes attended: 5 points each
    - Lessons completed: 1 point each
    - Likes received: 1 point each
    - Comments received: 1 point each
    - Comments made: 1 point each
*/

WITH current_week_engagement AS (
    -- Get the latest complete week's engagement data
    SELECT 
        wer.manager_email,
        wer.manager_full_name,
        wer.manager_first_name,
        wer.manager_last_name,
        wer.business_name,
        wer.week_start_date,
        wer.week_end_date,
        
        -- Team member details
        wer.member_community_id,
        wer.member_email,
        wer.member_first_name,
        wer.member_last_name,
        TRIM(CONCAT(COALESCE(wer.member_first_name, ''), ' ', COALESCE(wer.member_last_name, ''))) AS member_full_name,
        
        -- Raw engagement metrics
        wer.classes_attended,
        wer.lessons_completed,
        wer.likes_received,
        wer.comments_received,
        wer.comments_made,
        
        -- Calculate weighted points
        (wer.classes_attended * 5) AS points_from_classes,
        (wer.lessons_completed * 1) AS points_from_lessons,
        (wer.likes_received * 1) AS points_from_likes_received,
        (wer.comments_received * 1) AS points_from_comments_received,
        (wer.comments_made * 1) AS points_from_comments_made,
        
        -- Calculate total points
        (wer.classes_attended * 5) + 
        (wer.lessons_completed * 1) + 
        (wer.likes_received * 1) + 
        (wer.comments_received * 1) + 
        (wer.comments_made * 1) AS total_points,
        
        wer.has_activity
        
    FROM {{ ref('weekly_team_engagement_report') }} wer
    WHERE wer.week_start_date = (
        SELECT MAX(week_start_date) 
        FROM {{ ref('weekly_team_engagement_report') }}
    )
),

previous_week_engagement AS (
    -- Get previous week's data for comparison
    SELECT 
        wer.manager_email,
        wer.member_community_id,
        wer.week_start_date,
        wer.week_end_date,
        
        -- Calculate total points for previous week
        (wer.classes_attended * 5) + 
        (wer.lessons_completed * 1) + 
        (wer.likes_received * 1) + 
        (wer.comments_received * 1) + 
        (wer.comments_made * 1) AS prev_total_points
        
    FROM {{ ref('weekly_team_engagement_report') }} wer
    WHERE wer.week_start_date = (
        SELECT MAX(week_start_date) - INTERVAL 7 DAY
        FROM {{ ref('weekly_team_engagement_report') }}
    )
),

member_with_comparison AS (
    -- Join current and previous week to calculate changes
    SELECT 
        cw.*,
        COALESCE(pw.prev_total_points, 0) AS previous_week_points,
        cw.total_points - COALESCE(pw.prev_total_points, 0) AS points_change,
        
        -- Rank by total points (Top Performers)
        ROW_NUMBER() OVER (
            PARTITION BY cw.manager_email, cw.week_start_date 
            ORDER BY cw.total_points DESC, cw.member_full_name
        ) AS performance_rank,
        
        -- Rank by points change (Top Movers)
        ROW_NUMBER() OVER (
            PARTITION BY cw.manager_email, cw.week_start_date 
            ORDER BY (cw.total_points - COALESCE(pw.prev_total_points, 0)) DESC, cw.member_full_name
        ) AS mover_rank,
        
        -- Rank inactive members (for re-engagement priority)
        -- Prioritize those who were previously active
        ROW_NUMBER() OVER (
            PARTITION BY cw.manager_email, cw.week_start_date 
            ORDER BY 
                CASE WHEN cw.total_points = 0 THEN 1 ELSE 2 END,
                COALESCE(pw.prev_total_points, 0) DESC,
                cw.member_full_name
        ) AS inactive_rank,
        
        -- Calculate percentile for performance categorization
        PERCENT_RANK() OVER (
            PARTITION BY cw.manager_email, cw.week_start_date 
            ORDER BY cw.total_points
        ) AS performance_percentile
        
    FROM current_week_engagement cw
    LEFT JOIN previous_week_engagement pw
        ON cw.manager_email = pw.manager_email
        AND cw.member_community_id = pw.member_community_id
),

member_with_labels AS (
    -- Categorize members and generate action items
    SELECT 
        *,
        
        -- Performance category
        CASE 
            WHEN total_points = 0 THEN 'no_activity'
            WHEN performance_rank <= 3 THEN 'top_performer'
            WHEN performance_percentile >= 0.7 THEN 'high_performer'
            WHEN performance_percentile >= 0.3 THEN 'average_performer'
            ELSE 'low_performer'
        END AS performance_category,
        
        -- Momentum category
        CASE 
            WHEN mover_rank <= 3 AND points_change > 0 THEN 'top_mover'
            WHEN points_change >= 5 THEN 'improving'
            WHEN points_change <= -5 THEN 'declining'
            WHEN ABS(points_change) < 5 THEN 'stable'
            ELSE 'new_member'
        END AS momentum_category,
        
        -- Generate automated action items (only 3 types)
        CASE 
            -- 1. Celebrate top performers (top 3 with points)
            WHEN performance_rank <= 3 AND total_points > 0 THEN 
                CONCAT('🏆 Celebrate ', member_first_name, ' for leading the team with ', CAST(total_points AS STRING), ' points')
            
            -- 2. Encourage top movers (keep momentum)
            WHEN mover_rank <= 3 AND points_change > 0 THEN 
                CONCAT('📈 Encourage ', member_first_name, ' to keep up the momentum (+', CAST(points_change AS STRING), ' pts this week)')
            
            -- 3. Re-engage top 3 inactive members (prioritize previously active)
            WHEN total_points = 0 AND inactive_rank <= 3 THEN 
                CASE 
                    WHEN previous_week_points > 0 THEN 
                        CONCAT('⚠️ Re-engage ', member_first_name, ' with a lesson or live class invite (was active last week)')
                    ELSE 
                        CONCAT('💬 Re-engage ', member_first_name, ' with a personalized message or invitation')
                END
            
            ELSE NULL
        END AS suggested_action
        
    FROM member_with_comparison
),

manager_team_stats AS (
    -- Use precomputed team stats from business_relationships (handles multi-manager businesses).
    SELECT
        manager_email,
        business_name,
        MAX(team_size) as team_size,
        MAX(business_total_members) as business_total_members
    FROM {{ ref('business_relationships') }}
    GROUP BY
        manager_email,
        business_name
),

team_summary AS (
    -- Aggregate team-level statistics per manager
    SELECT 
        mwl.manager_email,
        mwl.manager_full_name,
        mwl.business_name,
        mwl.week_start_date,
        mwl.week_end_date,
        
        COALESCE(mts.team_size, COUNT(*)) AS team_size,
        COALESCE(mts.business_total_members, COUNT(*)) AS business_total_members,
        COUNT(CASE WHEN mwl.has_activity THEN 1 END) AS active_members,
        COUNT(CASE WHEN mwl.total_points = 0 THEN 1 END) AS inactive_members,
        
        SUM(mwl.total_points) AS team_total_points,
        ROUND(AVG(mwl.total_points), 1) AS team_avg_points,
        MAX(mwl.total_points) AS team_max_points,
        
        SUM(mwl.points_change) AS team_points_change,
        ROUND(AVG(mwl.points_change), 1) AS team_avg_points_change,
        
        -- Engagement breakdown
        SUM(mwl.classes_attended) AS team_classes_attended,
        SUM(mwl.lessons_completed) AS team_lessons_completed,
        SUM(mwl.likes_received) AS team_likes_received,
        SUM(mwl.comments_received) AS team_comments_received,
        SUM(mwl.comments_made) AS team_comments_made
        
    FROM member_with_labels mwl
    LEFT JOIN manager_team_stats mts
        ON mts.manager_email = mwl.manager_email
        AND mts.business_name = mwl.business_name
    GROUP BY 
        mwl.manager_email,
        mwl.manager_full_name,
        mwl.business_name,
        mwl.week_start_date,
        mwl.week_end_date,
        mts.team_size,
        mts.business_total_members
)

-- Final output: Denormalized structure optimized for Make.com consumption
SELECT 
    -- Manager & Business Info
    ml.manager_email,
    ml.manager_full_name,
    ml.manager_first_name,
    ml.manager_last_name,
    ml.business_name,
    
    -- Week Info
    ml.week_start_date,
    ml.week_end_date,
    FORMAT_DATE('%B %d, %Y', ml.week_start_date) AS week_start_formatted,
    FORMAT_DATE('%B %d, %Y', ml.week_end_date) AS week_end_formatted,
    
    -- Team Member Info
    ml.member_community_id,
    ml.member_email,
    ml.member_first_name,
    ml.member_last_name,
    ml.member_full_name,
    
    -- Current Week Metrics
    ml.classes_attended,
    ml.lessons_completed,
    ml.likes_received,
    ml.comments_received,
    ml.comments_made,
    ml.points_from_classes,
    ml.points_from_lessons,
    ml.points_from_likes_received,
    ml.points_from_comments_received,
    ml.points_from_comments_made,
    ml.total_points,
    
    -- Previous Week & Change
    ml.previous_week_points,
    ml.points_change,
    
    -- Rankings & Categories
    ml.performance_rank,
    ml.mover_rank,
    ml.performance_category,
    ml.momentum_category,
    ml.suggested_action,
    
    -- Flags for easy filtering in Make.com
    CASE WHEN ml.performance_rank <= 3 AND ml.total_points > 0 THEN TRUE ELSE FALSE END AS is_top_performer,
    CASE WHEN ml.mover_rank <= 3 AND ml.points_change > 0 THEN TRUE ELSE FALSE END AS is_top_mover,
    CASE WHEN ml.total_points = 0 THEN TRUE ELSE FALSE END AS needs_reengagement,
    
    -- Team Summary (denormalized for convenience)
    ts.team_size,
    ts.business_total_members,
    ts.active_members,
    ts.inactive_members,
    ts.team_total_points,
    ts.team_avg_points,
    ts.team_max_points,
    ts.team_points_change,
    ts.team_avg_points_change,
    ts.team_classes_attended,
    ts.team_lessons_completed,
    ts.team_likes_received,
    ts.team_comments_received,
    ts.team_comments_made,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS report_generated_at

FROM member_with_labels ml
INNER JOIN team_summary ts
    ON ml.manager_email = ts.manager_email
    AND ml.week_start_date = ts.week_start_date

-- Note: ORDER BY removed due to BigQuery partition_by constraint
-- Results are clustered by manager_email and week_start_date for efficient queries
