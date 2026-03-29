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
    Manager Weekly Team Detail Report - PDF Export
    
    Purpose: Generate detailed weekly engagement report for PDF export to business managers
    
    Features:
    - Complete team member breakdown with raw activity counts (no points)
    - Current and previous week comparison
    - Designed for PDF generation and download
    
    Metrics included (raw counts):
    - Live classes attended
    - Lessons completed
    - Likes received
    - Comments received
    - Comments made
    - Total engagement (sum of all activities)
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
        
        -- Raw engagement metrics (current week)
        wer.classes_attended,
        wer.lessons_completed,
        wer.likes_received,
        wer.comments_received,
        wer.comments_made,
        
        -- Total engagement (sum of all activities)
        (wer.classes_attended + 
         wer.lessons_completed + 
         wer.likes_received + 
         wer.comments_received + 
         wer.comments_made) AS week_total_engagement,
        
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
        wer.week_start_date AS previous_week_start_date,
        wer.week_end_date AS previous_week_end_date,
        
        -- Previous week metrics
        wer.classes_attended AS prev_classes_attended,
        wer.lessons_completed AS prev_lessons_completed,
        wer.likes_received AS prev_likes_received,
        wer.comments_received AS prev_comments_received,
        wer.comments_made AS prev_comments_made,
        
        -- Previous week total engagement
        (wer.classes_attended + 
         wer.lessons_completed + 
         wer.likes_received + 
         wer.comments_received + 
         wer.comments_made) AS previous_week_engagement
        
    FROM {{ ref('weekly_team_engagement_report') }} wer
    WHERE wer.week_start_date = (
        SELECT MAX(week_start_date) - INTERVAL 7 DAY
        FROM {{ ref('weekly_team_engagement_report') }}
    )
),

team_member_detail AS (
    -- Combine current and previous week data
    SELECT 
        cw.manager_email,
        cw.manager_full_name,
        cw.manager_first_name,
        cw.manager_last_name,
        cw.business_name,
        
        -- Week information
        cw.week_start_date,
        cw.week_end_date,
        FORMAT_DATE('%B %d, %Y', cw.week_start_date) AS week_start_formatted,
        FORMAT_DATE('%B %d, %Y', cw.week_end_date) AS week_end_formatted,
        
        -- Team member information
        cw.member_community_id,
        cw.member_email,
        cw.member_first_name,
        cw.member_last_name,
        cw.member_full_name,
        
        -- Current week metrics
        cw.classes_attended AS live_classes_attended,
        cw.lessons_completed,
        cw.likes_received,
        cw.comments_received,
        cw.comments_made,
        cw.week_total_engagement,
        
        -- Previous week metrics
        COALESCE(pw.prev_classes_attended, 0) AS prev_live_classes_attended,
        COALESCE(pw.prev_lessons_completed, 0) AS prev_lessons_completed,
        COALESCE(pw.prev_likes_received, 0) AS prev_likes_received,
        COALESCE(pw.prev_comments_received, 0) AS prev_comments_received,
        COALESCE(pw.prev_comments_made, 0) AS prev_comments_made,
        COALESCE(pw.previous_week_engagement, 0) AS previous_week_engagement,
        
        -- Week-over-week changes
        cw.classes_attended - COALESCE(pw.prev_classes_attended, 0) AS classes_change,
        cw.lessons_completed - COALESCE(pw.prev_lessons_completed, 0) AS lessons_change,
        cw.likes_received - COALESCE(pw.prev_likes_received, 0) AS likes_received_change,
        cw.comments_received - COALESCE(pw.prev_comments_received, 0) AS comments_received_change,
        cw.comments_made - COALESCE(pw.prev_comments_made, 0) AS comments_made_change,
        cw.week_total_engagement - COALESCE(pw.previous_week_engagement, 0) AS total_engagement_change,
        
        -- Activity status
        cw.has_activity,
        CASE 
            WHEN cw.has_activity THEN 'Active'
            ELSE 'Inactive'
        END AS activity_status
        
    FROM current_week_engagement cw
    LEFT JOIN previous_week_engagement pw
        ON cw.manager_email = pw.manager_email
        AND cw.member_community_id = pw.member_community_id
),

team_summary AS (
    -- Calculate team-level statistics
    SELECT 
        manager_email,
        week_start_date,
        
        COUNT(*) AS total_team_members,
        COUNT(CASE WHEN has_activity THEN 1 END) AS active_members,
        COUNT(CASE WHEN NOT has_activity THEN 1 END) AS inactive_members,
        
        -- Current week totals
        SUM(live_classes_attended) AS team_classes_attended,
        SUM(lessons_completed) AS team_lessons_completed,
        SUM(likes_received) AS team_likes_received,
        SUM(comments_received) AS team_comments_received,
        SUM(comments_made) AS team_comments_made,
        SUM(week_total_engagement) AS team_total_engagement,
        
        -- Previous week totals
        SUM(previous_week_engagement) AS team_previous_engagement,
        
        -- Averages
        ROUND(AVG(week_total_engagement), 1) AS avg_engagement_per_member,
        ROUND(AVG(previous_week_engagement), 1) AS avg_previous_engagement,
        
        -- Max values
        MAX(week_total_engagement) AS highest_individual_engagement
        
    FROM team_member_detail
    GROUP BY manager_email, week_start_date
)

-- Final output: Complete team detail for PDF report
SELECT 
    -- Manager & Business Info
    tm.manager_email,
    tm.manager_full_name,
    tm.manager_first_name,
    tm.manager_last_name,
    tm.business_name,
    
    -- Week Info
    tm.week_start_date,
    tm.week_end_date,
    tm.week_start_formatted,
    tm.week_end_formatted,
    
    -- Team Member Info
    tm.member_community_id,
    tm.member_email,
    tm.member_first_name,
    tm.member_last_name,
    tm.member_full_name AS team_member,
    
    -- Current Week Engagement (Raw Counts)
    tm.live_classes_attended,
    tm.lessons_completed,
    tm.likes_received,
    tm.comments_received,
    tm.comments_made,
    tm.week_total_engagement,
    
    -- Previous Week Engagement
    tm.prev_live_classes_attended,
    tm.prev_lessons_completed,
    tm.prev_likes_received,
    tm.prev_comments_received,
    tm.prev_comments_made,
    tm.previous_week_engagement,
    
    -- Week-over-Week Changes
    tm.classes_change,
    tm.lessons_change,
    tm.likes_received_change,
    tm.comments_received_change,
    tm.comments_made_change,
    tm.total_engagement_change,
    
    -- Status
    tm.activity_status,
    tm.has_activity,
    
    -- Team Summary (denormalized for report header)
    ts.total_team_members,
    ts.active_members,
    ts.inactive_members,
    ts.team_classes_attended,
    ts.team_lessons_completed,
    ts.team_likes_received,
    ts.team_comments_received,
    ts.team_comments_made,
    ts.team_total_engagement,
    ts.team_previous_engagement,
    ts.avg_engagement_per_member,
    ts.avg_previous_engagement,
    ts.highest_individual_engagement,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS report_generated_at

FROM team_member_detail tm
INNER JOIN team_summary ts
    ON tm.manager_email = ts.manager_email
    AND tm.week_start_date = ts.week_start_date

-- Note: No ORDER BY due to BigQuery partition_by constraint
-- Results are clustered by manager_email and week_start_date
-- Add ORDER BY in your PDF generation query
