{{
    config(
        materialized='table',
        schema='stg_weekly_reports',
        partition_by={
            "field": "week_start_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['manager_email']
    )
}}

WITH latest_week AS (
    SELECT MAX(week_start_date) AS latest_week_start
    FROM {{ ref('weekly_team_engagement_report') }}
),

manager_team_data AS (
    SELECT 
        wer.manager_email,
        wer.manager_full_name,
        wer.business_name,
        wer.week_start_date,
        wer.week_end_date,
        
        -- Team member details as a struct
        STRUCT(
            CONCAT(wer.member_first_name, ' ', wer.member_last_name) as member_name,
            wer.member_email,
            wer.member_community_id,
            wer.classes_attended,
            wer.lessons_completed,
            wer.likes_received,
            wer.comments_received,
            wer.comments_made,
            wer.total_engagement_score
        ) AS team_member
        
    FROM {{ ref('weekly_team_engagement_report') }} wer
    CROSS JOIN latest_week lw
    WHERE wer.week_start_date = lw.latest_week_start
)

SELECT 
    manager_email,
    manager_full_name as manager_name,
    business_name,
    week_start_date,
    week_end_date,
    
    -- Aggregate team member data
    ARRAY_AGG(
        team_member 
        ORDER BY team_member.total_engagement_score DESC
    ) AS team_members,
    
    -- Summary metrics
    COUNT(*) AS team_size,
    SUM(team_member.classes_attended) AS total_classes_attended,
    SUM(team_member.lessons_completed) AS total_lessons_completed,
    SUM(team_member.likes_received) AS total_likes_received,
    SUM(team_member.comments_received) AS total_comments_received,
    SUM(team_member.comments_made) AS total_comments_made,
    SUM(team_member.total_engagement_score) AS team_total_engagement_score,
    ROUND(AVG(team_member.total_engagement_score), 2) AS avg_engagement_score_per_member
    
FROM manager_team_data
GROUP BY 
    manager_email,
    manager_full_name,
    business_name,
    week_start_date,
    week_end_date
