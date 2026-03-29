{{
    config(
        materialized='table',
        schema='cc_stg_weekly_reports',
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
            TRIM(CONCAT(COALESCE(wer.member_first_name, ''), ' ', COALESCE(wer.member_last_name, ''))) as member_name,
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
)

SELECT 
    mtd.manager_email,
    mtd.manager_full_name as manager_name,
    mtd.business_name,
    mtd.week_start_date,
    mtd.week_end_date,
    
    -- Aggregate team member data
    ARRAY_AGG(
        mtd.team_member 
        ORDER BY mtd.team_member.total_engagement_score DESC
    ) AS team_members,
    
    -- Summary metrics
    COALESCE(mts.team_size, COUNT(*)) AS team_size,
    COALESCE(mts.business_total_members, COUNT(*)) AS business_total_members,
    SUM(mtd.team_member.classes_attended) AS total_classes_attended,
    SUM(mtd.team_member.lessons_completed) AS total_lessons_completed,
    SUM(mtd.team_member.likes_received) AS total_likes_received,
    SUM(mtd.team_member.comments_received) AS total_comments_received,
    SUM(mtd.team_member.comments_made) AS total_comments_made,
    SUM(mtd.team_member.total_engagement_score) AS team_total_engagement_score,
    ROUND(AVG(mtd.team_member.total_engagement_score), 2) AS avg_engagement_score_per_member
    
FROM manager_team_data mtd
LEFT JOIN manager_team_stats mts
    ON mts.manager_email = mtd.manager_email
    AND mts.business_name = mtd.business_name
GROUP BY 
    mtd.manager_email,
    mtd.manager_full_name,
    mtd.business_name,
    mtd.week_start_date,
    mtd.week_end_date,
    mts.team_size,
    mts.business_total_members
