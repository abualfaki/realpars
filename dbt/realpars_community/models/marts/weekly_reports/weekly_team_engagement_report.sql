{{
    config(
        materialized='table',
        schema='cc_stg_weekly_reports'
    )
}}

/*
    Weekly Team Engagement Report for Managers
    
    Combines weekly engagement metrics with business relationships to show:
    - Each team member's weekly activity
    - Their manager's information
    - Business/company they belong to
    
    Purpose: Enable sending weekly reports to managers about their team's engagement
    
    Metrics included:
    - Classes attended (live events)
    - Lessons completed
    - Likes received
    - Comments received
    - Comments made
    
    Source: Weekly member engagement + Business relationships
*/

WITH team_member_engagement AS (
    -- Get weekly engagement for team members (not managers)
    SELECT
        we.community_member_id,
        we.email as member_email,
        we.first_name as member_first_name,
        we.last_name as member_last_name,
        we.week_start_date,
        we.week_end_date,
        we.classes_attended,
        we.lessons_completed,
        we.likes_received,
        we.comments_received,
        we.comments_made,
        we.has_activity,
        we.dbt_updated_at
    FROM {{ ref('int_weekly_member_engagement_incremental') }} we
    WHERE we.community_member_id IS NOT NULL
),

business_mapping AS (
    -- Get unique business-manager-member relationships
    -- Includes all businesses (both 'has_team' and 'manager_only')
    SELECT DISTINCT
        br.business_name,
        br.business_tag_id,
        br.business_total_members,
        br.business_size,
        br.business_confidence,
        br.manager_community_id,
        br.manager_email,
        br.manager_first_name,
        br.manager_last_name,
        br.manager_full_name,
        br.member_community_id,
        br.member_email,
        br.member_first_name,
        br.member_last_name,
        br.relationship_type
    FROM {{ ref('business_relationships') }} br
    -- No filter - include all businesses (with or without team members)
),

latest_week AS (
    -- Get the latest week from engagement data
    SELECT 
        MAX(week_start_date) as week_start_date,
        MAX(week_end_date) as week_end_date
    FROM {{ ref('int_weekly_member_engagement_incremental') }}
    WHERE week_start_date IS NOT NULL
),

final_output AS (

    SELECT
        -- Business & Manager Info
        bm.business_name,
        bm.business_tag_id,
        bm.business_total_members,
        bm.business_size,
        bm.business_confidence,
        bm.manager_community_id,
        bm.manager_email,
        bm.manager_first_name,
        bm.manager_last_name,
        bm.manager_full_name,
        
        -- Team Member Info (from business_mapping to ensure all members are included)
        bm.member_community_id,
        bm.member_email,
        bm.member_first_name,
        bm.member_last_name,
        
        -- Week Info (use latest week even if member has no engagement)
        COALESCE(te.week_start_date, lw.week_start_date) as week_start_date,
        COALESCE(te.week_end_date, lw.week_end_date) as week_end_date,
        
        -- Engagement Metrics (NULL safe with COALESCE)
        COALESCE(te.classes_attended, 0) as classes_attended,
        COALESCE(te.lessons_completed, 0) as lessons_completed,
        COALESCE(te.likes_received, 0) as likes_received,
        COALESCE(te.comments_received, 0) as comments_received,
        COALESCE(te.comments_made, 0) as comments_made,
        COALESCE(te.has_activity, false) as has_activity,
        
        -- Total engagement score (sum of all activities)
        (COALESCE(te.classes_attended, 0) + COALESCE(te.lessons_completed, 0) + COALESCE(te.likes_received, 0) + 
        COALESCE(te.comments_received, 0) + COALESCE(te.comments_made, 0)) as total_engagement_score,
        
        -- Metadata
        te.dbt_updated_at,
        CURRENT_TIMESTAMP() as report_generated_at

    FROM business_mapping bm
    CROSS JOIN latest_week lw
    LEFT JOIN team_member_engagement te
        ON te.community_member_id = bm.member_community_id
        AND te.week_start_date = lw.week_start_date

    ORDER BY 
        week_start_date DESC,
        bm.member_email
)

SELECT * FROM final_output