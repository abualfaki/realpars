{{
    config(
        materialized='table',
        schema='cc_stg_weekly_reports'
    )
}}

/*
    Historical Weekly Team Engagement Report for Managers

    Combines weekly engagement metrics with business relationships across all
    completed reporting weeks so BI consumers can analyze trends over time.
*/

WITH team_member_engagement AS (
    SELECT
        we.community_member_id,
        we.email AS member_email,
        we.first_name AS member_first_name,
        we.last_name AS member_last_name,
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
      AND we.week_start_date IS NOT NULL
      AND we.week_start_date < DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
),

business_mapping AS (
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
),

completed_weeks AS (
    SELECT DISTINCT
        we.week_start_date,
        we.week_end_date
    FROM {{ ref('int_weekly_member_engagement_incremental') }} we
    WHERE we.week_start_date IS NOT NULL
      AND we.week_start_date < DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
),

final_output AS (
    SELECT
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
        bm.member_community_id,
        bm.member_email,
        bm.member_first_name,
        bm.member_last_name,
        TRIM(CONCAT(COALESCE(bm.member_first_name, ''), ' ', COALESCE(bm.member_last_name, ''))) AS member_full_name,
        cw.week_start_date,
        cw.week_end_date,
        CONCAT(CAST(cw.week_start_date AS STRING), ' to ', CAST(cw.week_end_date AS STRING)) AS report_week,
        COALESCE(te.classes_attended, 0) AS classes_attended,
        COALESCE(te.lessons_completed, 0) AS lessons_completed,
        COALESCE(te.likes_received, 0) AS likes_received,
        COALESCE(te.comments_received, 0) AS comments_received,
        COALESCE(te.comments_made, 0) AS comments_made,
        (
            COALESCE(te.likes_received, 0) +
            COALESCE(te.comments_received, 0) +
            COALESCE(te.comments_made, 0)
        ) AS community_interactions,
        COALESCE(te.has_activity, FALSE) AS has_activity,
        (
            COALESCE(te.classes_attended, 0) +
            COALESCE(te.lessons_completed, 0) +
            COALESCE(te.likes_received, 0) +
            COALESCE(te.comments_received, 0) +
            COALESCE(te.comments_made, 0)
        ) AS total_engagement_score,
        te.dbt_updated_at,
        CURRENT_TIMESTAMP() AS report_generated_at
    FROM business_mapping bm
    CROSS JOIN completed_weeks cw
    LEFT JOIN team_member_engagement te
        ON te.community_member_id = bm.member_community_id
        AND te.week_start_date = cw.week_start_date
)

SELECT *
FROM final_output