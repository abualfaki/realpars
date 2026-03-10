{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['community_member_id', 'week_start_date'],
        on_schema_change='sync_all_columns',
        schema='cc_intermediate_transformations'
    )
}}

/*
    Weekly Lessons Completed Per Community Member
    
    Calculates the number of lesson completions by each community member per week.
    Counts all completion events (not distinct lessons) - if someone completes the same lesson 
    multiple times in a week, each completion is counted for engagement tracking.
    Source: Course lesson completion events (includes all completion attempts).
    Week Definition: Weeks start on Monday (DATE_TRUNC with WEEK(MONDAY))
*/

with lesson_completions as (
    select
        initiator_community_id as community_member_id,
        lesson_id,
        created_at as completed_at,
        DATE_TRUNC(created_at, WEEK(MONDAY)) as week_start_date
    from {{ source('cc_stg_clean', 'clean_course_lesson_completed_table') }}
    where initiator_community_id is not null
        and created_at is not null
        and lesson_id is not null
        {% if is_incremental() %}
        -- Only process lessons from weeks with new completions since last run
        and DATE_TRUNC(created_at, WEEK(MONDAY)) >= (
            select DATE_TRUNC(MAX(created_at), WEEK(MONDAY)) 
            from {{ source('cc_stg_clean', 'clean_course_lesson_completed_table') }}
            where created_at <= (select MAX(week_start_date) from {{ this }})
        )
        {% endif %}
),

weekly_aggregation as (
    select
        community_member_id,
        week_start_date,
        COUNT(DISTINCT lesson_id) as lessons_completed
    from lesson_completions
    group by 
        community_member_id,
        week_start_date
)

select
    community_member_id,
    week_start_date,
    lessons_completed
from weekly_aggregation
order by 
    community_member_id,
    week_start_date desc,
    lessons_completed desc
