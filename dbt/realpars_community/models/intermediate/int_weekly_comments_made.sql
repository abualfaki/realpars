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
    Weekly Comments Made Per Community Member
    
    Calculates the number of comments made by each community member per week.
    A comment is "made" by the comment author (comment_community_member_id).
    Source: Post comment creation events.
    Week Definition: Weeks start on Monday (DATE_TRUNC with WEEK(MONDAY))
*/

with comments_made as (
    select
        comment_community_member_id as community_member_id,
        comment_id,
        created_at as commented_at,
        DATE_TRUNC(created_at, WEEK(MONDAY)) as week_start_date
    from {{ source('cc_stg_clean', 'clean_post_comments_table') }}
    where comment_community_member_id is not null
        and created_at is not null
        and comment_id is not null
        {% if is_incremental() %}
        -- Only process comments from weeks with new comments since last run
        and DATE_TRUNC(created_at, WEEK(MONDAY)) >= (
            select DATE_TRUNC(MAX(created_at), WEEK(MONDAY)) 
            from {{ source('cc_stg_clean', 'clean_post_comments_table') }}
            where created_at <= (select MAX(week_start_date) from {{ this }})
        )
        {% endif %}
),

weekly_aggregation as (
    select
        community_member_id,
        week_start_date,
        COUNT(DISTINCT comment_id) as comments_made
    from comments_made
    group by 
        community_member_id,
        week_start_date
)

select
    community_member_id,
    week_start_date,
    comments_made
from weekly_aggregation
order by
    week_start_date DESC
