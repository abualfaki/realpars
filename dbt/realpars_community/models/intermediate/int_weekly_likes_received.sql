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
    Weekly Likes Received Per Community Member
    
    Calculates the total number of likes received by each community member per week.
    Includes both post likes and comment likes received.
    - Post likes: Counted when member is post_owner_community_member_id
    - Comment likes: Counted when member is comment_community_member_id
    Week Definition: Weeks start on Monday (DATE_TRUNC with WEEK(MONDAY))
*/

with post_likes_received as (
    select
        post_owner_community_member_id as community_member_id,
        created_at,
        DATE_TRUNC(DATE(created_at), WEEK(MONDAY)) as week_start_date,
        record_id as like_id
    from {{ source('cc_stg_clean', 'clean_post_liked_table') }}
    where post_owner_community_member_id is not null
        and created_at is not null
        {% if is_incremental() %}
        -- Reprocess recent weeks so late-arriving likes update existing aggregates.
        and DATE_TRUNC(DATE(created_at), WEEK(MONDAY)) >= coalesce(
            (select DATE_SUB(DATE(MAX(week_start_date)), INTERVAL 4 WEEK) from {{ this }}),
            DATE '1970-01-05'
        )
        {% endif %}
),

comment_likes_received as (
    select
        comment_community_member_id as community_member_id,
        created_at,
        DATE_TRUNC(DATE(created_at), WEEK(MONDAY)) as week_start_date,
        record_id as like_id
    from {{ source('cc_stg_clean', 'clean_post_comment_liked_table') }}
    where comment_community_member_id is not null
        and created_at is not null
        {% if is_incremental() %}
        -- Reprocess recent weeks so late-arriving likes update existing aggregates.
        and DATE_TRUNC(DATE(created_at), WEEK(MONDAY)) >= coalesce(
            (select DATE_SUB(DATE(MAX(week_start_date)), INTERVAL 4 WEEK) from {{ this }}),
            DATE '1970-01-05'
        )
        {% endif %}
),

all_likes_received as (
    select * from post_likes_received
    union all
    select * from comment_likes_received
),

weekly_aggregation as (
    select
        community_member_id,
        week_start_date,
        COUNT(DISTINCT like_id) as likes_received
    from all_likes_received
    group by 
        community_member_id,
        week_start_date
)

select
    community_member_id,
    week_start_date,
    likes_received
from weekly_aggregation
order by
    week_start_date desc
