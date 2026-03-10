{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['community_member_id', 'week_start_date'],
        on_schema_change='sync_all_columns',
        schema='cc_intermediate_transformations',
        partition_by={
            "field": "week_start_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['community_member_id', 'week_start_date']
    )
}}

/*
    Weekly Member Engagement - Combined Metrics (Incremental)
    
    Combines all weekly engagement metrics for each community member:
    - Classes attended (live events)
    - Lessons completed
    - Likes received (posts + comments)
    - Comments received (on member's posts)
    - Comments made (by member)
    
    Incremental Strategy:
    - On full refresh: Builds complete history for all members from their join date
    - On incremental runs:
      1. Adds new weeks for all existing members
      2. Backfills complete history for newly joined members
      3. Reprocesses last 4 weeks to catch late-arriving data
    
    Week Definition: Weeks start on Monday (DATE_TRUNC with WEEK(MONDAY))
*/

with community_members as (
    select
        community_member_id,
        email,
        first_name,
        last_name,
        user_profile_created_at
    from {{ source('cc_stg_clean', 'clean_communtity_members_table') }}
    where community_member_id is not null
),

{% if is_incremental() %}

-- Incremental Mode: Process only recent weeks + new members

-- Get the latest week already processed
max_processed_week as (
    select 
        DATE(MAX(week_start_date)) as max_week
    from {{ this }}
),

-- Identify existing members in the table
existing_members as (
    select distinct 
        community_member_id
    from {{ this }}
),

-- Identify new members who need full historical backfill
-- Includes: 1) Members not in existing table, 2) Members who joined recently (within lookback window)
new_members as (
    select 
        cm.community_member_id,
        cm.email,
        cm.first_name,
        cm.last_name,
        cm.user_profile_created_at
    from community_members cm
    left join existing_members em
        on cm.community_member_id = em.community_member_id
    cross join max_processed_week mpw
    where em.community_member_id is null  -- Not in existing table
       or DATE(DATE_TRUNC(cm.user_profile_created_at, WEEK(MONDAY))) >= DATE_SUB(mpw.max_week, INTERVAL 4 WEEK)  -- Joined recently
),

-- Generate date spine for NEW weeks (for incremental updates to existing members)
-- Includes 4-week lookback to catch late-arriving data
new_weeks_spine as (
    select 
        week_start_date
    from UNNEST(
        GENERATE_DATE_ARRAY(
            (select DATE_SUB(max_week, INTERVAL 4 WEEK) from max_processed_week),
            DATE(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))),
            INTERVAL 1 WEEK
        )
    ) as week_start_date
),

-- Generate complete historical spine for new members
new_member_weeks_spine as (
    select distinct
        DATE(DATE_TRUNC(week_date, WEEK(MONDAY))) as week_start_date
    from UNNEST(
        GENERATE_DATE_ARRAY(
            (select MIN(DATE(DATE_TRUNC(user_profile_created_at, WEEK(MONDAY)))) from new_members),
            DATE(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))),
            INTERVAL 1 WEEK
        )
    ) as week_date
),

-- Path A: New members get full historical weeks
new_member_weeks as (
    select
        nm.community_member_id,
        nm.email,
        nm.first_name,
        nm.last_name,
        nm.user_profile_created_at,
        nmws.week_start_date,
        true as is_backfill
    from new_members nm
    cross join new_member_weeks_spine nmws
    where nmws.week_start_date >= DATE(DATE_TRUNC(nm.user_profile_created_at, WEEK(MONDAY)))
),

-- Path B: Existing members get only recent weeks
existing_member_weeks as (
    select
        cm.community_member_id,
        cm.email,
        cm.first_name,
        cm.last_name,
        cm.user_profile_created_at,
        nws.week_start_date,
        false as is_backfill
    from community_members cm
    inner join existing_members em
        on cm.community_member_id = em.community_member_id
    cross join new_weeks_spine nws
    left join new_members nm  -- Exclude members already in new_members
        on cm.community_member_id = nm.community_member_id
    where nm.community_member_id is null
      and nws.week_start_date >= DATE(DATE_TRUNC(cm.user_profile_created_at, WEEK(MONDAY)))
),

-- Combine both paths
member_weeks as (
    select * from new_member_weeks
    union all
    select * from existing_member_weeks
)

{% else %}

-- Full Refresh Mode: Build complete historical spine for all members

-- Get all unique weeks from activity data
all_weeks as (
    select distinct DATE(week_start_date) as week_start_date from {{ ref('int_weekly_live_classes_attended') }}
    union distinct
    select distinct DATE(week_start_date) as week_start_date from {{ ref('int_weekly_lessons_completed') }}
    union distinct
    select distinct DATE(week_start_date) as week_start_date from {{ ref('int_weekly_likes_received') }}
    union distinct
    select distinct DATE(week_start_date) as week_start_date from {{ ref('int_weekly_comments_received') }}
    union distinct
    select distinct DATE(week_start_date) as week_start_date from {{ ref('int_weekly_comments_made') }}
),

-- Cross join all members with all activity weeks
member_weeks as (
    select
        m.community_member_id,
        m.email,
        m.first_name,
        m.last_name,
        m.user_profile_created_at,
        w.week_start_date,
        false as is_backfill
    from community_members m
    cross join all_weeks w
    where w.week_start_date >= DATE(DATE_TRUNC(m.user_profile_created_at, WEEK(MONDAY)))
)

{% endif %}

,

-- Join all metric tables
combined_metrics as (
    select
        mw.community_member_id,
        mw.email,
        mw.first_name,
        mw.last_name,
        mw.week_start_date,
        mw.is_backfill,
        COALESCE(classes.live_classes_attended, 0) as classes_attended,
        COALESCE(lessons.lessons_completed, 0) as lessons_completed,
        COALESCE(likes.likes_received, 0) as likes_received,
        COALESCE(comments_rcv.comments_received, 0) as comments_received,
        COALESCE(comments_made.comments_made, 0) as comments_made
    from member_weeks mw
    left join {{ ref('int_weekly_live_classes_attended') }} classes
        on mw.community_member_id = classes.community_member_id
        and mw.week_start_date = DATE(classes.week_start_date)
    left join {{ ref('int_weekly_lessons_completed') }} lessons
        on mw.community_member_id = lessons.community_member_id
        and mw.week_start_date = DATE(lessons.week_start_date)
    left join {{ ref('int_weekly_likes_received') }} likes
        on mw.community_member_id = likes.community_member_id
        and mw.week_start_date = DATE(likes.week_start_date)
    left join {{ ref('int_weekly_comments_received') }} comments_rcv
        on mw.community_member_id = comments_rcv.community_member_id
        and mw.week_start_date = DATE(comments_rcv.week_start_date)
    left join {{ ref('int_weekly_comments_made') }} comments_made
        on mw.community_member_id = comments_made.community_member_id
        and mw.week_start_date = DATE(comments_made.week_start_date)
),

-- Add metadata and calculated fields
final_output as (
    select
        community_member_id,
        email,
        first_name,
        last_name,
        week_start_date,
        DATE_ADD(week_start_date, INTERVAL 6 DAY) as week_end_date,
        classes_attended,
        lessons_completed,
        likes_received,
        comments_received,
        comments_made,
        -- Flag for any activity this week
        case 
            when (classes_attended + lessons_completed + likes_received + 
                  comments_received + comments_made) > 0 
            then true 
            else false 
        end as has_activity,
        is_backfill,
        CURRENT_TIMESTAMP() as dbt_updated_at
    from combined_metrics
)

select
    community_member_id,
    email,
    first_name,
    last_name,
    week_start_date,
    week_end_date,
    classes_attended,
    lessons_completed,
    likes_received,
    comments_received,
    comments_made,
    has_activity,
    is_backfill,
    dbt_updated_at
from final_output
where week_start_date != "2026-03-09"
order by week_start_date desc