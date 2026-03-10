{{
    config(
        materialized='table',
        schema='cc_intermediate_transformations'
    )
}}

/*
    Weekly Member Engagement - Combined Metrics
    
    Combines all weekly engagement metrics for each community member:
    - Classes attended (live events)
    - Lessons completed
    - Likes received (posts + comments)
    - Comments received (on member's posts)
    - Comments made (by member)
    
    Creates a complete engagement picture per member per week with all metrics.
    Includes member profile details joined from community members table.
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

-- Get all unique weeks across all activity
all_weeks as (
    select distinct week_start_date from {{ ref('int_weekly_live_classes_attended') }}
    union distinct
    select distinct week_start_date from {{ ref('int_weekly_lessons_completed') }}
    union distinct
    select distinct week_start_date from {{ ref('int_weekly_likes_received') }}
    union distinct
    select distinct week_start_date from {{ ref('int_weekly_comments_received') }}
    union distinct
    select distinct week_start_date from {{ ref('int_weekly_comments_made') }}
),

-- Create spine of all members x all weeks
member_weeks as (
    select
        m.community_member_id,
        m.email,
        m.first_name,
        m.last_name,
        m.user_profile_created_at,
        w.week_start_date
    from community_members m
    cross join all_weeks w
    -- Only include weeks after member profile was created
    where w.week_start_date >= DATE_TRUNC(m.user_profile_created_at, WEEK(MONDAY))
),

-- Join all metric tables
combined_metrics as (
    select
        mw.community_member_id,
        mw.email,
        mw.first_name,
        mw.last_name,
        mw.week_start_date,
        COALESCE(classes.live_classes_attended, 0) as classes_attended,
        COALESCE(lessons.lessons_completed, 0) as lessons_completed,
        COALESCE(likes.likes_received, 0) as likes_received,
        COALESCE(comments_rcv.comments_received, 0) as comments_received,
        COALESCE(comments_made.comments_made, 0) as comments_made
    from member_weeks mw
    left join {{ ref('int_weekly_live_classes_attended') }} classes
        on mw.community_member_id = classes.community_member_id
        and mw.week_start_date = classes.week_start_date
    left join {{ ref('int_weekly_lessons_completed') }} lessons
        on mw.community_member_id = lessons.community_member_id
        and mw.week_start_date = lessons.week_start_date
    left join {{ ref('int_weekly_likes_received') }} likes
        on mw.community_member_id = likes.community_member_id
        and mw.week_start_date = likes.week_start_date
    left join {{ ref('int_weekly_comments_received') }} comments_rcv
        on mw.community_member_id = comments_rcv.community_member_id
        and mw.week_start_date = comments_rcv.week_start_date
    left join {{ ref('int_weekly_comments_made') }} comments_made
        on mw.community_member_id = comments_made.community_member_id
        and mw.week_start_date = comments_made.week_start_date
),

-- Add metadata
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
        end as has_activity
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
    has_activity
from final_output
order by 
    community_member_id,
    week_start_date desc
