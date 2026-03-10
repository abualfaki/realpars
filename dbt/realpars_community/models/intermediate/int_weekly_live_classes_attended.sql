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
    Weekly Classes Attended Per Community Member
    
    Calculates the number of distinct live classes/events attended by each community member per week.
    Source: Event attendees with confirmed RSVP status.
    Week Definition: Weeks start on Monday (DATE_TRUNC with WEEK(MONDAY))
*/

with event_attendees as (
    select
        community_member_id,
        event_id,
        rsvp_date,
        rsvp_status,
        DATE_TRUNC(rsvp_date, WEEK(MONDAY)) as week_start_date
    from {{ source('cc_stg_clean', 'clean_events_attendees_table') }}
    where rsvp_status = 'yes'  -- Only count confirmed attendees
        and community_member_id is not null
        and rsvp_date is not null
        {% if is_incremental() %}
        -- Only process events from weeks with new RSVPs since last run
        and DATE_TRUNC(rsvp_date, WEEK(MONDAY)) >= (
            select DATE_TRUNC(MAX(rsvp_date), WEEK(MONDAY)) 
            from {{ source('cc_stg_clean', 'clean_events_attendees_table') }}
            where rsvp_date <= (select MAX(week_start_date) from {{ this }})
        )
        {% endif %}
),

weekly_aggregation as (
    select
        community_member_id,
        week_start_date,
        COUNT(DISTINCT event_id) as classes_attended
    from event_attendees
    group by 
        community_member_id,
        week_start_date
)

select
    community_member_id,
    week_start_date,
    classes_attended as live_classes_attended
from weekly_aggregation
order by
    week_start_date desc
