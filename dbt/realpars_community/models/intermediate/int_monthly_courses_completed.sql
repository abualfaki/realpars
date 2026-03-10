{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['community_member_id', 'month_start_date'],
        on_schema_change='sync_all_columns',
        schema='cc_intermediate_transformations'
    )
}}

/*
    Monthly Courses Completed by Members
    
    Aggregated view showing the number of courses completed by each member per month.
    One row per member per month with course completion counts.
    Source: Course completion events (deduplicated to earliest completion per member per course).
*/

with base_completions as (
    select
        safe_cast(initiator_community_id as int64) as community_member_id,
        course_id,
        course_name,
        created_at as completed_at,
        DATE_TRUNC(created_at, MONTH) as month_start_date
    from {{ source('cc_stg_clean', 'clean_courses_completed_table') }}
    where initiator_community_id is not null
        and created_at is not null
        and course_id is not null
        {% if is_incremental() %}
        -- Only process completions from recent months
        and DATE_TRUNC(created_at, MONTH) >= (select MAX(month_start_date) from {{ this }})
        {% endif %}
),

-- Deduplicate to get unique course per member per month
deduplicated_completions as (
    select
        community_member_id,
        course_id,
        course_name,
        month_start_date,
        MIN(completed_at) as completed_at
    from base_completions
    group by 
        community_member_id,
        course_id,
        course_name,
        month_start_date
)

select
    community_member_id,
    month_start_date,
    COUNT(DISTINCT course_id) as courses_completed_count,
    ARRAY_AGG(course_id ORDER BY course_id) as course_ids_completed,
    ARRAY_AGG(course_name ORDER BY course_id) as course_names_completed,
    MIN(completed_at) as first_completion_in_month,
    MAX(completed_at) as last_completion_in_month
from deduplicated_completions
group by 
    community_member_id,
    month_start_date
order by 
    community_member_id,
    month_start_date