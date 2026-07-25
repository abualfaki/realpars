{{
    config(
        materialized='view',
        schema='bi_reports'
    )
}}

/*
    Team member course completion dates.

    One row per team member, course, and completion date.
    Team members with no recorded course completion are still included with NULL completion dates.
*/

with manager_business as (
    select
        business_name,
        member_community_id,
        concat(
            coalesce(nullif(trim(member_first_name), ''), ''),
            case
                when coalesce(nullif(trim(member_first_name), ''), '') != ''
                 and coalesce(nullif(trim(member_last_name), ''), '') != '' then ' '
                else ''
            end,
            coalesce(nullif(trim(member_last_name), ''), '')
        ) as member_full_name,
        string_agg(distinct lower(trim(manager_email)), ', ' order by lower(trim(manager_email))) as manager_emails
    from {{ ref('business_relationships') }}
    where member_community_id is not null
      and manager_email is not null
    group by
        business_name,
        member_community_id,
        member_full_name
),

manager_ids as (
    select distinct
        manager_community_id
    from {{ ref('business_relationships') }}
    where manager_community_id is not null
),

non_manager_team_members as (
    select
        mb.business_name,
        mb.member_community_id,
        mb.member_full_name,
        mb.manager_emails
    from manager_business as mb
    left join manager_ids as mi
        on mb.member_community_id = mi.manager_community_id
    where mi.manager_community_id is null
),

member_course_completion_dates as (
    select distinct
        safe_cast(initiator_community_id as int64) as member_community_id,
        initcap(trim(course_name)) as course_name,
        date(created_at) as course_completed_date,
        date_trunc(date(created_at), month) as completion_month
    from {{ source('cc_stg_clean', 'clean_courses_completed_table') }}
    where initiator_community_id is not null
      and created_at is not null
      and course_name is not null
),

team_members_with_completion_dates as (
    select
        nmtm.business_name,
        nmtm.member_community_id,
        nmtm.member_full_name,
        nmtm.manager_emails,
        mccd.course_name,
        mccd.course_completed_date,
        mccd.completion_month
    from non_manager_team_members as nmtm
    left join member_course_completion_dates as mccd
        on nmtm.member_community_id = mccd.member_community_id
)

select
    business_name,
    member_community_id,
    member_full_name,
    manager_emails,
    coalesce(course_name, 'No Completions') as course_name,
    coalesce(format_date('%Y-%m-%d', course_completed_date), 'No Completions') as course_completed_date,
    coalesce(format_date('%Y-%m-%d', completion_month), 'No Completions') as completion_month,
    coalesce(format_date('%B %Y', completion_month), 'No Completions') as completion_month_formatted
from team_members_with_completion_dates
order by
    business_name,
    member_full_name,
    course_name,
    course_completed_date