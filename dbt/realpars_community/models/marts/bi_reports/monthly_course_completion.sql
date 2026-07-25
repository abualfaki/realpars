{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Monthly course completions by team member.

    One row per team member and month with a plain-text list of courses completed.
*/

with report_months as (
    select distinct
        date_trunc(date(created_at), month) as report_month_start_date
    from {{ source('cc_stg_clean', 'clean_courses_completed_table') }}
    where created_at is not null
),

-- Get manager-member relationships (only for course completion tracking)
manager_business as (
    select distinct
        manager_email,
        manager_full_name,
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
        ) as member_full_name
    from {{ ref('business_relationships') }}
    where manager_email is not null
      and member_community_id is not null
),

base_completions as (
    select
        safe_cast(initiator_community_id as int64) as member_community_id,
        initcap(trim(course_name)) as course_name,
        date_trunc(date(created_at), month) as report_month_start_date
    from {{ source('cc_stg_clean', 'clean_courses_completed_table') }}
    where initiator_community_id is not null
      and created_at is not null
      and course_name is not null
),

-- Keep only one completion per member/course/month
deduplicated_member_course as (
    select
        member_community_id,
        course_name,
        report_month_start_date
    from base_completions
    group by
        member_community_id,
        course_name,
        report_month_start_date
),

team_members_by_month as (
    select distinct
        mb.manager_email,
        mb.manager_full_name,
        mb.business_name,
        mb.member_community_id,
        mb.member_full_name,
        rm.report_month_start_date
    from manager_business as mb
    cross join report_months as rm
),

member_courses_by_month as (
    select
        mb.manager_email,
        mb.manager_full_name,
        mb.business_name,
        mb.member_community_id,
        mb.member_full_name,
        d.course_name,
        d.report_month_start_date
    from deduplicated_member_course as d
    inner join manager_business as mb
        on d.member_community_id = mb.member_community_id
),

member_monthly_courses as (
    select
        manager_email,
        manager_full_name,
        business_name,
        member_community_id,
        member_full_name,
        report_month_start_date,
        string_agg(
            course_name,
            ', '
            order by course_name
        ) as completed_courses,
        count(*) as completed_courses_count
    from member_courses_by_month
    group by
        manager_email,
        manager_full_name,
        business_name,
        member_community_id,
        member_full_name,
        report_month_start_date
)

select
    tmm.manager_email,
    tmm.manager_full_name,
    tmm.business_name,
    tmm.member_community_id,
    tmm.member_full_name,
    tmm.report_month_start_date as completion_month,
    format_date('%B %Y', tmm.report_month_start_date) as completion_month_formatted,
    coalesce(mmc.completed_courses_count, 0) as completed_courses_count,
    case
        when coalesce(mmc.completed_courses_count, 0) = 0 then 'No course completions'
        else mmc.completed_courses
    end as completed_courses,
    current_timestamp() as report_generated_at
from team_members_by_month as tmm
left join member_monthly_courses as mmc
    on tmm.manager_email = mmc.manager_email
   and tmm.business_name = mmc.business_name
   and tmm.member_community_id = mmc.member_community_id
   and tmm.report_month_start_date = mmc.report_month_start_date
order by
    tmm.business_name,
    tmm.manager_email,
    tmm.member_full_name,
    tmm.report_month_start_date