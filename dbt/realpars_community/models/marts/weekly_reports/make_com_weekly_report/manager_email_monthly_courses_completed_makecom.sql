{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: Monthly Courses Completed Table (HTML-ready)

    One row per manager/business for the latest month in source data.
    Includes:
    - courses_completed_rows_html: only <tr> rows
    - courses_completed_table_html: complete HTML table with inline styles for Gmail
*/

with latest_month as (
    select
        max(date_trunc(date(created_at), month)) as report_month_start_date
    from {{ source('cc_stg_clean', 'clean_courses_completed_table') }}
    where created_at is not null
),

-- Get all managers (including those without team members)
all_managers as (
    select distinct
        manager_email,
        manager_full_name,
        business_name
    from {{ ref('business_relationships') }}
    where manager_email is not null
),

-- Get manager-member relationships (only for course completion tracking)
manager_business as (
    select distinct
        manager_email,
        manager_full_name,
        business_name,
        member_community_id
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

-- Get ALL managers (even those with no course completions)
all_managers_for_report as (
    select distinct
        am.manager_email,
        am.manager_full_name,
        am.business_name,
        lm.report_month_start_date
    from all_managers am
    cross join latest_month lm
),

member_course_with_business as (
    select
        mb.manager_email,
        mb.manager_full_name,
        mb.business_name,
        d.course_name,
        d.report_month_start_date,
        concat(
            coalesce(nullif(trim(br.member_first_name), ''), ''),
            case
                when coalesce(nullif(trim(br.member_first_name), ''), '') != ''
                 and coalesce(nullif(trim(br.member_last_name), ''), '') != '' then ' '
                else ''
            end,
            coalesce(nullif(trim(br.member_last_name), ''), '')
        ) as member_full_name
    from deduplicated_member_course d
    inner join manager_business mb
        on d.member_community_id = mb.member_community_id
    inner join {{ ref('business_relationships') }} br
        on br.member_community_id = d.member_community_id
       and br.manager_email = mb.manager_email
       and br.business_name = mb.business_name
),

course_agg as (
    select
        manager_email,
        manager_full_name,
        business_name,
        report_month_start_date,
        course_name,
        count(*) as completed_count,
        string_agg(
            replace(replace(replace(member_full_name, '&', '&amp;'), '<', '&lt;'), '>', '&gt;'),
            ', '
            order by member_full_name
        ) as completed_by_names
    from member_course_with_business
    where report_month_start_date = (select report_month_start_date from latest_month)
    group by
        manager_email,
        manager_full_name,
        business_name,
        report_month_start_date,
        course_name
),

course_rows as (
    select
        manager_email,
        manager_full_name,
        business_name,
        report_month_start_date,
        concat(
            '<tr>',
            '<td style="padding:10px 12px;border:1px solid #ddd;color:#000000;background-color:#ffffff;">',
            replace(replace(replace(course_name, '&', '&amp;'), '<', '&lt;'), '>', '&gt;'),
            '</td>',
            '<td style="padding:10px 12px;border:1px solid #ddd;color:#000000;background-color:#ffffff;text-align:center;">',
            cast(completed_count as string),
            '</td>',
            '<td style="padding:10px 12px;border:1px solid #ddd;color:#000000;background-color:#ffffff;">',
            coalesce(completed_by_names, 'N/A'),
            '</td>',
            '</tr>'
        ) as row_html,
        completed_count
    from course_agg
),

rows_by_manager as (
    select
        manager_email,
        manager_full_name,
        business_name,
        report_month_start_date,
        string_agg(row_html, '' order by completed_count desc, row_html) as courses_completed_rows_html,
        count(*) as total_distinct_courses,
        sum(completed_count) as total_course_completions
    from course_rows
    group by
        manager_email,
        manager_full_name,
        business_name,
        report_month_start_date
),

-- Get team members and their completion status
team_members_status as (
    select
        mb.manager_email,
        mb.business_name,
        lm.report_month_start_date,
        mb.member_community_id,
        concat(
            coalesce(nullif(trim(br.member_first_name), ''), ''),
            case
                when coalesce(nullif(trim(br.member_first_name), ''), '') != ''
                 and coalesce(nullif(trim(br.member_last_name), ''), '') != '' then ' '
                else ''
            end,
            coalesce(nullif(trim(br.member_last_name), ''), '')
        ) as member_full_name,
        count(distinct mcc.course_name) as member_completion_count
    from manager_business mb
    cross join latest_month lm
    inner join {{ ref('business_relationships') }} br
        on br.member_community_id = mb.member_community_id
       and br.manager_email = mb.manager_email
       and br.business_name = mb.business_name
    left join deduplicated_member_course mcc
        on mcc.member_community_id = mb.member_community_id
       and mcc.report_month_start_date = lm.report_month_start_date
    group by
        mb.manager_email,
        mb.business_name,
        lm.report_month_start_date,
        mb.member_community_id,
        member_full_name
),

-- Aggregate team summary stats
team_summary_stats as (
    select
        manager_email,
        business_name,
        report_month_start_date,
        count(distinct member_community_id) as team_size,
        countif(member_completion_count > 0) as unique_learners_count,
        string_agg(
            case when member_completion_count = 0 then member_full_name else null end,
            ', '
            order by member_full_name
        ) as zero_completions_members_list
    from team_members_status
    group by
        manager_email,
        business_name,
        report_month_start_date
)

select
    am.manager_email,
    am.manager_full_name,
    am.business_name,
    am.report_month_start_date,
    format_date('%B %Y', am.report_month_start_date) as report_month_formatted,
    coalesce(rb.total_distinct_courses, 0) as total_distinct_courses,
    coalesce(rb.total_course_completions, 0) as total_course_completions,
    coalesce(tss.team_size, 0) as team_size,
    coalesce(tss.unique_learners_count, 0) as unique_learners_count,
    coalesce(tss.zero_completions_members_list, 'None 🎉') as zero_completions_members_list,
    coalesce(
        rb.courses_completed_rows_html,
        '<tr><td style="padding:10px 12px;border:1px solid #2f3542;color:#94a3b8;" colspan="3">No course completions this month.</td></tr>'
    ) as courses_completed_rows_html,
    concat(
        '<table style="width:100%;border-collapse:collapse;background-color:#ffffff;font-family:Arial,sans-serif;font-size:14px;">',
        '<thead><tr>',
        '<th style="padding:10px 12px;border:1px solid #000000;color:#ffffff;text-align:left;background:#000000;">Course</th>',
        '<th style="padding:10px 12px;border:1px solid #000000;color:#ffffff;text-align:center;background:#000000;">#</th>',
        '<th style="padding:10px 12px;border:1px solid #000000;color:#ffffff;text-align:left;background:#000000;">Completed by</th>',
        '</tr></thead>',
        '<tbody>',
        coalesce(
            rb.courses_completed_rows_html,
            '<tr><td style="padding:10px 12px;border:1px solid #ddd;color:#000000;background-color:#ffffff;" colspan="3">No course completions this month.</td></tr>'
        ),
        '</tbody>',
        '</table>'
    ) as courses_completed_table_html,
    current_timestamp() as report_generated_at
from all_managers_for_report am
left join rows_by_manager rb
    on rb.manager_email = am.manager_email
   and rb.business_name = am.business_name
   and rb.report_month_start_date = am.report_month_start_date
left join team_summary_stats tss
    on tss.manager_email = am.manager_email
   and tss.business_name = am.business_name
   and tss.report_month_start_date = am.report_month_start_date
order by am.business_name, am.manager_email