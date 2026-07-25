{{
    config(
        materialized='view',
        schema='slack_reports'
    )
}}

/*
    Slack automation report for businesses with no activity in the latest completed week.

    This model is built at business grain so each business appears only once.
    Consecutive inactivity weeks are calculated from historical weekly member engagement.
*/

with manager_lists as (
    select
        business_name,
        array_to_string(
            array_agg(distinct manager_full_name ignore nulls order by manager_full_name limit 3),
            ', '
        ) as manager_names,
        array_to_string(
            array_agg(distinct lower(trim(manager_email)) ignore nulls order by lower(trim(manager_email)) limit 3),
            ', '
        ) as manager_emails
    from {{ ref('business_relationships') }}
    where business_name is not null
    group by business_name
),

business_team_members as (
    select distinct
        business_name,
        member_community_id
    from {{ ref('business_relationships') }}
    where business_name is not null
      and member_community_id is not null
),

completed_weeks as (
    select distinct
        week_start_date,
        week_end_date
    from {{ ref('int_weekly_member_engagement_incremental') }}
    where week_start_date < date_trunc(current_date(), week(monday))
),

business_week_points_history as (
    select
        btm.business_name,
        cw.week_start_date,
        cw.week_end_date,
        count(distinct btm.member_community_id) as team_size,
        count(distinct case when wme.has_activity then btm.member_community_id end) as active_members,
        count(distinct case when not coalesce(wme.has_activity, false) then btm.member_community_id end) as inactive_members,
        coalesce(sum(coalesce(wme.classes_attended, 0)), 0) as team_live_classes_attended,
        coalesce(sum(coalesce(wme.lessons_completed, 0)), 0) as team_lessons_completed,
        coalesce(
            sum(
                (coalesce(wme.classes_attended, 0) * 5) +
                coalesce(wme.lessons_completed, 0) +
                coalesce(wme.likes_received, 0) +
                coalesce(wme.comments_received, 0) +
                coalesce(wme.comments_made, 0)
            ),
            0
        ) as team_total_points
    from business_team_members as btm
    cross join completed_weeks as cw
    left join {{ ref('int_weekly_member_engagement_incremental') }} as wme
        on btm.member_community_id = wme.community_member_id
       and cw.week_start_date = wme.week_start_date
    group by
        btm.business_name,
        cw.week_start_date,
        cw.week_end_date
),

business_week_with_previous as (
    select
        business_name,
        week_start_date,
        week_end_date,
        team_size,
        active_members,
        inactive_members,
        team_live_classes_attended,
        team_lessons_completed,
        team_total_points as current_week_points,
        lag(team_live_classes_attended) over (
            partition by business_name
            order by week_start_date
        ) as previous_week_live_classes_attended,
        lag(team_lessons_completed) over (
            partition by business_name
            order by week_start_date
        ) as previous_week_lessons_completed,
        lag(team_total_points) over (
            partition by business_name
            order by week_start_date
        ) as previous_week_points,
        team_total_points - coalesce(
            lag(team_total_points) over (
                partition by business_name
                order by week_start_date
            ),
            0
        ) as team_points_change
    from business_week_points_history
),

latest_business_no_activity as (
    select
        *
    from business_week_with_previous
    where week_start_date = (select max(week_start_date) from completed_weeks)
      and current_week_points = 0
),

history_ranked as (
    select
        lbna.business_name,
        bwwp.week_start_date,
        bwwp.current_week_points,
        sum(case when bwwp.current_week_points > 0 then 1 else 0 end) over (
            partition by lbna.business_name
            order by bwwp.week_start_date desc
            rows between unbounded preceding and current row
        ) as active_weeks_seen_from_latest
    from latest_business_no_activity as lbna
    inner join business_week_with_previous as bwwp
        on lbna.business_name = bwwp.business_name
       and bwwp.week_start_date <= lbna.week_start_date
),

inactivity_streaks as (
    select
        business_name,
        count(*) as weeks_of_inactivity
    from history_ranked
    where current_week_points = 0
      and active_weeks_seen_from_latest = 0
    group by
        business_name
)

select
    lbna.business_name,
    ml.manager_names,
    ml.manager_emails,
    lbna.week_start_date,
    lbna.week_end_date,
    lbna.team_size,
    lbna.active_members,
    lbna.inactive_members,
    lbna.current_week_points,
    coalesce(lbna.previous_week_points, 0) as previous_week_points,
    coalesce(lbna.previous_week_live_classes_attended, 0) as previous_week_live_classes_attended,
    coalesce(lbna.previous_week_lessons_completed, 0) as previous_week_lessons_completed,
    lbna.team_points_change,
    coalesce(streak.weeks_of_inactivity, 1) as weeks_of_inactivity
from latest_business_no_activity as lbna
left join manager_lists as ml
    on lbna.business_name = ml.business_name
left join inactivity_streaks as streak
    on lbna.business_name = streak.business_name
order by weeks_of_inactivity desc