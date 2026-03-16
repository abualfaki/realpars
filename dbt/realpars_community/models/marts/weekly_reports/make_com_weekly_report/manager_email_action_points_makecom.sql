{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: Action Points (Flattened)

    Returns one row per manager/week with three action points:
    1) Celebrate top performer (rank 1 this week)
    2) Encourage top mover (strong jump vs steady progress)
    3) Re-engage a member with zero points this week and last week

    This model reuses fields already computed in manager_email_report_with_rankings
    (performance_rank, mover_rank, points_change, previous_week_points) to avoid
    recomputing full business logic.
*/

WITH manager_weeks AS (
    SELECT DISTINCT
        manager_email,
        manager_full_name,
        business_name,
        week_start_date,
        week_end_date,
        week_start_formatted,
        week_end_formatted,
        report_generated_at,
        EXTRACT(ISOWEEK FROM week_start_date) AS week_number,
        EXTRACT(YEAR FROM week_start_date) AS year
    FROM {{ ref('manager_email_report_with_rankings') }}
),

celebrate_candidate AS (
    SELECT
        manager_email,
        week_start_date,
        member_first_name,
        member_full_name,
        total_points,
        ROW_NUMBER() OVER (
            PARTITION BY manager_email, week_start_date
            ORDER BY performance_rank, total_points DESC, member_full_name
        ) AS rn
    FROM {{ ref('manager_email_report_with_rankings') }}
    WHERE performance_rank = 1
      AND total_points > 0
),

encourage_candidate AS (
    SELECT
        manager_email,
        week_start_date,
        member_first_name,
        member_full_name,
        previous_week_points,
        points_change,
        ROW_NUMBER() OVER (
            PARTITION BY manager_email, week_start_date
            ORDER BY mover_rank, points_change DESC, member_full_name
        ) AS rn
    FROM {{ ref('manager_email_report_with_rankings') }}
    WHERE points_change > 0
),

reengage_candidate AS (
    SELECT
        manager_email,
        week_start_date,
        member_first_name,
        member_full_name,
        ROW_NUMBER() OVER (
            PARTITION BY manager_email, week_start_date
            ORDER BY member_full_name
        ) AS rn
    FROM {{ ref('manager_email_report_with_rankings') }}
    WHERE total_points = 0
      AND COALESCE(previous_week_points, 0) = 0
)

SELECT 
    mw.manager_email,
    mw.manager_full_name,
    mw.business_name,
    mw.week_start_date,
    mw.week_end_date,
    mw.week_start_formatted,
    mw.week_end_formatted,
    mw.week_number,
    mw.year,

    c.member_full_name AS celebrate_member_name,
    c.member_first_name AS celebrate_member_first_name,

    e.member_full_name AS encourage_member_name,
    e.member_first_name AS encourage_member_first_name,
    e.previous_week_points AS encourage_prev_points,
    e.points_change AS encourage_points_change,
    CASE
        WHEN e.member_full_name IS NULL THEN NULL
        WHEN COALESCE(e.previous_week_points, 0) = 0 THEN 'strong jump this week'
        WHEN SAFE_DIVIDE(e.points_change, e.previous_week_points) >= 0.5 THEN 'strong jump this week'
        ELSE 'steady progress'
    END AS encourage_label,

    r.member_full_name AS reengage_member_name,
    r.member_first_name AS reengage_member_first_name,

    CONCAT(
        IF(
            c.member_first_name IS NOT NULL,
            CONCAT('<li>🏆 Celebrate ', c.member_first_name, ' for leading this week.</li>'),
            ''
        ),
        '<li>📈 ',
        IFNULL(
            CONCAT(
                'Encourage ', e.member_first_name, ' to keep momentum (+', CAST(e.points_change AS STRING), ' pts, ',
                CASE
                    WHEN COALESCE(e.previous_week_points, 0) = 0 THEN 'strong jump this week'
                    WHEN SAFE_DIVIDE(e.points_change, e.previous_week_points) >= 0.5 THEN 'strong jump this week'
                    ELSE 'steady progress'
                END,
                ').'
            ),
            'Encourage your team to build momentum next week.'
        ),
        '</li>',
        '<li>💬 ',
        IFNULL(
            CONCAT('Re-engage ', r.member_first_name, ' with a short lesson or live class invite.'),
            'Great job: no fully inactive members to re-engage this week.'
        ),
        '</li>'
    ) AS action_points_html,

    mw.report_generated_at

FROM manager_weeks mw
LEFT JOIN celebrate_candidate c
    ON mw.manager_email = c.manager_email
   AND mw.week_start_date = c.week_start_date
   AND c.rn = 1
LEFT JOIN encourage_candidate e
    ON mw.manager_email = e.manager_email
   AND mw.week_start_date = e.week_start_date
   AND e.rn = 1
LEFT JOIN reengage_candidate r
    ON mw.manager_email = r.manager_email
   AND mw.week_start_date = r.week_start_date
   AND r.rn = 1

ORDER BY mw.manager_email, mw.week_start_date DESC
