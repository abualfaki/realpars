{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: Top Performers Summary (Flattened)

    Returns ONE row per manager/week with top performers as flat columns
    and a ready-to-render top_performers_html field.

    Edge cases are handled in SQL:
    - No top performers => "No top performers this week."
    - 1 or 2 top performers => only those lines are rendered
    - 3 top performers => all 3 lines are rendered
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
        report_generated_at
    FROM {{ ref('manager_email_report_with_rankings') }}
),

performers AS (
    SELECT
        manager_email,
        week_start_date,
        member_full_name,
        total_points,
        performance_rank
    FROM {{ ref('manager_email_report_with_rankings') }}
    WHERE is_top_performer = TRUE
      AND performance_rank <= 3
),

aggregated AS (
    SELECT
        mw.manager_email,
        mw.manager_full_name,
        mw.business_name,
        mw.week_start_date,
        mw.week_end_date,
        mw.week_start_formatted,
        mw.week_end_formatted,

        COUNTIF(p.performance_rank IS NOT NULL)                                 AS count_top_performers,

        MAX(CASE WHEN p.performance_rank = 1 THEN p.member_full_name END)      AS top_performer_1_name,
        MAX(CASE WHEN p.performance_rank = 1 THEN p.total_points END)           AS top_performer_1_score,

        MAX(CASE WHEN p.performance_rank = 2 THEN p.member_full_name END)      AS top_performer_2_name,
        MAX(CASE WHEN p.performance_rank = 2 THEN p.total_points END)           AS top_performer_2_score,

        MAX(CASE WHEN p.performance_rank = 3 THEN p.member_full_name END)      AS top_performer_3_name,
        MAX(CASE WHEN p.performance_rank = 3 THEN p.total_points END)           AS top_performer_3_score,

        MAX(mw.report_generated_at)                                             AS report_generated_at
    FROM manager_weeks mw
    LEFT JOIN performers p
        ON mw.manager_email = p.manager_email
       AND mw.week_start_date = p.week_start_date
    GROUP BY
        mw.manager_email,
        mw.manager_full_name,
        mw.business_name,
        mw.week_start_date,
        mw.week_end_date,
        mw.week_start_formatted,
        mw.week_end_formatted
)

SELECT
    manager_email,
    manager_full_name,
    business_name,
    week_start_date,
    week_end_date,
    week_start_formatted,
    week_end_formatted,
    count_top_performers,
    top_performer_1_name,
    top_performer_1_score,
    top_performer_2_name,
    top_performer_2_score,
    top_performer_3_name,
    top_performer_3_score,
    CASE
        WHEN count_top_performers = 0 THEN '<li>No top performers this week.</li>'
        WHEN count_top_performers = 1 THEN CONCAT(
            '<li>🥇 <strong>', top_performer_1_name, '</strong> — ', CAST(top_performer_1_score AS STRING), ' pts</li>'
        )
        WHEN count_top_performers = 2 THEN CONCAT(
            '<li>🥇 <strong>', top_performer_1_name, '</strong> — ', CAST(top_performer_1_score AS STRING), ' pts</li>',
            '<li>🥈 <strong>', top_performer_2_name, '</strong> — ', CAST(top_performer_2_score AS STRING), ' pts</li>'
        )
        ELSE CONCAT(
            '<li>🥇 <strong>', top_performer_1_name, '</strong> — ', CAST(top_performer_1_score AS STRING), ' pts</li>',
            '<li>🥈 <strong>', top_performer_2_name, '</strong> — ', CAST(top_performer_2_score AS STRING), ' pts</li>',
            '<li>🥉 <strong>', top_performer_3_name, '</strong> — ', CAST(top_performer_3_score AS STRING), ' pts</li>'
        )
    END AS top_performers_html,
    report_generated_at
FROM aggregated
ORDER BY manager_email, week_start_date DESC
