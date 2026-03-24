{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: Top Movers Summary (Flattened)

    Returns ONE row per manager per week with the top 3 movers as flat columns.
    "Top movers" = members with the highest positive points_change vs the previous week.

    Use this in Make.com to populate the "Top Movers" section of the email.
    All variables are available directly on the single row — no iteration needed.

    Example usage in Make.com:
    SELECT * FROM manager_email_top_movers_makecom
    WHERE manager_email = '{{manager_email}}'
    AND week_start_date = (SELECT MAX(week_start_date) FROM manager_email_report_with_rankings)

    Variables emitted:
        count_movers            — 0, 1, 2 or 3
        top_mover_1_name        — full name of rank-1 mover (NULL if none)
        top_mover_1_change      — points gained this week vs last
        top_mover_2_name        — full name of rank-2 mover (NULL if none)
        top_mover_2_change
        top_mover_3_name        — full name of rank-3 mover (NULL if none)
        top_mover_3_change
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

movers AS (
    SELECT
        manager_email,
        week_start_date,
        member_full_name,
        previous_week_points,
        points_change,
        mover_rank
    FROM {{ ref('manager_email_report_with_rankings') }}
    WHERE is_top_mover = TRUE
      AND points_change > 0
      AND mover_rank <= 3
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

        -- How many movers are available (0–3)
        COUNTIF(m.mover_rank IS NOT NULL)                                     AS count_movers,

        -- Rank 1
        MAX(CASE WHEN m.mover_rank = 1 THEN m.member_full_name END)          AS top_mover_1_name,
        MAX(CASE WHEN m.mover_rank = 1 THEN m.previous_week_points END)      AS top_mover_1_prev_points,
        MAX(CASE WHEN m.mover_rank = 1 THEN m.points_change END)             AS top_mover_1_change,

        -- Rank 2
        MAX(CASE WHEN m.mover_rank = 2 THEN m.member_full_name END)          AS top_mover_2_name,
        MAX(CASE WHEN m.mover_rank = 2 THEN m.previous_week_points END)      AS top_mover_2_prev_points,
        MAX(CASE WHEN m.mover_rank = 2 THEN m.points_change END)             AS top_mover_2_change,

        -- Rank 3
        MAX(CASE WHEN m.mover_rank = 3 THEN m.member_full_name END)          AS top_mover_3_name,
        MAX(CASE WHEN m.mover_rank = 3 THEN m.previous_week_points END)      AS top_mover_3_prev_points,
        MAX(CASE WHEN m.mover_rank = 3 THEN m.points_change END)             AS top_mover_3_change,

        MAX(mw.report_generated_at)                                           AS report_generated_at
    FROM manager_weeks mw
    LEFT JOIN movers m
        ON mw.manager_email = m.manager_email
       AND mw.week_start_date = m.week_start_date
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
    aggregated.manager_email,
    aggregated.manager_full_name,
    aggregated.business_name,
    aggregated.week_start_date,
    aggregated.week_end_date,
    aggregated.week_start_formatted,
    aggregated.week_end_formatted,
    aggregated.count_movers,
    aggregated.top_mover_1_name,
    aggregated.top_mover_1_prev_points,
    aggregated.top_mover_1_change,
    aggregated.top_mover_2_name,
    aggregated.top_mover_2_prev_points,
    aggregated.top_mover_2_change,
    aggregated.top_mover_3_name,
    aggregated.top_mover_3_prev_points,
    aggregated.top_mover_3_change,
    CASE
        WHEN aggregated.count_movers = 0 AND COALESCE(top_performer_counts.count_top_performers, 0) > 0
            THEN 'Your top performers did well, but their points stayed flat or dropped compared with last week.'
        ELSE ''
    END AS top_movers_context_note,
    CASE
        WHEN aggregated.count_movers = 0 THEN '<li>No top movers this week.</li>'
        WHEN aggregated.count_movers = 1 THEN CONCAT(
            '<li>🚀 <strong>', aggregated.top_mover_1_name, '</strong> — +', CAST(aggregated.top_mover_1_change AS STRING), ' pts',
            ' (',
            CASE
                WHEN COALESCE(aggregated.top_mover_1_prev_points, 0) = 0 THEN 'strong jump this week'
                WHEN SAFE_DIVIDE(aggregated.top_mover_1_change, aggregated.top_mover_1_prev_points) >= 0.5 THEN 'strong jump this week'
                ELSE 'steady progress'
            END,
            ')</li>'
        )
        WHEN aggregated.count_movers = 2 THEN CONCAT(
            '<li>🚀 <strong>', aggregated.top_mover_1_name, '</strong> — +', CAST(aggregated.top_mover_1_change AS STRING), ' pts',
            ' (',
            CASE
                WHEN COALESCE(aggregated.top_mover_1_prev_points, 0) = 0 THEN 'strong jump this week'
                WHEN SAFE_DIVIDE(aggregated.top_mover_1_change, aggregated.top_mover_1_prev_points) >= 0.5 THEN 'strong jump this week'
                ELSE 'steady progress'
            END,
            ')</li>',
            '<li>📈 <strong>', aggregated.top_mover_2_name, '</strong> — +', CAST(aggregated.top_mover_2_change AS STRING), ' pts',
            ' (',
            CASE
                WHEN COALESCE(aggregated.top_mover_2_prev_points, 0) = 0 THEN 'strong jump this week'
                WHEN SAFE_DIVIDE(aggregated.top_mover_2_change, aggregated.top_mover_2_prev_points) >= 0.5 THEN 'strong jump this week'
                ELSE 'steady progress'
            END,
            ')</li>'
        )
        ELSE CONCAT(
            '<li>🚀 <strong>', aggregated.top_mover_1_name, '</strong> — +', CAST(aggregated.top_mover_1_change AS STRING), ' pts',
            ' (',
            CASE
                WHEN COALESCE(aggregated.top_mover_1_prev_points, 0) = 0 THEN 'strong jump this week'
                WHEN SAFE_DIVIDE(aggregated.top_mover_1_change, aggregated.top_mover_1_prev_points) >= 0.5 THEN 'strong jump this week'
                ELSE 'steady progress'
            END,
            ')</li>',
            '<li>📈 <strong>', aggregated.top_mover_2_name, '</strong> — +', CAST(aggregated.top_mover_2_change AS STRING), ' pts',
            ' (',
            CASE
                WHEN COALESCE(aggregated.top_mover_2_prev_points, 0) = 0 THEN 'strong jump this week'
                WHEN SAFE_DIVIDE(aggregated.top_mover_2_change, aggregated.top_mover_2_prev_points) >= 0.5 THEN 'strong jump this week'
                ELSE 'steady progress'
            END,
            ')</li>',
            '<li>⬆️ <strong>', aggregated.top_mover_3_name, '</strong> — +', CAST(aggregated.top_mover_3_change AS STRING), ' pts',
            ' (',
            CASE
                WHEN COALESCE(aggregated.top_mover_3_prev_points, 0) = 0 THEN 'strong jump this week'
                WHEN SAFE_DIVIDE(aggregated.top_mover_3_change, aggregated.top_mover_3_prev_points) >= 0.5 THEN 'strong jump this week'
                ELSE 'steady progress'
            END,
            ')</li>'
        )
    END AS top_movers_html,
    aggregated.report_generated_at
FROM aggregated
LEFT JOIN (
    SELECT
        manager_email,
        week_start_date,
        COUNTIF(is_top_performer = TRUE) AS count_top_performers
    FROM {{ ref('manager_email_report_with_rankings') }}
    GROUP BY manager_email, week_start_date
) top_performer_counts
    ON aggregated.manager_email = top_performer_counts.manager_email
   AND aggregated.week_start_date = top_performer_counts.week_start_date
ORDER BY aggregated.manager_email, aggregated.week_start_date DESC
