{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: PDF Top Movers + Suggested Actions

    Purpose:
    - Provide PDFMonkey-ready HTML blocks for:
      1) Top Movers
      2) Suggested Actions
      3) Combined summary block

    Edge cases handled in SQL:
    - No top movers
    - No top performer
    - Top performer exists but no top movers

    Notes:
    - Uses manager_email_report_with_rankings to avoid recomputing rankings/points logic.
    - Adds activity-driver labels for movers:
      - higher engagement (likes/comments activity)
      - more lessons
      - more live classes
      - more lessons + engagement
      - steady growth
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
        EXTRACT(ISOWEEK FROM week_start_date) AS week_number,
        EXTRACT(YEAR FROM week_start_date) AS year,
        report_generated_at
    FROM {{ ref('manager_email_report_with_rankings') }}
),

top_performer AS (
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
),

top_movers_labeled AS (
    SELECT
        manager_email,
        week_start_date,
        mover_rank,
        member_first_name,
        member_full_name,
        points_change,
        previous_week_points,
        classes_attended,
        lessons_completed,
        (COALESCE(likes_received, 0) + COALESCE(comments_received, 0) + COALESCE(comments_made, 0)) AS engagement_events,
        CASE
            WHEN lessons_completed > 0
                 AND (COALESCE(likes_received, 0) + COALESCE(comments_received, 0) + COALESCE(comments_made, 0)) > 0
                 AND lessons_completed >= classes_attended
                 AND (COALESCE(likes_received, 0) + COALESCE(comments_received, 0) + COALESCE(comments_made, 0)) >= classes_attended
                THEN 'more lessons + engagement'
            WHEN lessons_completed >= classes_attended
                 AND lessons_completed >= (COALESCE(likes_received, 0) + COALESCE(comments_received, 0) + COALESCE(comments_made, 0))
                 AND lessons_completed > 0
                THEN 'more lessons'
            WHEN classes_attended >= lessons_completed
                 AND classes_attended >= (COALESCE(likes_received, 0) + COALESCE(comments_received, 0) + COALESCE(comments_made, 0))
                 AND classes_attended > 0
                THEN 'more live classes'
            WHEN (COALESCE(likes_received, 0) + COALESCE(comments_received, 0) + COALESCE(comments_made, 0)) > 0
                THEN 'higher engagement'
            WHEN COALESCE(previous_week_points, 0) = 0
                 OR SAFE_DIVIDE(points_change, NULLIF(previous_week_points, 0)) >= 0.5
                THEN 'strong jump this week'
            ELSE 'steady growth'
        END AS mover_driver_label,
        CASE
            WHEN COALESCE(previous_week_points, 0) = 0
                 OR SAFE_DIVIDE(points_change, NULLIF(previous_week_points, 0)) >= 0.5
                THEN 'strong jump this week'
            ELSE 'steady growth'
        END AS jump_label
    FROM {{ ref('manager_email_report_with_rankings') }}
    WHERE is_top_mover = TRUE
      AND points_change > 0
      AND mover_rank <= 3
),

movers_agg AS (
    SELECT
        manager_email,
        week_start_date,
        COUNT(*) AS count_top_movers,
        STRING_AGG(
            CONCAT(
                '<li>',
                member_full_name,
                ' — +', CAST(points_change AS STRING),
                ' pts (', mover_driver_label, ')</li>'
            ),
            ''
            ORDER BY mover_rank
        ) AS movers_li_html,
        MAX(CASE WHEN mover_rank = 1 THEN member_first_name END) AS top_mover_1_first_name,
        MAX(CASE WHEN mover_rank = 1 THEN jump_label END) AS top_mover_1_jump_label
    FROM top_movers_labeled
    GROUP BY manager_email, week_start_date
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

    -- Detailed fields for optional token mapping
    tp.member_full_name AS top_performer_name,
    tp.total_points AS top_performer_points,
    ma.count_top_movers,
    ma.top_mover_1_first_name,
    ma.top_mover_1_jump_label,
    rc.member_full_name AS reengage_member_name,

    -- Section 1: Top Movers HTML
    CONCAT(
        CASE
            WHEN COALESCE(ma.count_top_movers, 0) = 0 THEN '<p>No top movers this week.</p>'
            ELSE CONCAT('<ul>', ma.movers_li_html, '</ul>')
        END
    ) AS top_movers_html,

    -- Section 2: Suggested Actions HTML
    CONCAT(
        '<ul>',
        IF(
            tp.member_first_name IS NOT NULL,
            CONCAT('<li>🎉 <strong>Recognize ', tp.member_first_name, '</strong> – steady performance across lessons, classes, and community.</li>'),
            '<li>🎉 <strong>No recognition this week</strong> – no top performer recorded.</li>'
        ),
        IF(
            ma.top_mover_1_first_name IS NOT NULL,
            CONCAT(
                '<li>👏 <strong>Encourage ', ma.top_mover_1_first_name, '</strong> – ',
                CASE
                    WHEN ma.top_mover_1_jump_label = 'strong jump this week' THEN 'noticeable jump this week, keep building.'
                    ELSE 'steady growth this week, keep building.'
                END,
                '</li>'
            ),
            '<li>👏 <strong>Encourage team</strong> – no notable jump this week; focus on small wins.</li>'
        ),
        IF(
            rc.member_first_name IS NOT NULL,
            CONCAT('<li>🕊️ <strong>Support ', rc.member_first_name, '</strong> – very low engagement; suggest a quick lesson or invite to the next live class.</li>'),
            '<li>🕊️ <strong>No low-engagement member flagged</strong> – everyone showed some momentum this week.</li>'
        ),
        '</ul>'
    ) AS suggested_actions_html,

    -- Combined block for single-token insertion in PDFMonkey
    CONCAT(
        CASE
            WHEN COALESCE(ma.count_top_movers, 0) = 0 THEN '<p>No top movers this week.</p>'
            ELSE CONCAT('<ul>', ma.movers_li_html, '</ul>')
        END,
        '<ul>',
        IF(
            tp.member_first_name IS NOT NULL,
            CONCAT('<li>🎉 <strong>Recognize ', tp.member_first_name, '</strong> – steady performance across lessons, classes, and community.</li>'),
            '<li>🎉 <strong>No recognition this week</strong> – no top performer recorded.</li>'
        ),
        IF(
            ma.top_mover_1_first_name IS NOT NULL,
            CONCAT(
                '<li>👏 <strong>Encourage ', ma.top_mover_1_first_name, '</strong> – ',
                CASE
                    WHEN ma.top_mover_1_jump_label = 'strong jump this week' THEN 'noticeable jump this week, keep building.'
                    ELSE 'steady growth this week, keep building.'
                END,
                '</li>'
            ),
            '<li>👏 <strong>Encourage team</strong> – no notable jump this week; focus on small wins.</li>'
        ),
        IF(
            rc.member_first_name IS NOT NULL,
            CONCAT('<li>🕊️ <strong>Support ', rc.member_first_name, '</strong> – very low engagement; suggest a quick lesson or invite to the next live class.</li>'),
            '<li>🕊️ <strong>No low-engagement member flagged</strong> – everyone showed some momentum this week.</li>'
        ),
        '</ul>'
    ) AS pdf_summary_html,

    mw.report_generated_at

FROM manager_weeks mw
LEFT JOIN top_performer tp
    ON mw.manager_email = tp.manager_email
   AND mw.week_start_date = tp.week_start_date
   AND tp.rn = 1
LEFT JOIN movers_agg ma
    ON mw.manager_email = ma.manager_email
   AND mw.week_start_date = ma.week_start_date
LEFT JOIN reengage_candidate rc
    ON mw.manager_email = rc.manager_email
   AND mw.week_start_date = rc.week_start_date
   AND rc.rn = 1

ORDER BY mw.manager_email, mw.week_start_date DESC
