{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: Top 3 Performers per Manager
    
    Returns top 3 team members by engagement score for each manager/week.
    Pre-ranked and ready for PDF "Top 3 Performers" section.
    
    Example usage in Make.com:
    SELECT * FROM manager_top_performers_pdf_makecom
    WHERE manager_email = '{{manager_email}}'
    AND week_start_date = (SELECT MAX(week_start_date) FROM manager_weekly_team_detail_report)
    ORDER BY rank
*/

WITH latest_week AS (
  SELECT MAX(week_start_date) as latest_week_date
  FROM {{ ref('manager_weekly_team_detail_report') }}
),

ranked_team_members AS (
  SELECT 
    manager_email,
    manager_full_name,
    business_name,
    week_start_date,
    week_end_date,
    
    team_member as name,
    week_total_engagement as score,
    
    ROW_NUMBER() OVER (
      PARTITION BY manager_email, week_start_date 
      ORDER BY week_total_engagement DESC, team_member ASC
    ) as rank
    
  FROM {{ ref('manager_weekly_team_detail_report') }}
  WHERE week_start_date = (SELECT latest_week_date FROM latest_week)
)

SELECT 
  manager_email,
  manager_full_name,
  business_name,
  week_start_date,
  week_end_date,
  rank,
  name,
  score
  
FROM ranked_team_members
WHERE rank <= 3
ORDER BY manager_email, rank
