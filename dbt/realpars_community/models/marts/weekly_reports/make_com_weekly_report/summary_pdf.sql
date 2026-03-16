{{
    config(
        materialized='view',
        schema='cc_make_com_weekly_reports'
    )
}}

/*
    Make.com Helper View: Manager Summary for PDF Header
    
    Returns team summary statistics for each manager/week combination.
    Used to populate PDF report header with total scores and week info.
    
    Example usage in Make.com:
    SELECT * FROM manager_summary_pdf_makecom
    WHERE manager_email = '{{manager_email}}'
    AND week_start_date = (SELECT MAX(week_start_date) FROM manager_weekly_team_detail_report)
*/

WITH latest_week AS (
  SELECT MAX(week_start_date) as latest_week_date
  FROM {{ ref('manager_weekly_team_detail_report') }}
)

SELECT 
  manager_email,
  manager_full_name,
  business_name,
  
  -- Week Info
  week_start_date,
  week_end_date,
  week_start_formatted,
  week_end_formatted,
  EXTRACT(WEEK FROM week_start_date) as week_number,
  EXTRACT(YEAR FROM week_start_date) as year,
  
  -- Team Summary Statistics
  COUNT(DISTINCT team_member) as total_team_members,
  COUNT(DISTINCT CASE WHEN activity_status = 'Active' THEN team_member END) as active_members,
  COUNT(DISTINCT CASE WHEN activity_status = 'Inactive' THEN team_member END) as inactive_members,
  
  -- Team Total Scores
  SUM(week_total_engagement) as team_total_score,
  SUM(previous_week_engagement) as team_previous_score,
  SUM(total_engagement_change) as team_score_change,
  
  -- Average per member
  ROUND(AVG(week_total_engagement), 1) as avg_score_per_member,
  
  report_generated_at

FROM {{ ref('manager_weekly_team_detail_report') }}
WHERE week_start_date = (SELECT latest_week_date FROM latest_week)
GROUP BY 
  manager_email,
  manager_full_name,
  business_name,
  week_start_date,
  week_end_date,
  week_start_formatted,
  week_end_formatted,
  report_generated_at
ORDER BY business_name, manager_full_name
