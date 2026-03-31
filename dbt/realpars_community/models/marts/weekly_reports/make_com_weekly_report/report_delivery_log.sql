{{
    config(
        materialized='incremental',
        unique_key='delivery_id',
        schema='cc_make_com_weekly_reports',
        partition_by={
            "field": "sent_at",
            "data_type": "timestamp",
            "granularity": "month"
        }
    )
}}

/*
    Report Delivery Log
    
    This table is populated by Make.com after each email is sent to a manager.
    Make.com inserts a row via BigQuery INSERT after sending each report email.
    
    Make.com should INSERT into this table with:
    
    INSERT INTO `{{ env_var('PROJECT_ID') }}.cc_make_com_weekly_reports.report_delivery_log`
        (manager_email, manager_full_name, business_name, report_type, 
         week_start_date, sent_at, delivery_status, make_scenario_id, make_execution_id)
    VALUES
        (@manager_email, @manager_full_name, @business_name, @report_type,
         @week_start_date, CURRENT_TIMESTAMP(), 'sent', @scenario_id, @execution_id)
*/

SELECT
    CAST(NULL AS STRING) AS delivery_id,
    CAST(NULL AS STRING) AS manager_email,
    CAST(NULL AS STRING) AS manager_full_name,
    CAST(NULL AS STRING) AS business_name,
    CAST(NULL AS STRING) AS report_type,  -- 'weekly' or 'monthly'
    CAST(NULL AS DATE) AS week_start_date,
    CAST(NULL AS TIMESTAMP) AS sent_at,
    CAST(NULL AS STRING) AS delivery_status,  -- 'sent', 'failed', 'bounced'
    CAST(NULL AS STRING) AS make_scenario_id,
    CAST(NULL AS STRING) AS make_execution_id
LIMIT 0
