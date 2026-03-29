{{
    config(
        materialized='view',
        schema='cc_stg_clean',
        tags=['staging', 'comments'],
        enabled=false
    )
}}

WITH base_comments AS (
    SELECT
        -- Primary identifiers
        id as comment_id,
        community_id,
        parent_comment_id,
        
        -- Extract user information from JSON
        SAFE_CAST(JSON_EXTRACT_SCALAR(user, '$.id') AS INT64) as author_community_member_id,
        JSON_EXTRACT_SCALAR(user, '$.name') as author_full_name,
        JSON_EXTRACT_SCALAR(user, '$.email') as author_email,
        JSON_EXTRACT_SCALAR(user, '$.avatar_url') as author_avatar_url,
        
        -- Extract post information from JSON
        SAFE_CAST(JSON_EXTRACT_SCALAR(post, '$.id') AS INT64) as post_id,
        JSON_EXTRACT_SCALAR(post, '$.name') as post_name,
        JSON_EXTRACT_SCALAR(post, '$.slug') as post_slug,
        
        -- Extract space information from JSON
        SAFE_CAST(JSON_EXTRACT_SCALAR(space, '$.id') AS INT64) as space_id,
        JSON_EXTRACT_SCALAR(space, '$.name') as space_name,
        JSON_EXTRACT_SCALAR(space, '$.slug') as space_slug,
        
        -- Extract comment body text (clean HTML)
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                JSON_EXTRACT_SCALAR(body, '$.body'),
                r'<[^>]*>',  -- Remove HTML tags
                ' '
            ),
            r'\s+',  -- Normalize whitespace
            ' '
        ) as comment_text,
        
        -- URL
        url as comment_url,
        
        -- Timestamps
        CASE 
            WHEN created_at IS NOT NULL THEN 
                PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)
            ELSE NULL 
        END as comment_created_at,
        
        -- Engagement metrics
        COALESCE(likes_count, 0) as comment_likes_count,
        COALESCE(replies_count, 0) as replies_to_comment_count,
        
        -- User activity metrics (at time of comment)
        COALESCE(user_likes_count, 0) as authors_total_likes,
        COALESCE(user_posts_count, 0) as authors_total_posts,
        COALESCE(user_comments_count, 0) as authors_total_comments,
        
        -- Airbyte metadata
        _airbyte_extracted_at as data_last_synced_at,
        _airbyte_raw_id as airbyte_record_id,
        
        -- ETL metadata
        CURRENT_TIMESTAMP() as data_cleaned_at
        
    FROM {{ source('raw_cc_data', 'comments') }}
    WHERE 
        -- Filter out invalid records
        id IS NOT NULL
        AND community_id IS NOT NULL
        AND created_at IS NOT NULL
),

enriched_comments AS (
    SELECT
        *,
        
        -- Date decomposition
        DATE(comment_created_at) as comment_date,
        EXTRACT(YEAR FROM comment_created_at) as comment_created_year,
        EXTRACT(MONTH FROM comment_created_at) as comment_created_month,
        EXTRACT(WEEK FROM comment_created_at) as comment_created_week,
        EXTRACT(DAYOFWEEK FROM comment_created_at) as comment_created_day_of_week,
        EXTRACT(HOUR FROM comment_created_at) as comment_created_hour,
        
        -- Week start date (for weekly aggregation)
        DATE_TRUNC(DATE(comment_created_at), WEEK(MONDAY)) as comment_created_week_start_date,
        DATE_TRUNC(DATE(comment_created_at), MONTH) as comment_created_month_start_date,
        
        -- Comment classification
        CASE 
            WHEN parent_comment_id IS NOT NULL THEN 'reply'
            ELSE 'top_level'
        END as comment_type,
        
        -- Engagement level
        CASE 
            WHEN comment_likes_count = 0 AND replies_to_comment_count = 0 THEN 'no_engagement'
            WHEN comment_likes_count > 0 AND replies_to_comment_count = 0 THEN 'liked'
            WHEN comment_likes_count = 0 AND replies_to_comment_count > 0 THEN 'replied'
            WHEN comment_likes_count > 0 AND replies_to_comment_count > 0 THEN 'highly_engaged'
        END as engagement_level,
        
        -- User activity level at time of comment
        CASE 
            WHEN authors_total_comments BETWEEN 1 AND 5 THEN 'new_commenter'
            WHEN authors_total_comments BETWEEN 6 AND 20 THEN 'regular_commenter'
            WHEN authors_total_comments BETWEEN 21 AND 50 THEN 'active_commenter'
            WHEN authors_total_comments > 50 THEN 'power_commenter'
            ELSE 'unknown'
        END as commenter_level
        
    FROM base_comments
)

SELECT *
FROM enriched_comments
ORDER BY comment_created_at DESC, community_id
