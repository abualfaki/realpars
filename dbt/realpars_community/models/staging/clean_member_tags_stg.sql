{{
    config(
        materialized='view',
        schema='cc_stg_clean'
    )
}}

/*
    Clean Member Tags
    
    Cleans and transforms member tags data from raw Airbyte table.
    
    Transformations:
    - Parses JSON fields (display_locations)
    - Categorizes tags into types (membership_tier, business_role, etc.)
    - Identifies tags visible beside member names
    - Extracts course names from completion tags
    - Enriches with calculated fields
    
    Source: Raw member_tags table from Airbyte
*/

WITH base_tags AS (
    SELECT
        -- Primary identifiers
        id as tag_id,
        TRIM(name) as tag_name,
        
        -- Tag properties
        color as tag_color,
        emoji as tag_emoji,
        COALESCE(is_public, false) as is_public,
        display_format,
        
        -- Parse display_locations JSON
        CASE 
            WHEN display_locations IS NOT NULL AND JSON_TYPE(display_locations) = 'object' THEN
                COALESCE(CAST(JSON_EXTRACT_SCALAR(display_locations, '$.member_directory') AS BOOL), false)
            ELSE false
        END as show_in_member_directory,
        
        CASE 
            WHEN display_locations IS NOT NULL AND JSON_TYPE(display_locations) = 'object' THEN
                COALESCE(CAST(JSON_EXTRACT_SCALAR(display_locations, '$.post_bio') AS BOOL), false)
            ELSE false
        END as show_in_post_bio,
        
        CASE 
            WHEN display_locations IS NOT NULL AND JSON_TYPE(display_locations) = 'object' THEN
                COALESCE(CAST(JSON_EXTRACT_SCALAR(display_locations, '$.profile_page') AS BOOL), false)
            ELSE false
        END as show_in_profile_page,
        
        -- Member count
        COALESCE(tagged_members_count, 0) as tagged_members_count,
        
        -- Timestamps
        CASE 
            WHEN created_at IS NOT NULL THEN 
                PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)
            ELSE NULL 
        END as created_at,
        
        CASE 
            WHEN updated_at IS NOT NULL THEN 
                PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', updated_at)
            ELSE NULL 
        END as updated_at,
        
        -- Airbyte metadata
        _airbyte_extracted_at as last_synced_at,
        _airbyte_raw_id as airbyte_record_id
        
    FROM {{ source('raw_cc_data', 'member_tags_list') }}
    WHERE 
        -- Filter out invalid records
        id IS NOT NULL
        AND name IS NOT NULL
),

enriched_tags AS (
    SELECT
        *,
        
        -- Tag categorization based on name patterns
        CASE 
            WHEN LOWER(tag_name) = 'basic' THEN 'membership_tier'
            WHEN LOWER(tag_name) = 'pro' THEN 'membership_tier'
            WHEN LOWER(tag_name) = 'individuals' THEN 'membership_tier'
            WHEN LOWER(tag_name) = 'individual' THEN 'membership_tier'
            WHEN LOWER(tag_name) = 'annual discount' THEN 'membership_tier'
            WHEN LOWER(tag_name) = 'free trial' THEN 'membership_tier'
            WHEN LOWER(tag_name) LIKE 'free trial%' THEN 'membership_tier'
            
            WHEN LOWER(tag_name) = 'business' THEN 'business_role'
            WHEN LOWER(tag_name) = 'manager' THEN 'business_role'
            WHEN LOWER(tag_name) = 'business trial' THEN 'business_role'
            WHEN LOWER(tag_name) LIKE 'business%' THEN 'business_role'
            
            WHEN LOWER(tag_name) = 'instructor' THEN 'community_role'
            WHEN LOWER(tag_name) = 'realpars team members' THEN 'community_role'
            
            WHEN LOWER(tag_name) LIKE 'completed -%' THEN 'course_completion'
            WHEN LOWER(tag_name) = 'checklist completed' THEN 'course_completion'
            
            WHEN LOWER(tag_name) LIKE '%cancelled%' THEN 'cancellation'
            WHEN LOWER(tag_name) LIKE 'cancel confirmed%' THEN 'cancellation'

            WHEN LOWER(tag_name) = 'removed from all space group' THEN 'removed'
            
            WHEN LOWER(tag_name) LIKE 'ac -%' THEN 'migration'
            WHEN LOWER(tag_name) = 'source is thinkific' THEN 'migration'
            
            WHEN LOWER(tag_name) = 'resell' THEN 'acquisition'
            WHEN LOWER(tag_name) = 'business member replaced' THEN 'business_management'
            
            -- Potential business name (doesn't match known patterns)
            ELSE 'potential_business_name'
        END as tag_category,
        
        -- Is this tag visible beside member name?
        CASE 
            WHEN LOWER(tag_name) IN ('pro', 'instructor') THEN true
            -- Business name tags are typically visible
            WHEN show_in_profile_page = true 
                AND show_in_member_directory = true
                AND LOWER(tag_name) NOT LIKE 'completed -%'
                AND LOWER(tag_name) NOT IN ('basic', 'individuals', 'checklist completed')
                THEN true
            ELSE false
        END as visible_beside_name,
        
        -- Extract course name from completion tags
        CASE 
            WHEN LOWER(tag_name) LIKE 'completed -%' THEN
                TRIM(REGEXP_REPLACE(tag_name, r'(?i)^completed\s*-\s*', ''))
            ELSE NULL
        END as course_name,
        
        -- Days since tag creation
        DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY) as days_since_created,
        
        -- Date decomposition
        DATE(created_at) as created_date,
        EXTRACT(YEAR FROM created_at) as created_year,
        EXTRACT(MONTH FROM created_at) as created_month
        
    FROM base_tags
)

SELECT 
    tag_id,
    tag_name,
    tag_color,
    tagged_members_count,
    created_date
FROM enriched_tags
WHERE tag_category = 'potential_business_name'
ORDER BY tagged_members_count DESC, created_at DESC
