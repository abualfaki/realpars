{{
    config(
        materialized='table',
        schema='cc_business_members_relationships'
    )
}}

/*
    Business Names List
    
    Extracts business names from member tags by identifying potential business name tags.
    
    Logic:
    - Business names are tags in the 'potential_business_name' category
    - Excludes known system tags (Manager, Business, etc.)
    - Calculates actual member counts from community members table
    - Includes confidence scoring and business size classification
    
    Source: Clean member tags and community members tables
*/

WITH 
-- Recalculate actual member counts from all tag_id's from community_members table
actual_member_counts AS (
    SELECT 
        CAST(tag_id AS INT64) as tag_id,
        COUNT(DISTINCT email) as actual_member_count -- Memeber count from all tag id's
    FROM {{ source('cc_stg_clean', 'clean_communtity_members_table') }},
        UNNEST(tag_ids) as tag_id
    GROUP BY tag_id
),

potential_business_tags AS (
    SELECT
        mt.tag_id,
        mt.tag_name as business_name,
        -- Use actual member count instead of tagged_members_count from API
        COALESCE(amc.actual_member_count, 0) as actual_member_count,
        mt.tagged_members_count as api_member_count,  -- Original API count (for reference)
        mt.visible_beside_name,
        mt.is_public,
        mt.created_at,
        mt.updated_at,
        mt.days_since_created,
        
        -- Additional confidence scoring (now based on actual member count)
        CASE 
            -- High confidence: visible, has members, right category
            WHEN mt.tag_category = 'potential_business_name' 
                AND mt.visible_beside_name = true 
                AND COALESCE(amc.actual_member_count, 0) >= 2 
                THEN 'high'
            -- Medium confidence: visible but few members
            WHEN mt.tag_category = 'potential_business_name' 
                AND mt.visible_beside_name = true 
                AND COALESCE(amc.actual_member_count, 0) = 1 
                THEN 'medium'
            -- Low confidence: not visible or edge case
            WHEN mt.tag_category = 'potential_business_name' 
                THEN 'low'
            ELSE 'not_business'
        END as confidence_level,
        
        -- Check if name looks corporate
        CASE 
            WHEN REGEXP_CONTAINS(mt.tag_name, r'(?i)(Inc|LLC|Ltd|Corporation|Corp|Company|Co\.|Group|Industries|International|Solutions|Technologies|Tech|Systems|Services)') 
                THEN true
            ELSE false
        END as has_corporate_suffix
        
    FROM {{ ref('clean_member_tags_stg') }} mt
    LEFT JOIN actual_member_counts amc
        ON CAST(mt.tag_id AS INT64) = amc.tag_id
    WHERE 
        mt.tag_category = 'potential_business_name'
        -- Must have at least 1 actual member (not API's stale count)
        AND COALESCE(amc.actual_member_count, 0) > 0
        -- Exclude obvious non-business tags
        AND LOWER(mt.tag_name) NOT IN (
            'manager',  -- This is a role, not a company
            'business',  -- This is a category, not a company
            'individual',  -- This is a membership tier, not a company
            'individuals',  -- Plural form
            'team',
            'member',
            'staff'
        )
        -- Exclude very long names (likely course titles)
        AND LENGTH(mt.tag_name) <= 100
),

ranked_businesses AS (
    SELECT
        *,
        -- Rank by actual member count to prioritize active businesses
        ROW_NUMBER() OVER (ORDER BY actual_member_count DESC, created_at DESC) as business_rank,
        
        -- Classify by size
        CASE 
            WHEN actual_member_count >= 20 THEN 'large'
            WHEN actual_member_count BETWEEN 10 AND 19 THEN 'medium'
            WHEN actual_member_count BETWEEN 5 AND 9 THEN 'small'
            WHEN actual_member_count BETWEEN 2 AND 4 THEN 'micro'
            ELSE 'single'
        END as business_size,
        
        -- Activity status
        CASE 
            WHEN days_since_created <= 30 THEN 'newly_added'
            WHEN days_since_created <= 90 THEN 'recent'
            WHEN days_since_created <= 365 THEN 'established'
            ELSE 'long_term'
        END as business_age_category,
        
        -- ETL metadata
        CURRENT_TIMESTAMP() as transformed_at
        
    FROM potential_business_tags
)

SELECT 
    tag_id,
    business_name,
    actual_member_count,
    api_member_count,
    visible_beside_name,
    is_public,
    created_at,
    updated_at,
    days_since_created,
    confidence_level,
    has_corporate_suffix,
    business_rank,
    business_size,
    business_age_category,
    transformed_at
FROM ranked_businesses
ORDER BY 
    business_rank,
    actual_member_count DESC