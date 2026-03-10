{{
    config(
        materialized='table',
        schema='cc_business_members_relationships'
    )
}}

/*
    Businesses Without Managers
    
    Identifies businesses that have members but no assigned managers.
    
    Logic:
    - Takes all businesses from business_names_list
    - Excludes businesses that appear in business_relationships (which only includes businesses with managers)
    - Lists all team members who could be assigned the Manager tag
    
    Use cases:
    - Identify which businesses need manager assignment
    - Understand coverage gaps in the manager-team structure
    - Prioritize businesses for manager onboarding
    - See list of members to promote to manager
    
    Source: Business names list, business relationships, and community members
*/

WITH system_tag_ids AS (
    -- Get the tag IDs for Manager and Business system tags
    SELECT 
        CAST(MAX(CASE WHEN LOWER(tag_name) = 'manager' THEN tag_id END) AS INT64) as manager_tag_id,
        CAST(MAX(CASE WHEN LOWER(tag_name) = 'business' THEN tag_id END) AS INT64) as business_tag_id
    FROM {{ ref('clean_member_tags_stg') }}
),

businesses_with_managers AS (
    -- Get all unique business tag IDs that have at least one manager assigned
    SELECT DISTINCT
        business_tag_id
    FROM {{ ref('business_relationships') }}
),

businesses_without_managers AS (
    -- Find businesses that don't have any managers
    SELECT
        b.tag_id as business_tag_id,
        b.business_name,
        b.actual_member_count,
        b.business_size,
        b.confidence_level,
        b.has_corporate_suffix,
        b.business_rank,
        b.business_age_category,
        b.visible_beside_name,
        b.is_public,
        b.created_at,
        b.updated_at,
        b.days_since_created,
        b.transformed_at
    FROM {{ ref('business_names_list') }} b
    LEFT JOIN businesses_with_managers bwm
        ON CAST(b.tag_id AS INT64) = bwm.business_tag_id
    WHERE 
        bwm.business_tag_id IS NULL  -- No manager assigned
),

business_members AS (
    -- Get members for each business without managers
    SELECT
        bwm.business_tag_id,
        bwm.business_name,
        cm.email,
        cm.full_name,
        cm.tag_ids
    FROM businesses_without_managers bwm
    INNER JOIN {{ source('cc_stg_clean', 'clean_communtity_members_table') }} cm
        ON CAST(bwm.business_tag_id AS INT64) IN UNNEST(cm.tag_ids)
    CROSS JOIN system_tag_ids s
    WHERE 
        -- Member does NOT have the manager tag
        (s.manager_tag_id NOT IN UNNEST(cm.tag_ids) OR s.manager_tag_id IS NULL)
        AND cm.email IS NOT NULL
),

member_list_aggregated AS (
    -- Aggregate member emails into a comma-separated list per business
    SELECT
        business_tag_id,
        STRING_AGG(email, ', ' ORDER BY email) as member_emails
    FROM business_members
    GROUP BY business_tag_id
)

SELECT 
    bwm.business_tag_id,
    bwm.business_name,
    bwm.actual_member_count,
    bwm.business_size,
    bwm.confidence_level,
    bwm.has_corporate_suffix,
    bwm.business_rank,
    bwm.business_age_category,
    bwm.visible_beside_name,
    bwm.is_public,
    bwm.created_at,
    bwm.updated_at,
    bwm.days_since_created,
    -- Action needed column
    CONCAT('Add Manager tag to at least one member from: ', COALESCE(mla.member_emails, 'NO MEMBERS FOUND')) as action_needed,
    bwm.transformed_at
FROM businesses_without_managers bwm
LEFT JOIN member_list_aggregated mla
    ON bwm.business_tag_id = mla.business_tag_id
ORDER BY 
    bwm.actual_member_count DESC,
    bwm.business_name
