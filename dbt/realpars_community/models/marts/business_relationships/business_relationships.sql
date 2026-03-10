{{
    config(
        materialized='table',
        schema='cc_business_members_relationships'
    )
}}

/*
    Business to Manager to Team Member Relationships
    
    Creates relationship mapping between:
    - Businesses (identified from tags)
    - Managers (members with both 'Manager' and 'Business' tags)
    - Team Members (members with business tag but not manager tag)
    
    Purpose: Enable sending weekly reports to managers about their team's activity
    
    Source: Community members, member tags, and business names list
*/

WITH system_tag_ids AS (
    -- Get the tag IDs for Manager and Business system tags
    SELECT 
        CAST(MAX(CASE WHEN LOWER(tag_name) = 'manager' THEN tag_id END) AS INT64) as manager_tag_id,
        CAST(MAX(CASE WHEN LOWER(tag_name) = 'business' THEN tag_id END) AS INT64) as business_tag_id
    FROM {{ ref('clean_member_tags_stg') }}
),

members_with_tags AS (
    -- Filter to members who have tags
    SELECT
        community_member_id,
        member_id,
        first_name,
        last_name,
        full_name,
        email,
        posts_count,
        comments_count,
        tag_names,
        tag_ids,
        tag_count,
        profile_completeness
    FROM {{ source('cc_stg_clean', 'clean_communtity_members_table') }}
    WHERE 
        tag_ids IS NOT NULL 
        AND ARRAY_LENGTH(tag_ids) > 0
),

managers AS (
    -- Find members who are managers (have both Manager and Business tag IDs)
    SELECT DISTINCT
        m.community_member_id as manager_community_id,
        m.member_id as manager_member_id,
        m.first_name as manager_first_name,
        m.last_name as manager_last_name,
        m.full_name as manager_full_name,
        m.email as manager_email,
        m.tag_ids as manager_tag_ids
    FROM members_with_tags m
    CROSS JOIN system_tag_ids s
    WHERE 
        -- Must have both Manager and Business tag IDs
        s.manager_tag_id IN UNNEST(m.tag_ids)
        AND s.business_tag_id IN UNNEST(m.tag_ids)
),

manager_business_mapping AS (
    -- Map managers to their business names using tag IDs
    SELECT
        m.manager_community_id,
        m.manager_member_id,
        m.manager_first_name,
        m.manager_last_name,
        m.manager_full_name,
        m.manager_email,
        b.business_name,
        CAST(b.tag_id AS INT64) as business_tag_id,
        b.actual_member_count as business_total_members,
        b.business_size,
        b.confidence_level as business_confidence
    FROM managers m
    CROSS JOIN {{ ref('business_names_list') }} b
    WHERE 
        -- Manager has this business tag ID
        CAST(b.tag_id AS INT64) IN UNNEST(m.manager_tag_ids)
),

team_members AS (
    -- Find team members for each business (have Business tag ID but NOT Manager tag ID)
    SELECT
        m.community_member_id as member_community_id,
        m.member_id as member_member_id,
        m.first_name as member_first_name,
        m.last_name as member_last_name,
        m.full_name as member_full_name,
        m.email as member_email,
        m.posts_count as member_posts_count,
        m.comments_count as member_comments_count,
        m.profile_completeness as member_profile_completeness,
        m.tag_ids as member_tag_ids
    FROM members_with_tags m
    CROSS JOIN system_tag_ids s
    WHERE 
        -- Has Business tag ID but NOT Manager tag ID
        s.business_tag_id IN UNNEST(m.tag_ids)
        AND s.manager_tag_id NOT IN UNNEST(m.tag_ids)
),

team_members_with_business AS (
    -- Expand tag_ids array into rows to get individual business tag IDs per member
    SELECT
        tm.*,
        CAST(tag_id AS INT64) as business_tag_id
    FROM team_members tm,
    UNNEST(tm.member_tag_ids) as tag_id
),

business_relationships AS (
    -- Join managers with their team members via business name
    SELECT
        mb.business_name,
        mb.business_tag_id,
        mb.business_total_members,
        mb.business_size,
        mb.business_confidence,
        
        -- Manager info
        mb.manager_community_id,
        mb.manager_member_id,
        mb.manager_first_name,
        mb.manager_last_name,
        mb.manager_full_name,
        mb.manager_email,
        
        -- Team member info
        tm.member_community_id,
        tm.member_member_id,
        tm.member_first_name,
        tm.member_last_name,
        tm.member_full_name,
        tm.member_email,
        tm.member_posts_count,
        tm.member_comments_count,
        tm.member_profile_completeness,
        
        -- Relationship metadata
        CASE 
            WHEN tm.member_community_id IS NOT NULL THEN 'has_team'
            ELSE 'manager_only'
        END as relationship_type,
        
        -- ETL metadata
        CURRENT_TIMESTAMP() as transformed_at
        
    FROM manager_business_mapping mb
    LEFT JOIN team_members_with_business tm
        ON tm.business_tag_id = mb.business_tag_id
)

/*SELECT 
    business_name,
    business_tag_id,
    business_total_members,
    business_size,
    business_confidence,
    manager_community_id,
    manager_first_name,
    manager_last_name,
    manager_full_name,
    manager_email,
    member_community_id,
    member_first_name,
    member_last_name,
    member_full_name,
    member_email,
    member_profile_completeness,
    relationship_type,
    transformed_at
FROM business_relationships
ORDER BY 
    business_name,
    manager_email,
    member_email */

SELECT COUNT(DISTINCT business_tag_id) FROM business_relationships
