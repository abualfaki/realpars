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

community_manager_business_assignments AS (
    -- Map managers to their business names using tag IDs
    SELECT
        m.manager_community_id,
        m.manager_first_name,
        m.manager_last_name,
        m.manager_full_name,
        m.manager_email,
        b.business_name,
        CAST(b.tag_id AS INT64) as business_tag_id,
        b.actual_member_count as business_total_members,
        b.business_size,
        b.confidence_level as business_confidence,
        true as manager_in_community,
        'community' as manager_source,
        cast(null as string) as invitation_status,
        cast(null as timestamp) as invitation_date,
        cast(null as timestamp) as join_date
    FROM managers m
    CROSS JOIN {{ ref('business_names_list') }} b
    WHERE 
        -- Manager has this business tag ID
        CAST(b.tag_id AS INT64) IN UNNEST(m.manager_tag_ids)
),

invited_manager_business_assignments AS (
    -- Map invited/non-profile-complete managers from client CSV to known business names.
    SELECT
        cast(im.manager_id as int64) as manager_community_id,
        im.first_name as manager_first_name,
        im.last_name as manager_last_name,
        im.manager_full_name,
        im.manager_email,
        b.business_name,
        cast(b.tag_id as int64) as business_tag_id,
        b.actual_member_count as business_total_members,
        b.business_size,
        b.confidence_level as business_confidence,
        coalesce(im.member_in_community, false) as manager_in_community,
        'invited_csv' as manager_source,
        im.invitation_status,
        im.invitation_date,
        im.join_date
    from {{ ref('clean_managers_not_joined_table') }} im
    inner join {{ ref('business_names_list') }} b
        -- Use exact name matching after normalizing case + internal whitespace only.
        -- Avoid stripping characters (like uppercase letters), which can collapse names (e.g., "CMC" -> "").
        on regexp_replace(lower(trim(im.business_name_from_tags)), r'\s+', ' ')
         = regexp_replace(lower(trim(b.business_name)), r'\s+', ' ')
    where im.business_name_from_tags is not null
),

manager_business_assignments AS (
    select * from community_manager_business_assignments
    union all
    select * from invited_manager_business_assignments
),

deduped_manager_business_assignments AS (
    select
        * except (dedupe_rank)
    from (
        select
            *,
            row_number() over (
                partition by lower(manager_email), business_tag_id
                order by
                    case when manager_source = 'community' then 1 else 2 end,
                    manager_in_community desc,
                    invitation_date desc
            ) as dedupe_rank
        from manager_business_assignments
    )
    where dedupe_rank = 1
),

manager_counts AS (
    -- Count community-active managers per business for team size math.
    SELECT
        business_tag_id,
        COUNT(DISTINCT manager_community_id) as manager_count
    FROM deduped_manager_business_assignments
    where manager_in_community = true
    GROUP BY business_tag_id
),

manager_business_mapping AS (
    SELECT
        mba.manager_community_id,
        mba.manager_first_name,
        mba.manager_last_name,
        mba.manager_full_name,
        mba.manager_email,
        mba.manager_source,
        mba.manager_in_community,
        mba.invitation_status,
        mba.invitation_date,
        mba.join_date,
        mba.business_name,
        mba.business_tag_id,
        mba.business_total_members,
        GREATEST(mba.business_total_members - COALESCE(mc.manager_count, 0), 0) as team_size, -- Exludes manager count
        mba.business_size,
        mba.business_confidence
    FROM deduped_manager_business_assignments mba
    LEFT JOIN manager_counts mc
        ON mc.business_tag_id = mba.business_tag_id
),

team_members AS (
    -- Find team members for each business (have Business tag ID but NOT Manager tag ID)
    SELECT
        m.community_member_id as member_community_id,
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
        mb.team_size,
        mb.business_size,
        mb.business_confidence,
        
        -- Manager info
        mb.manager_community_id,
        mb.manager_first_name,
        mb.manager_last_name,
        mb.manager_full_name,
        mb.manager_email,
        mb.manager_source,
        mb.manager_in_community,
        mb.invitation_status,
        mb.invitation_date,
        mb.join_date,
        
        -- Team member info
        tm.member_community_id,
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

SELECT 
    business_name,
    business_tag_id,
    business_total_members,
    team_size,
    business_size,
    business_confidence,
    manager_community_id,
    manager_first_name,
    manager_last_name,
    manager_full_name,
    manager_email,
    manager_source,
    manager_in_community,
    invitation_status,
    invitation_date,
    join_date,
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
    member_email