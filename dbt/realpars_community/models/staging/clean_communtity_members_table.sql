WITH source AS (
    SELECT * 
    FROM {{ source('raw_cc_data', 'community_members') }}
    WHERE 
        community_member_id IS NOT NULL
        OR email IS NOT NULL
),

base_data as (
    select
        -- Primary identifiers
        community_member_id,

        -- Personal information
        initcap(trim(first_name))                                       as first_name,
        initcap(trim(last_name))                                        as last_name,
        initcap(trim(name))                                             as full_name,
        lower(trim(email))                                              as email,

        -- Activity metrics
        coalesce(posts_count, 0)                                        as posts_count,
        coalesce(comments_count, 0)                                     as comments_count,

        -- Timestamps
        case
            when created_at is not null
            then parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', created_at)
            else null
        end                                                             as user_profile_created_at,

        -- Member tags raw JSON
        member_tags,

        -- Data quality flags
        case
            when first_name is null or trim(first_name) = '' then false
            else true
        end                                                             as has_first_name,

        case
            when last_name is null or trim(last_name) = '' then false
            else true
        end                                                             as has_last_name,

        case
            when email is null or trim(email) = '' then false
            else true
        end                                                             as has_email,

        -- Airbyte metadata
        _airbyte_extracted_at                                           as last_synced_at,
        _airbyte_raw_id                                                 as airbyte_record_id,

        -- dbt metadata
        current_timestamp()                                             as cleaned_at

    from source
),

tag_counts as (
    select
        community_member_id,

        -- Count number of tags
        case
            when member_tags is null then 0
            when json_type(member_tags) = 'array'
            then array_length(json_extract_array(member_tags))
            else 0
        end                                                             as tag_count,

        -- Extract tag names as array
        case
            when member_tags is not null and json_type(member_tags) = 'array'
            then array(
                select json_extract_scalar(tag, '$.name')
                from unnest(json_extract_array(member_tags)) as tag
            )
            else []
        end                                                             as tag_names,

        -- Extract tag IDs as array
        case
            when member_tags is not null and json_type(member_tags) = 'array'
            then array(
                select safe_cast(json_extract_scalar(tag, '$.id') as int64)
                from unnest(json_extract_array(member_tags)) as tag
            )
            else []
        end                                                             as tag_ids

    from base_data
),


final as (
    select
        -- All base fields
        b.community_member_id,
        b.first_name,
        b.last_name,
        b.full_name,
        b.email,
        b.posts_count,
        b.comments_count,
        b.user_profile_created_at,
        b.has_first_name,
        b.has_last_name,
        b.has_email,
        b.last_synced_at,
        b.airbyte_record_id,
        b.cleaned_at,

        -- Tag fields from tag_counts CTE
        coalesce(t.tag_count, 0)                                        as tag_count,
        t.tag_names,
        t.tag_ids,

        -- Profile completeness
        case
            when b.has_first_name and b.has_last_name and b.has_email then 'complete'
            when b.has_email then 'partial'
            else 'incomplete'
        end                                                             as profile_completeness,

        -- Days since created
        case
            when b.user_profile_created_at is not null
            then date_diff(current_date(), date(b.user_profile_created_at), day)
            else null
        end                                                             as days_since_created,

    from base_data b
    left join tag_counts t
        on b.community_member_id = t.community_member_id
)

select * from final