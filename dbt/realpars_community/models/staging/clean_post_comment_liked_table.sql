{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'record_id',
    on_schema_change = 'sync_all_columns'
)}}

WITH source AS (
    SELECT *
    FROM {{ source('raw_cc_data', 'post_comment_liked') }}

    {% if is_incremental() %}
        -- Only new raw rows since last successful build of this model
        WHERE safe_cast(created_at AS timestamp) > 
            (SELECT MAX(safe_cast(created_at AS timestamp)) from {{ this }})
    {% endif %}
),

base_data as (
    SELECT

        -- Extracting fields from the JSON payload comment object
        json_value(payload, '$.comment.community_member_id') as comment_community_member_id,
        json_value(payload, '$.comment.id') as comment_id,
        json_value(payload, '$.comment.parent_comment_id') as parent_comment_id,
        json_value(payload, '$.comment.post_id') as post_id,
        json_value(payload, '$.comment.record_owner_is_admin') as comment_owner_is_admin,
        json_value(payload, '$.comment.type') as comment_type,

        -- Extracting fields from the JSON payload initiator object (person who liked the comment)
        json_value(payload, '$.initiator.type') as initiator_type,
        json_value(payload, '$.initiator.public_uid') as initiator_public_uid,
        json_value(payload, '$.initiator.id') as initiator_community_id,
        json_value(payload, '$.initiator.name') as initiator_name,
        json_value(payload, '$.initiator.first_name') as initiator_first_name,
        json_value(payload, '$.initiator.last_name') as initiator_last_name,
        json_value(payload, '$.initiator.email') as initiator_email,
        json_value(payload, '$.initiator.is_admin') as initiator_is_admin,
        json_value(payload, '$.initiator.is_moderator') as initiator_is_moderator,

        -- Extracting fields from the JSON payload posts_basic object
        json_value(payload, '$.posts_basic.community_member_id') as post_owner_community_member_id,
        json_value(payload, '$.posts_basic.id') as post_basic_id,
        json_value(payload, '$.posts_basic.name') as post_name,
        json_value(payload, '$.posts_basic.published_at') as post_published_at,
        json_value(payload, '$.posts_basic.record_owner_is_admin') as post_owner_is_admin,
        json_value(payload, '$.posts_basic.slug') as post_slug,
        json_value(payload, '$.posts_basic.space_id') as post_space_id,
        json_value(payload, '$.posts_basic.status') as post_status,
        json_value(payload, '$.posts_basic.type') as post_type,

        -- Extracting fields from the JSON payload space object
        json_value(payload, '$.space.id') as space_id,
        json_value(payload, '$.space.name') as space_name,
        json_value(payload, '$.space.slug') as space_slug,
        json_value(payload, '$.space.type') as space_type,

        -- Record meta data
        id as record_id,
        name as event_name,

        -- Timestamps
        safe_cast(_airbyte_extracted_at as timestamp) as _airbyte_extracted_at,
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(triggered_at as timestamp) as triggered_at,

    FROM source
),

clean_base_data as (
    SELECT
        -- Comment fields
        safe_cast(comment_id as int64) as comment_id,
        safe_cast(comment_community_member_id as int64) as comment_community_member_id,
        safe_cast(parent_comment_id as int64) as parent_comment_id,
        safe_cast(post_id as int64) as post_id,
        safe_cast(comment_owner_is_admin as bool) as comment_owner_is_admin,
        comment_type,

        -- Initiator fields (person who liked the comment)
        initiator_type,
        initiator_public_uid,
        safe_cast(initiator_community_id as int64) as initiator_community_member_id,
        initcap(trim(initiator_name)) as initiator_name,
        initcap(trim(initiator_first_name)) as initiator_first_name,
        initcap(trim(initiator_last_name)) as initiator_last_name,
        lower(trim(initiator_email)) as initiator_email,

        -- Post fields
        safe_cast(post_owner_community_member_id as int64) as post_owner_community_member_id,
        safe_cast(post_basic_id as int64) as post_basic_id,
        trim(post_name) as post_name,
        safe_cast(post_published_at as timestamp) as post_published_at,
        safe_cast(post_owner_is_admin as bool) as post_owner_is_admin,
        lower(trim(post_slug)) as post_slug,
        safe_cast(post_space_id as int64) as post_space_id,
        post_status,
        post_type,

        -- Space fields
        safe_cast(space_id as int64) as space_id,
        initcap(trim(space_name)) as space_name,
        lower(trim(space_slug)) as space_slug,
        space_type,

        -- Boolean flags
        safe_cast(initiator_is_admin as bool) as initiator_is_admin,
        safe_cast(initiator_is_moderator as bool) as initiator_is_moderator,

        -- Record meta data
        record_id,
        event_name,

        -- Timestamps
        _airbyte_extracted_at,
        created_at,
        triggered_at,

    FROM base_data
),

deduped_clean_base_data as (
    SELECT *
    FROM clean_base_data
    QUALIFY ROW_NUMBER()
    OVER (
        PARTITION BY record_id
        ORDER BY _airbyte_extracted_at DESC
    ) = 1
)

SELECT *
FROM deduped_clean_base_data
