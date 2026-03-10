{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['initiator_email', 'course_id'],
    on_schema_change = 'sync_all_columns'
)}}

WITH source AS (
    SELECT *
    FROM {{ source('raw_cc_data', 'courses_completed') }}

    {% if is_incremental() %}
        -- Only new raw rows since last successful build of this model
        WHERE safe_cast(created_at AS timestamp) > 
            (SELECT MAX(safe_cast(created_at AS timestamp)) from {{ this }})
    {% endif %}
),

base_data as (
    SELECT

        -- Extracting fields from the JSON payload intitiator object
        json_value(payload, '$.initiator.type') as initiator_type,
        json_value(payload, '$.initiator.public_user_id') as initiator_public_user_id,
        json_value(payload, '$.initiator.id') as initiator_community_id,
        json_value(payload, '$.initiator.name') as initiator_name,
        json_value(payload, '$.initiator.first_name') as initiator_first_name,
        json_value(payload, '$.initiator.last_name') as initiator_last_name,
        json_value(payload, '$.initiator.email') as initiator_email,
        json_value(payload, '$.initiator.is_admin') as initiator_is_admin,
        json_value(payload, '$.initiator.is_moderator') as initiator_is_moderator,

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
        initiator_type,
        safe_cast(initiator_community_id as string) as initiator_community_id,
        initcap(trim(initiator_name)) as initiator_name,
        initcap(trim(initiator_first_name)) as initiator_first_name,
        initcap(trim(initiator_last_name)) as initiator_last_name,
        lower(trim(initiator_email)) as initiator_email,

        space_id as course_id,
        initcap(trim(space_name)) as course_name,
        initcap(trim(space_slug)) as course_slug,
        initcap(trim(space_type)) as space_type,

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
        PARTITION BY initiator_email, course_id
        ORDER BY created_at ASC
    ) = 1
)

SELECT *
FROM deduped_clean_base_data