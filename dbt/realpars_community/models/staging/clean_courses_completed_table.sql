{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['initiator_community_id', 'course_id'],
    on_schema_change = 'sync_all_columns'
)}}

WITH source AS (
    SELECT *
    FROM {{ source('raw_cc_data', 'course_completed') }}

    {% if is_incremental() %}
        -- Reprocess a small extraction lookback so late-arriving events still flow downstream.
        WHERE safe_cast(created_at AS timestamp) >= TIMESTAMP_SUB(
            COALESCE((SELECT MAX(created_at) FROM {{ this }}), TIMESTAMP('1970-01-01')),


            -- We using a 45 day lookback window because the documentation is unclear
            -- about the logic of creating values for created_at and triggered_at
            -- when a course.completed event is back populated. Futhermore, in the documentation 
            -- there are no  examples of circumstnacesthat will require the need 
            -- to back populate data.

            -- Since the Data in circle_community_raw_datsets.course_completed is 
            -- updated monthly. A 45 Day lookback window is reasonable to catch potentially
            -- backfilled events.
            INTERVAL 45 DAY
        )
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

        -- We aren't using record_id here because if a course is repeated multiple times we won't
        -- be able to tell because record_id is unique for all course completions
        PARTITION BY initiator_community_id, course_id -- better to use community_member_id because an emails can change.
        ORDER BY created_at ASC
    ) = 1
)

SELECT *
FROM deduped_clean_base_data