{{ config(
    materialized = 'view'
)}}

WITH source AS (
    SELECT *
    FROM {{ source('raw_cc_data', 'event_attendees') }}
),

base_data as (
    SELECT
        -- Primary identifiers
        id as attendee_record_id,
        event_id,
        contact_id,
        community_member_id,
        
        -- Member information
        initcap(trim(member_name)) as attendee_name,
        lower(trim(member_email)) as attendee_email,
        member_avatar_url as attendee_avatar_url,
        headline,
        
        -- Event details
        event_name,
        
        -- RSVP information
        rsvp_status,
        contact_type,
        
        -- RSVP timestamp
        safe_cast(rsvp_date as timestamp) as rsvp_date,
        
        -- Airbyte metadata
        _airbyte_extracted_at as data_last_synced_at,
        _airbyte_raw_id as airbyte_record_id,
        
        -- ETL metadata
        current_timestamp() as data_cleaned_at
        
    FROM source
    WHERE 
        -- Filter out invalid records
        id IS NOT NULL
        AND event_id IS NOT NULL
)

SELECT * FROM base_data
