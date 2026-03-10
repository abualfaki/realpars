{{ config(
    materialized = 'view'
)}}

WITH source AS (
    SELECT *
    FROM {{ source('raw_cc_data', 'events_list') }}
),

base_data as (
    SELECT
        -- Primary identifiers
        id as event_id,
        user_id,
        community_member_id as hosts_community_member_id,
        
        -- Host/Member information
        host as host_name,
        lower(trim(member_email)) as host_email,
        member_avatar_url as host_avatar_url,
        
        -- Event details
        name as event_name,
        slug as event_slug,
        url as event_url,
        
        -- Extract event body text (clean HTML)
        regexp_replace(
          regexp_replace(
            body,
            r'<[^>]*>',  -- Remove HTML tags
            ' '
          ),
          r'\s+',  -- Normalize whitespace
          ' '
        ) as event_description,
        
        -- Extract space information from JSON
        safe_cast(json_extract_scalar(space, '$.id') as int64) as space_id,
        json_extract_scalar(space, '$.name') as space_name,
        json_extract_scalar(space, '$.slug') as space_slug,
        safe_cast(json_extract_scalar(space, '$.community_id') as int64) as space_community_id,
        
        -- Parse topics array
        topics,
        case 
          when topics is not null and json_type(topics) = 'array' then
            array_length(json_extract_array(topics))
          else 0
        end as topic_count,
        
        -- Event timing
        safe_cast(starts_at as timestamp) as starts_at,
        safe_cast(ends_at as timestamp) as ends_at,
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(updated_at as timestamp) as updated_at,
        
        -- Event configuration
        location_type,
        coalesce(duration_in_seconds, 0) as duration_in_seconds,
        coalesce(rsvp_disabled, false) as rsvp_disabled,
        coalesce(hide_attendees, false) as hide_attendees,
        coalesce(hide_meta_info, false) as hide_meta_info,
        coalesce(hide_location_from_non_attendees, false) as hide_location_from_non_attendees,
        
        -- Notifications
        coalesce(send_email_reminder, false) as send_email_reminder,
        coalesce(send_email_confirmation, false) as send_email_confirmation,
        coalesce(send_in_app_notification_reminder, false) as send_in_app_notification_reminder,
        coalesce(send_in_app_notification_confirmation, false) as send_in_app_notification_confirmation,
        coalesce(enable_custom_thank_you_message, false) as enable_custom_thank_you_message,
        
        -- Engagement metrics
        coalesce(likes_count, 0) as likes_count,
        coalesce(comments_count, 0) as comments_count,
        
        -- Media
        cover_image_url,
        zapier_display_title,
        
        -- Airbyte metadata
        _airbyte_extracted_at as last_synced_at,
        _airbyte_raw_id as airbyte_record_id,
        
        -- ETL metadata
        current_timestamp() as cleaned_at
        
    FROM source
    WHERE 
        -- Filter out invalid records
        id IS NOT NULL
)

SELECT * FROM base_data
