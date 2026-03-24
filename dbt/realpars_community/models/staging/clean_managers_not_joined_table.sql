{{
    config(
        materialized='view',
        schema='cc_stg_clean'
    )
}}

/*
    Clean Invited Managers Table

    Source file lives in GCS and is loaded to raw table `managers_not_joined`.
    This model normalizes key fields and extracts business name from tags.
*/

with source_data as (
    select
        uid,
        safe_cast(id as int64) as manager_id,
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        lower(trim(email)) as manager_email,
        cast(join_date as string) as join_date_raw,
        trim(tags) as tags_raw,
        trim(profile_url) as profile_url,
        cast(member__y_n_ as string) as member_flag_raw,
        trim(invitation_status) as invitation_status,
        cast(invitation_date as string) as invitation_date_raw
    from {{ source('raw_cc_data', 'managers_not_joined') }}
    where email is not null
      and trim(email) != ''
),

parsed_tags as (
    select
        *,
        array(
            select trim(tag_name)
            from unnest(regexp_extract_all(coalesce(tags_raw, ''), r'"([^"]+)"')) as tag_name
            where trim(tag_name) != ''
        ) as tag_names
    from source_data
),

normalized as (
    select
        uid,
        manager_id,
        first_name,
        last_name,
        trim(concat(coalesce(first_name, ''), ' ', coalesce(last_name, ''))) as manager_full_name,
        manager_email,
        case
            when join_date_raw is not null and join_date_raw != '' then safe_cast(join_date_raw as timestamp)
            else null
        end as join_date,
        profile_url,
        case
            when lower(member_flag_raw) in ('yes', 'y') then true
            when lower(member_flag_raw) in ('no', 'n') then false
            else null
        end as member_in_community,
        lower(nullif(invitation_status, '')) as invitation_status,
        case
            when invitation_date_raw is not null and invitation_date_raw != '' then safe_cast(invitation_date_raw as timestamp)
            else null
        end as invitation_date,
        tag_names,
        (
            select trim(candidate_tag)
            from unnest(tag_names) as candidate_tag
            where lower(trim(candidate_tag)) not in (
                'business',
                'manager',
                'pro',
                'basic',
                'individual',
                'partner',
                'source is thinkific',
                'checklist completed',
                'ac - master list import'
            )
              and lower(candidate_tag) not like 'completed -%'
              and lower(candidate_tag) not like 'cancel confirmed%'
              and lower(candidate_tag) not like '%cancelled%'
            order by length(candidate_tag) desc
            limit 1
        ) as business_name_from_tags,
        current_timestamp() as cleaned_at
    from parsed_tags
)

select
    uid,
    manager_id,
    first_name,
    last_name,
    manager_full_name,
    manager_email,
    join_date,
    profile_url,
    member_in_community,
    invitation_status,
    invitation_date,
    tag_names,
    business_name_from_tags,
    cleaned_at
from normalized
where manager_email is not null
  and manager_email != ''