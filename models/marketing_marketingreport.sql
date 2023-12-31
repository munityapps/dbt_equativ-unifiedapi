{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT 
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_reports"."campaign_id" ||
      "{{ var("table_prefix") }}_reports"."site_url" ||
      "{{ var("table_prefix") }}_reports"."country_id" ||
      "{{ var("table_prefix") }}_reports"."mobile_connection_type_id" ||
      date_trunc('hour', "{{ var("table_prefix") }}_reports"."_airbyte_emitted_at") ||
      'campaign' ||
      'equativ'
    )  as id,
    'equativ' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_reports._airbyte_data as last_raw_data, 
    "{{ var("table_prefix") }}_reports"."campaign_id" as external_id,
    '{{ var("timestamp") }}' as sync_timestamp,
    date_trunc('hour', "{{ var("table_prefix") }}_reports"."_airbyte_emitted_at") as event_hour,
    impressions,
    campaign_id,
    campaign_name,
    to_timestamp(SUBSTRING(campaign_start_date, 1, 10)::numeric)::date as campaign_start_date,
    to_timestamp(SUBSTRING(campaign_end_date, 1, 10)::numeric)::date as campaign_end_date,
    campaign_external_id,
    campaign_status,
    site_name,
    site_url,
    image_weight,
    video_content_duration,
    mobile_connection_type_id,
    country_id,
    country_name,
    agency_name,
    advertiser_name,
    advertiser_sales_person_name,
    domain,
    pack_id,
    pack_name
FROM "{{ var("table_prefix") }}_reports"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_reports
ON _airbyte_raw_{{ var("table_prefix") }}_reports._airbyte_ab_id = "{{ var("table_prefix") }}_reports"._airbyte_ab_id

