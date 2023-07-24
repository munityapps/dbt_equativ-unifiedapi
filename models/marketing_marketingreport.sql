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
      'campaign' ||
      'equativ'
    )  as id,
    'equativ' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_reports._airbyte_data as last_raw_data, 
    "{{ var("table_prefix") }}_reports"."campaign_id" as external_id,
    '{{ var("timestamp") }}' as sync_timestamp,
    impressions,
    campaign_id,
    campaign_name,
    to_timestamp(SUBSTRING(campaign_start_date, 1, 10)::numeric)::date as campaign_start_date,
    to_timestamp(SUBSTRING(campaign_end_date, 1, 10)::numeric)::date as campaign_end_date,
    campaign_external_id,
    campaign_status,
    site_name,
    site_url,
    image_weight
FROM "{{ var("table_prefix") }}_reports"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_reports
ON _airbyte_raw_{{ var("table_prefix") }}_reports._airbyte_ab_id = "{{ var("table_prefix") }}_reports"._airbyte_ab_id

