WITH CTE_UNION as (
SELECT 
us.id,
us.tracking_uuid,
us.account_id,
us.country,
us.campaign_id,
us.master_campaign_id,
us.master_advertiser_id,
us.webpublisher_campaign_id,
us.original_number,
us.target_number,
us.tracking_number,
us.status_id,
us.number_type,
us.initial_number_type,
us.number_match,
us.campaign_type,
us.referrer_type,
us.call_recording,
us.whisper,
us.advertiser_address_id,
us.site_id,
us.location_id,
us.location_uri,
us.error_messages,
us.is_legacy,
us.is_match,
us.created_on,
us.updated_on,
us.remarks,
us.auto_delete_status,
us.call_verify,
us.call_verify_notification_text,
us.advertiser_business_name,
us.source,
us.datastream_metadata.uuid,
us.datastream_metadata.source_timestamp
FROM `localiq-dms-analytics-v2-prod.rl_cts.TrackingNumber` us

UNION ALL

SELECT 
ca.id,
ca.tracking_uuid,
ca.account_id,
ca.country,
ca.campaign_id,
ca.master_campaign_id,
ca.master_advertiser_id,
ca.webpublisher_campaign_id,
ca.original_number,
ca.target_number,
ca.tracking_number,
ca.status_id,
ca.number_type,
ca.initial_number_type,
ca.number_match,
ca.campaign_type,
ca.referrer_type,
ca.call_recording,
ca.whisper,
ca.advertiser_address_id,
ca.site_id,
ca.location_id,
ca.location_uri,
ca.error_messages,
ca.is_legacy,
ca.is_match,
ca.created_on,
ca.updated_on,
ca.remarks,
ca.auto_delete_status,
ca.call_verify,
ca.call_verify_notification_text,
ca.advertiser_business_name,
ca.source,
ca.datastream_metadata.uuid,
ca.datastream_metadata.source_timestamp
FROM `localiq-dms-analytics-v2-prod.ca_cts.TrackingNumber` ca

UNION ALL

SELECT 
uk.id,
uk.tracking_uuid,
uk.account_id,
uk.country,
uk.campaign_id,
uk.master_campaign_id,
uk.master_advertiser_id,
uk.webpublisher_campaign_id,
uk.original_number,
uk.target_number,
uk.tracking_number,
uk.status_id,
uk.number_type,
uk.initial_number_type,
uk.number_match,
uk.campaign_type,
uk.referrer_type,
uk.call_recording,
uk.whisper,
uk.advertiser_address_id,
uk.site_id,
uk.location_id,
uk.location_uri,
uk.error_messages,
uk.is_legacy,
uk.is_match,
uk.created_on,
uk.updated_on,
uk.remarks,
uk.auto_delete_status,
uk.call_verify,
uk.call_verify_notification_text,
uk.advertiser_business_name,
uk.source,
uk.datastream_metadata.uuid,
uk.datastream_metadata.source_timestamp
FROM `localiq-dms-analytics-v2-prod.uk_cts.TrackingNumber` uk

UNION ALL

SELECT 
au.id,
au.tracking_uuid,
au.account_id,
au.country,
au.campaign_id,
au.master_campaign_id,
au.master_advertiser_id,
au.webpublisher_campaign_id,
au.original_number,
au.target_number,
au.tracking_number,
au.status_id,
au.number_type,
au.initial_number_type,
au.number_match,
au.campaign_type,
au.referrer_type,
au.call_recording,
au.whisper,
au.advertiser_address_id,
au.site_id,
au.location_id,
au.location_uri,
au.error_messages,
au.is_legacy,
au.is_match,
au.created_on,
au.updated_on,
au.remarks,
au.auto_delete_status,
au.call_verify,
au.call_verify_notification_text,
au.advertiser_business_name,
au.source,
au.datastream_metadata.uuid,
au.datastream_metadata.source_timestamp
FROM `localiq-dms-analytics-v2-prod.au_cts.TrackingNumber` au
),
not_found AS (
  SELECT 
    cast(mncr.id as string) AS call_id,
    mncr.status AS call_type,
    mncr.answer_status AS call_status,
    mncr.caller_number AS caller_number,
    -- cast(mncr.tracking_number as string) AS tracking_number,
   mncr.tracking_number AS dialed_number,  
    -- Mapping for target_number
    mncr.termination_number AS target_number,
    mncr.caller_details_name AS caller_name,
    caller_details_city AS caller_city,
    caller_details_state AS caller_state,
    caller_details_zip_code AS caller_zip,
    caller_details_country AS caller_country,
    caller_details_address AS caller_address,
    NULL AS attribution_source,
    TIMESTAMP(mncr.start_time_utc) AS start_time,
    CAST(mncr.call_duration AS STRING) AS duration,
    CAST(CURRENT_TIME() AS STRING) AS created_on,
    CAST(CURRENT_TIME() AS STRING) AS updated_on,
    mncr.conversation_analytics_voice_link AS mailbox_url,
    attribution_details_landing_page_url as call_page_info,
    NULL AS call_page_geo_keyword_id,
    2114 as webpublisher_id
  FROM `dms-data-lake-development.marchex_neighbourly.calls_report` mncr
  LEFT JOIN `localiq-dms-analytics-v2-prod.rl_cts.DniGroup` dg
    ON mncr.group_id = dg.publisher_group_id
  WHERE DATE(start_time_utc) > '2020-01-01'
    AND (dg.idDniGroup IS NULL OR mncr.group_id IS NULL)
)
SELECT 
  n.call_id,
  n.call_type,
  n.call_status,
  n.caller_number,
  n.dialed_number,
  n.target_number,
  n.caller_name,
  n.caller_city,
  n.caller_state,
  n.caller_zip,
  n.caller_country,
  n.caller_address,
  n.attribution_source,
  n.start_time,
  n.duration,
  n.created_on,
  n.updated_on,
  n.mailbox_url,
  n.call_page_info,
  n.call_page_geo_keyword_id,
  n.webpublisher_id
FROM not_found n
LEFT JOIN CTE_UNION cu  
  ON n.target_number = CAST(cu.tracking_number AS STRING)
WHERE cu.tracking_number IS NULL;
