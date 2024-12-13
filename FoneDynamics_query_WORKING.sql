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
not_found as (
SELECT 
cast(fdcr.CallSid as string) AS call_id,
NULL AS call_type,
fdcr.Disposition AS call_status,
fdcr.From AS caller_number,
CAST(
    CASE 
      when fdcr.To is not null then REGEXP_REPLACE(cast(`To` as string), r'^1|^55|^81|^44|^52|^61|^64|^65|^31|^49|^43|^32|^353|^56|^54','') 
    END AS STRING
) AS dialed_number,

CAST(
    CASE 
      when fdcr.Endpoint is not null then REGEXP_REPLACE(cast(`Endpoint` as string), r'^1|^55|^81|^44|^52|^61|^64|^65|^31|^49|^43|^32|^353|^56|^54','') 
    END AS STRING
) AS target_number,
NULL as caller_name,
fdcr.From_City as caller_city,
fdcr.From_Region as caller_state,
NULL as caller_zip,
fdcr.From_CountryCode as caller_country,
NULL as caller_address,
TIMESTAMP(fdcr.Date_Start) AS start_time,
fdcr.duration AS duration,
CURRENT_TIME() AS created_on,
CURRENT_TIME() AS updated_on,
fdcr.Recording_Uri as mailbox_url,
NULL AS call_page_info,
NULL as call_page_geo_keyword_id,
NULL as attribution_source,
fdcr.Attribution_Medium as wpc_id,
fdcr.Attribution_Campaign as MCID,
CASE 
    WHEN REGEXP_CONTAINS(attribution_source, r'^[A-Z]{3}_[0-9]+$') THEN 
      SPLIT(attribution_source, '_')[SAFE_OFFSET(1)]
END AS MAID,
dg.publisher_group_id as publisher_group_id
FROM `dms-data-lake-development.fonedynamics.calls_report` fdcr
LEFT JOIN
 `localiq-dms-analytics-v2-prod.rl_cts.DniGroup` dg
ON CAST(REGEXP_REPLACE(fdcr.GroupName , '[^0-9]', '') as BIGINT) = dg.publisher_group_id
WHERE DATE(fdcr.start_time) > '2020-01-01'
and (dg.idDniGroup is null or fdcr.GroupName is null)
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
n.start_time,
n.duration,
n.created_on,
n.updated_on,
n.mailbox_url,
n.call_page_info,
n.call_page_geo_keyword_id,
n.attribution_source,
n.wpc_id,
n.MCID,
n.MAID,
n.publisher_group_id,
1023 as web_publisher_id
FROM not_found n
LEFT JOIN CTE_UNION cu
ON n.target_number = CAST(cu.publisher_group_id AS STRING)
WHERE cu.publisher_group_id IS NULL;
