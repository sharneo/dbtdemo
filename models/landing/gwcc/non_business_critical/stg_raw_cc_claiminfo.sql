{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_claiminfo.
                                                claiminfo_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    transient=True,
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "cc_claiminfo"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:LossLocationCode::TEXT AS VARCHAR(5)) AS losslocationcode,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(40)) AS claimnumber,
                TO_TIMESTAMP_TZ(data_payload:NoticeDate::NUMBER/1000) AS noticedate,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:AssignedGroupID::NUMBER AS assignedgroupid,
                data_payload:ExcludedFromArchive::BOOLEAN AS excludedfromarchive,
                data_payload:ArchiveState::NUMBER AS archivestate,
                data_payload:Currency::NUMBER AS currency,
                TO_TIMESTAMP_TZ(data_payload:LossDate::NUMBER/1000) AS lossdate,
                data_payload:ArchiveSchemaInfo::NUMBER AS archiveschemainfo,
                data_payload:DoNotDestroy::BOOLEAN AS donotdestroy,
                data_payload:ArchiveFailureDetailsID::NUMBER AS archivefailuredetailsid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:AdjusterID::NUMBER AS adjusterid,
                data_payload:ClaimID::NUMBER AS claimid,
                TO_TIMESTAMP_TZ(data_payload:PurgeDate::NUMBER/1000) AS purgedate,
                data_payload:ID::NUMBER AS id,
                data_payload:CoverageLineMatchDataInfoValid::BOOLEAN AS coveragelinematchdatainfovalid,
                CAST(data_payload:ExcludeReason::TEXT AS VARCHAR(255)) AS excludereason,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ArchiveFailureID::NUMBER AS archivefailureid,
                CAST(data_payload:RootPublicID::TEXT AS VARCHAR(64)) AS rootpublicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:JurisdictionState::NUMBER AS jurisdictionstate,
                TO_TIMESTAMP_TZ(data_payload:ArchiveDate::NUMBER/1000) AS archivedate,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(40)) AS policynumber,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS STRING) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'AVRO' file_type
            FROM {{ source('gwcc', 'cc_claiminfo') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:losslocationcode::TEXT AS VARCHAR(5)) AS losslocationcode,
                CAST($1:claimnumber::TEXT AS VARCHAR(40)) AS claimnumber,
                $1:noticedate::TIMESTAMP_TZ AS noticedate,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:assignedgroupid::NUMBER AS assignedgroupid,
                $1:excludedfromarchive::BOOLEAN AS excludedfromarchive,
                $1:archivestate::NUMBER AS archivestate,
                $1:currency::NUMBER AS currency,
                $1:lossdate::TIMESTAMP_TZ AS lossdate,
                $1:archiveschemainfo::NUMBER AS archiveschemainfo,
                $1:donotdestroy::BOOLEAN AS donotdestroy,
                $1:archivefailuredetailsid::NUMBER AS archivefailuredetailsid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:adjusterid::NUMBER AS adjusterid,
                $1:claimid::NUMBER AS claimid,
                $1:purgedate::TIMESTAMP_TZ AS purgedate,
                $1:id::NUMBER AS id,
                $1:coveragelinematchdatainfovalid::BOOLEAN AS coveragelinematchdatainfovalid,
                CAST($1:excludereason::TEXT AS VARCHAR(255)) AS excludereason,
                $1:createuserid::NUMBER AS createuserid,
                $1:archivefailureid::NUMBER AS archivefailureid,
                CAST($1:rootpublicid::TEXT AS VARCHAR(64)) AS rootpublicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:jurisdictionstate::NUMBER AS jurisdictionstate,
                $1:archivedate::TIMESTAMP_TZ AS archivedate,
                CAST($1:policynumber::TEXT AS VARCHAR(40)) AS policynumber,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::STRING as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'PARQUET' file_type
            FROM {{ source('gwcc', 'cc_claiminfo') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS claiminfo_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'losslocationcode',
                        'claimnumber',
                        'noticedate',
                        'createtime',
                        'assignedgroupid',
                        'excludedfromarchive',
                        'archivestate',
                        'currency',
                        'lossdate',
                        'archiveschemainfo',
                        'donotdestroy',
                        'archivefailuredetailsid',
                        'updatetime',
                        'adjusterid',
                        'claimid',
                        'purgedate',
                        'coveragelinematchdatainfovalid',
                        'excludereason',
                        'createuserid',
                        'archivefailureid',
                        'rootpublicid',
                        'beanversion',
                        'retired',
                        'updateuserid',
                        'jurisdictionstate',
                        'archivedate',
                        'policynumber'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}