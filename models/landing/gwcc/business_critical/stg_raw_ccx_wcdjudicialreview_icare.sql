{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_wcdjudicialreview_icare.
                                                wcdjudicialreview_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "ccx_wcdjudicialreview_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                TO_TIMESTAMP_TZ(data_payload:DateApplicationReceived::NUMBER/1000) AS dateapplicationreceived,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ReviewDetailsID::NUMBER AS reviewdetailsid,
                TO_TIMESTAMP_TZ(data_payload:DateNotified::NUMBER/1000) AS datenotified,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:ApplicationLodgedBy::NUMBER AS applicationlodgedby,
                data_payload:SolicitorID::NUMBER AS solicitorid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:NewWCDRequired::BOOLEAN AS newwcdrequired,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:DateCompleted::NUMBER/1000) AS datecompleted,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:FurtherInfoSubmitted::BOOLEAN AS furtherinfosubmitted,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:OutcomeDescription::TEXT AS VARCHAR(16777216)) AS outcomedescription,
                data_payload:Section54StillActive::BOOLEAN AS section54stillactive,
                TO_TIMESTAMP_TZ(data_payload:ResponseDate::NUMBER/1000) AS responsedate,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:ReviewRequestedWithin30Days::BOOLEAN AS reviewrequestedwithin30days,
                data_payload:FollowUpFlag::BOOLEAN AS followupflag,
                data_payload:StayApplicable::BOOLEAN AS stayapplicable,
                CAST(data_payload:CaseNumber::TEXT AS VARCHAR(255)) AS casenumber,
                data_payload:ReviewOutcome::NUMBER AS reviewoutcome,
                TO_TIMESTAMP_TZ(data_payload:StayExpiryDate::NUMBER/1000) AS stayexpirydate,
                data_payload:SectionS80StillActive::BOOLEAN AS sections80stillactive,
                CAST(data_payload:PIAWE AS NUMBER(18,2)) AS piawe,
                data_payload:WithinS80NoticePeriod::BOOLEAN AS withins80noticeperiod,
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
            FROM {{ source('gwcc', 'ccx_wcdjudicialreview_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:dateapplicationreceived::TIMESTAMP_TZ AS dateapplicationreceived,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:reviewdetailsid::NUMBER AS reviewdetailsid,
                $1:datenotified::TIMESTAMP_TZ AS datenotified,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:applicationlodgedby::NUMBER AS applicationlodgedby,
                $1:solicitorid::NUMBER AS solicitorid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:newwcdrequired::BOOLEAN AS newwcdrequired,
                $1:id::NUMBER AS id,
                $1:datecompleted::TIMESTAMP_TZ AS datecompleted,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:furtherinfosubmitted::BOOLEAN AS furtherinfosubmitted,
                $1:retired::NUMBER AS retired,
                CAST($1:outcomedescription::TEXT AS VARCHAR(16777216)) AS outcomedescription,
                $1:section54stillactive::BOOLEAN AS section54stillactive,
                $1:responsedate::TIMESTAMP_TZ AS responsedate,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:reviewrequestedwithin30days::BOOLEAN AS reviewrequestedwithin30days,
                $1:followupflag::BOOLEAN AS followupflag,
                $1:stayapplicable::BOOLEAN AS stayapplicable,
                CAST($1:casenumber::TEXT AS VARCHAR(255)) AS casenumber,
                $1:reviewoutcome::NUMBER AS reviewoutcome,
                $1:stayexpirydate::TIMESTAMP_TZ AS stayexpirydate,
                $1:sections80stillactive::BOOLEAN AS sections80stillactive,
                CAST($1:piawe AS NUMBER(18,2)) AS piawe,
                $1:withins80noticeperiod::BOOLEAN AS withins80noticeperiod,
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
            FROM {{ source('gwcc', 'ccx_wcdjudicialreview_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS wcdjudicialreview_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'dateapplicationreceived',
                        'publicid',
                        'reviewdetailsid',
                        'datenotified',
                        'createtime',
                        'applicationlodgedby',
                        'solicitorid',
                        'updatetime',
                        'newwcdrequired',
                        'datecompleted',
                        'createuserid',
                        'beanversion',
                        'archivepartition',
                        'furtherinfosubmitted',
                        'retired',
                        'outcomedescription',
                        'section54stillactive',
                        'responsedate',
                        'updateuserid',
                        'reviewrequestedwithin30days',
                        'followupflag',
                        'stayapplicable',
                        'casenumber',
                        'reviewoutcome',
                        'stayexpirydate',
                        'sections80stillactive',
                        'piawe',
                        'withins80noticeperiod'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}