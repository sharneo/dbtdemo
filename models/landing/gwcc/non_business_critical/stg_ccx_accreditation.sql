{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_accreditation.
                                                accreditation_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_accreditation"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:Notes::TEXT AS VARCHAR(240)) AS notes,
                CAST(data_payload:AccreditationNumber::TEXT AS VARCHAR(20)) AS accreditationnumber,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:PanelName::NUMBER AS panelname,
                TO_TIMESTAMP_TZ(data_payload:AccreditationEndDate::NUMBER/1000) AS accreditationenddate,
                CAST(data_payload:AddressBookUID::TEXT AS VARCHAR(64)) AS addressbookuid,
                TO_TIMESTAMP_TZ(data_payload:AccreditationStartDate::NUMBER/1000) AS accreditationstartdate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:ExternalLinkID::TEXT AS VARCHAR(64)) AS externallinkid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ProviderType::NUMBER AS providertype,
                data_payload:ServiceType::NUMBER AS servicetype,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:AccreditationType::NUMBER AS accreditationtype,
                data_payload:BlockedVendor::BOOLEAN AS blockedvendor,
                TO_TIMESTAMP_TZ(data_payload:ContractEndDate::NUMBER/1000) AS contractenddate,
                data_payload:ContactID::NUMBER AS contactid,
                TO_TIMESTAMP_TZ(data_payload:ContractStartDate::NUMBER/1000) AS contractstartdate,
                CAST(data_payload:PeakBodyNumber::TEXT AS VARCHAR(20)) AS peakbodynumber,
                CAST(data_payload:PeakBodyName::TEXT AS VARCHAR(80)) AS peakbodyname,
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
            FROM {{ source('gwcc', 'ccx_accreditation') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:notes::TEXT AS VARCHAR(240)) AS notes,
                CAST($1:accreditationnumber::TEXT AS VARCHAR(20)) AS accreditationnumber,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:panelname::NUMBER AS panelname,
                $1:accreditationenddate::TIMESTAMP_TZ AS accreditationenddate,
                CAST($1:addressbookuid::TEXT AS VARCHAR(64)) AS addressbookuid,
                $1:accreditationstartdate::TIMESTAMP_TZ AS accreditationstartdate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                CAST($1:externallinkid::TEXT AS VARCHAR(64)) AS externallinkid,
                $1:createuserid::NUMBER AS createuserid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:providertype::NUMBER AS providertype,
                $1:servicetype::NUMBER AS servicetype,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:accreditationtype::NUMBER AS accreditationtype,
                $1:blockedvendor::BOOLEAN AS blockedvendor,
                $1:contractenddate::TIMESTAMP_TZ AS contractenddate,
                $1:contactid::NUMBER AS contactid,
                $1:contractstartdate::TIMESTAMP_TZ AS contractstartdate,
                CAST($1:peakbodynumber::TEXT AS VARCHAR(20)) AS peakbodynumber,
                CAST($1:peakbodyname::TEXT AS VARCHAR(80)) AS peakbodyname,
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
            FROM {{ source('gwcc', 'ccx_accreditation') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS accreditation_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'notes',
                        'accreditationnumber',
                        'publicid',
                        'createtime',
                        'panelname',
                        'accreditationenddate',
                        'addressbookuid',
                        'accreditationstartdate',
                        'updatetime',
                        'externallinkid',
                        'createuserid',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'providertype',
                        'servicetype',
                        'updateuserid',
                        'accreditationtype',
                        'blockedvendor',
                        'contractenddate',
                        'contactid',
                        'contractstartdate',
                        'peakbodynumber',
                        'peakbodyname'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}