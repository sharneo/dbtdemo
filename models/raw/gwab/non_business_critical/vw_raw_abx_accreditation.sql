
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This Converts Parquet or AVRO Data Loaded in the Variant Column in the RAW DB into Flattend Views
                                                This also creates a HASH_KEY for Incremental Tables for the Curated Layer 
                                                Additional CDA Files are Null in the AVRO but not in CDA .
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    tags=["raw_gwab","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:Notes::TEXT AS VARCHAR(240)) AS notes,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:AccreditationNumber::TEXT AS VARCHAR(20)) AS accreditationnumber,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ProviderType::NUMBER AS providertype,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:AccreditationEndDate::NUMBER/1000) AS accreditationenddate,
                data_payload:ServiceType::NUMBER AS servicetype,
                data_payload:PanelName::NUMBER AS panelname,
                data_payload:BlockedVendor::BOOLEAN AS blockedvendor,
                data_payload:AccreditationType::NUMBER AS accreditationtype,
                CAST(data_payload:LinkID::TEXT AS VARCHAR(64)) AS linkid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                TO_TIMESTAMP_TZ(data_payload:AccreditationStartDate::NUMBER/1000) AS accreditationstartdate,
                TO_TIMESTAMP_TZ(data_payload:ContractEndDate::NUMBER/1000) AS contractenddate,
                data_payload:ID::NUMBER AS id,
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
            FROM {{ source('gwab', 'abx_accreditation') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:notes::TEXT AS VARCHAR(240)) AS notes,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:accreditationnumber::TEXT AS VARCHAR(20)) AS accreditationnumber,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:providertype::NUMBER AS providertype,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:accreditationenddate::TIMESTAMP_TZ AS accreditationenddate,
                $1:servicetype::NUMBER AS servicetype,
                $1:panelname::NUMBER AS panelname,
                $1:blockedvendor::BOOLEAN AS blockedvendor,
                $1:accreditationtype::NUMBER AS accreditationtype,
                CAST($1:linkid::TEXT AS VARCHAR(64)) AS linkid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:accreditationstartdate::TIMESTAMP_TZ AS accreditationstartdate,
                $1:contractenddate::TIMESTAMP_TZ AS contractenddate,
                $1:id::NUMBER AS id,
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
            FROM {{ source('gwab', 'abx_accreditation') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),
{#-
    Driving CTE Over 
    Transformed CTE is To Create the HASH_KEY Based on the Right Combination
-#}   
cte_transformed AS (
    SELECT
        *,
        CASE
             WHEN file_type = 'AVRO' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'loadcommandid',
                        'notes',
                        'createuserid',
                        'accreditationnumber',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'providertype',
                        'updateuserid',
                        'accreditationenddate',
                        'servicetype',
                        'panelname',
                        'blockedvendor',
                        'accreditationtype',
                        'linkid',
                        'updatetime',
                        'accreditationstartdate',
                        'contractenddate',
                        'id',
                        'contactid',
                        'contractstartdate',
                        'peakbodynumber',
                        'peakbodyname'
                        ]) }}
            WHEN file_type = 'PARQUET' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'id',
                        'gwcbi_seqval'
                        ]) }}
        END AS hash_key    
    FROM cte_source_data
)
SELECT * FROM cte_transformed
        