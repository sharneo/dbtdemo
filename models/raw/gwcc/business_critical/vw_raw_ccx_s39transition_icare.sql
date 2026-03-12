
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
    tags=["raw_gwcc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:ExposureID::NUMBER AS exposureid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:WPIPercentage::NUMBER AS wpipercentage,
                data_payload:InterventionLevel::NUMBER AS interventionlevel,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Outcome::NUMBER AS outcome,
                data_payload:Status::NUMBER AS status,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:IMEDocumentID::NUMBER AS imedocumentid,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:DateofMAC::NUMBER/1000) AS dateofmac,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                CAST(data_payload:OutcomeReason::TEXT AS VARCHAR(128)) AS outcomereason,
                data_payload:NoticePeriod::NUMBER AS noticeperiod,
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
            FROM {{ source('gwcc', 'ccx_s39transition_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:exposureid::NUMBER AS exposureid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:wpipercentage::NUMBER AS wpipercentage,
                $1:interventionlevel::NUMBER AS interventionlevel,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:outcome::NUMBER AS outcome,
                $1:status::NUMBER AS status,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:imedocumentid::NUMBER AS imedocumentid,
                $1:id::NUMBER AS id,
                $1:dateofmac::TIMESTAMP_TZ AS dateofmac,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                CAST($1:outcomereason::TEXT AS VARCHAR(128)) AS outcomereason,
                $1:noticeperiod::NUMBER AS noticeperiod,
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
            FROM {{ source('gwcc', 'ccx_s39transition_icare') }}
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
                        'exposureid',
                        'createuserid',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'createtime',
                        'wpipercentage',
                        'interventionlevel',
                        'updateuserid',
                        'outcome',
                        'status',
                        'updatetime',
                        'imedocumentid',
                        'id',
                        'dateofmac',
                        'effectivedate',
                        'outcomereason',
                        'noticeperiod'
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
        