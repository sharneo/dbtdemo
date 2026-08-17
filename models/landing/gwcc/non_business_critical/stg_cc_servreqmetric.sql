{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_servreqmetric.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwcc", "claim_centre", "non_business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:DecimalValue AS NUMBER(20,4)) AS decimalvalue,
                TO_TIMESTAMP_TZ(data_payload:WaitingStartTime::NUMBER/1000) AS waitingstarttime,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:ReachRedTime::NUMBER/1000) AS reachredtime,
                CAST(data_payload:DecimalYellowValue AS NUMBER(20,4)) AS decimalyellowvalue,
                data_payload:PercentValue::NUMBER AS percentvalue,
                CAST(data_payload:DecimalTargetValue AS NUMBER(20,4)) AS decimaltargetvalue,
                data_payload:TimeSpentWorking::NUMBER AS timespentworking,
                CAST(data_payload:DecimalRedValue AS NUMBER(20,4)) AS decimalredvalue,
                data_payload:ServiceRequestID::NUMBER AS servicerequestid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:DaysDifferentFromExpected::NUMBER AS daysdifferentfromexpected,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:StartTime::NUMBER/1000) AS starttime,
                data_payload:Skipped::BOOLEAN AS skipped,
                CAST(data_payload:MoneyValue AS NUMBER(18,2)) AS moneyvalue,
                data_payload:Escalated::BOOLEAN AS escalated,
                data_payload:IntegerValue::NUMBER AS integervalue,
                TO_TIMESTAMP_TZ(data_payload:ReachYellowTime::NUMBER/1000) AS reachyellowtime,
                data_payload:MetricUnit::NUMBER AS metricunit,
                data_payload:IsOpen::BOOLEAN AS isopen,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS VARCHAR(300)) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'AVRO' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_servreqmetric') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:decimalvalue AS NUMBER(20,4)) AS decimalvalue,
                $1:waitingstarttime::TIMESTAMP_TZ AS waitingstarttime,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:reachredtime::TIMESTAMP_TZ AS reachredtime,
                CAST($1:decimalyellowvalue AS NUMBER(20,4)) AS decimalyellowvalue,
                $1:percentvalue::NUMBER AS percentvalue,
                CAST($1:decimaltargetvalue AS NUMBER(20,4)) AS decimaltargetvalue,
                $1:timespentworking::NUMBER AS timespentworking,
                CAST($1:decimalredvalue AS NUMBER(20,4)) AS decimalredvalue,
                $1:servicerequestid::NUMBER AS servicerequestid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                $1:daysdifferentfromexpected::NUMBER AS daysdifferentfromexpected,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:starttime::TIMESTAMP_TZ AS starttime,
                $1:skipped::BOOLEAN AS skipped,
                CAST($1:moneyvalue AS NUMBER(18,2)) AS moneyvalue,
                $1:escalated::BOOLEAN AS escalated,
                $1:integervalue::NUMBER AS integervalue,
                $1:reachyellowtime::TIMESTAMP_TZ AS reachyellowtime,
                $1:metricunit::NUMBER AS metricunit,
                $1:isopen::BOOLEAN AS isopen,
                $1:subtype::NUMBER AS subtype,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::VARCHAR(300) as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'PARQUET' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_servreqmetric') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS servreqmetric_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'decimalvalue',
                        'waitingstarttime',
                        'publicid',
                        'createtime',
                        'reachredtime',
                        'decimalyellowvalue',
                        'percentvalue',
                        'decimaltargetvalue',
                        'timespentworking',
                        'decimalredvalue',
                        'servicerequestid',
                        'updatetime',
                        'createuserid',
                        'daysdifferentfromexpected',
                        'archivepartition',
                        'beanversion',
                        'updateuserid',
                        'starttime',
                        'skipped',
                        'moneyvalue',
                        'escalated',
                        'integervalue',
                        'reachyellowtime',
                        'metricunit',
                        'isopen',
                        'subtype'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date
