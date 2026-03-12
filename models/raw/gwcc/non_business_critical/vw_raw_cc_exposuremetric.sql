
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
                CAST(data_payload:DecimalValue AS NUMBER(20,4)) AS decimalvalue,
                data_payload:ActivitySkipped::BOOLEAN AS activityskipped,
                data_payload:ExposureID::NUMBER AS exposureid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:MetricLimitDenormID::NUMBER AS metriclimitdenormid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:ReachRedTime::NUMBER/1000) AS reachredtime,
                data_payload:PercentValue::NUMBER AS percentvalue,
                TO_TIMESTAMP_TZ(data_payload:StartTime::NUMBER/1000) AS starttime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Skipped::BOOLEAN AS skipped,
                CAST(data_payload:MoneyValue AS NUMBER(18,2)) AS moneyvalue,
                data_payload:IntegerValue::NUMBER AS integervalue,
                data_payload:IsOpen::BOOLEAN AS isopen,
                TO_TIMESTAMP_TZ(data_payload:ReachYellowTime::NUMBER/1000) AS reachyellowtime,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
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
            FROM {{ source('gwcc', 'cc_exposuremetric') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:decimalvalue AS NUMBER(20,4)) AS decimalvalue,
                $1:activityskipped::BOOLEAN AS activityskipped,
                $1:exposureid::NUMBER AS exposureid,
                $1:createuserid::NUMBER AS createuserid,
                $1:metriclimitdenormid::NUMBER AS metriclimitdenormid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:reachredtime::TIMESTAMP_TZ AS reachredtime,
                $1:percentvalue::NUMBER AS percentvalue,
                $1:starttime::TIMESTAMP_TZ AS starttime,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:skipped::BOOLEAN AS skipped,
                CAST($1:moneyvalue AS NUMBER(18,2)) AS moneyvalue,
                $1:integervalue::NUMBER AS integervalue,
                $1:isopen::BOOLEAN AS isopen,
                $1:reachyellowtime::TIMESTAMP_TZ AS reachyellowtime,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
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
            FROM {{ source('gwcc', 'cc_exposuremetric') }}
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
                                'decimalvalue',
                        'activityskipped',
                        'exposureid',
                        'createuserid',
                        'metriclimitdenormid',
                        'publicid',
                        'archivepartition',
                        'beanversion',
                        'createtime',
                        'reachredtime',
                        'percentvalue',
                        'starttime',
                        'updateuserid',
                        'skipped',
                        'moneyvalue',
                        'integervalue',
                        'isopen',
                        'reachyellowtime',
                        'updatetime',
                        'subtype',
                        'id'
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
        