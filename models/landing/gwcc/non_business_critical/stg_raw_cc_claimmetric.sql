{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_claimmetric.
                                                claimmetric_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "cc_claimmetric"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:DecimalValue AS NUMBER(20,4)) AS decimalvalue,
                data_payload:ActivitySkipped::BOOLEAN AS activityskipped,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:ReachRedTime::NUMBER/1000) AS reachredtime,
                data_payload:PercentValue::NUMBER AS percentvalue,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                TO_TIMESTAMP_TZ(data_payload:NextOverdueTime::NUMBER/1000) AS nextoverduetime,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:MetricLimitDenormID::NUMBER AS metriclimitdenormid,
                CAST(data_payload:TotalReserveChange AS NUMBER(20,4)) AS totalreservechange,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:ClaimMetricCategory::NUMBER AS claimmetriccategory,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:StartTime::NUMBER/1000) AS starttime,
                data_payload:Skipped::BOOLEAN AS skipped,
                CAST(data_payload:MoneyValue AS NUMBER(18,2)) AS moneyvalue,
                data_payload:IntegerValue::NUMBER AS integervalue,
                TO_TIMESTAMP_TZ(data_payload:ReachYellowTime::NUMBER/1000) AS reachyellowtime,
                data_payload:IsOpen::BOOLEAN AS isopen,
                CAST(data_payload:InitialReserve AS NUMBER(20,4)) AS initialreserve,
                data_payload:Subtype::NUMBER AS subtype,
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
            FROM {{ source('gwcc', 'cc_claimmetric') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:decimalvalue AS NUMBER(20,4)) AS decimalvalue,
                $1:activityskipped::BOOLEAN AS activityskipped,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:reachredtime::TIMESTAMP_TZ AS reachredtime,
                $1:percentvalue::NUMBER AS percentvalue,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:nextoverduetime::TIMESTAMP_TZ AS nextoverduetime,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                $1:metriclimitdenormid::NUMBER AS metriclimitdenormid,
                CAST($1:totalreservechange AS NUMBER(20,4)) AS totalreservechange,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:claimmetriccategory::NUMBER AS claimmetriccategory,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:starttime::TIMESTAMP_TZ AS starttime,
                $1:skipped::BOOLEAN AS skipped,
                CAST($1:moneyvalue AS NUMBER(18,2)) AS moneyvalue,
                $1:integervalue::NUMBER AS integervalue,
                $1:reachyellowtime::TIMESTAMP_TZ AS reachyellowtime,
                $1:isopen::BOOLEAN AS isopen,
                CAST($1:initialreserve AS NUMBER(20,4)) AS initialreserve,
                $1:subtype::NUMBER AS subtype,
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
            FROM {{ source('gwcc', 'cc_claimmetric') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS claimmetric_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'decimalvalue',
                        'activityskipped',
                        'publicid',
                        'createtime',
                        'reachredtime',
                        'percentvalue',
                        'updatetime',
                        'claimid',
                        'nextoverduetime',
                        'createuserid',
                        'metriclimitdenormid',
                        'totalreservechange',
                        'beanversion',
                        'archivepartition',
                        'claimmetriccategory',
                        'updateuserid',
                        'starttime',
                        'skipped',
                        'moneyvalue',
                        'integervalue',
                        'reachyellowtime',
                        'isopen',
                        'initialreserve',
                        'subtype'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}