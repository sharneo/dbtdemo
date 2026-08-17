{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_claimmetriclimit.
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
                CAST(data_payload:MoneyRedValue AS NUMBER(18,2)) AS moneyredvalue,
                CAST(data_payload:MoneyTargetValue AS NUMBER(18,2)) AS moneytargetvalue,
                data_payload:CreatedGeneration::NUMBER AS createdgeneration,
                data_payload:RetiredGeneration::NUMBER AS retiredgeneration,
                data_payload:IntegerTargetValue::NUMBER AS integertargetvalue,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:PercentRedValue::NUMBER AS percentredvalue,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:AscendingLimitOrder::BOOLEAN AS ascendinglimitorder,
                CAST(data_payload:DecimalYellowValue AS NUMBER(20,4)) AS decimalyellowvalue,
                CAST(data_payload:DecimalTargetValue AS NUMBER(20,4)) AS decimaltargetvalue,
                data_payload:PolicyTypeMetricLimitsID::NUMBER AS policytypemetriclimitsid,
                CAST(data_payload:DecimalRedValue AS NUMBER(20,4)) AS decimalredvalue,
                data_payload:Currency::NUMBER AS currency,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                TO_TIMESTAMP_TZ(data_payload:RetiredDate::NUMBER/1000) AS retireddate,
                data_payload:PercentYellowValue::NUMBER AS percentyellowvalue,
                data_payload:ID::NUMBER AS id,
                data_payload:PercentTargetValue::NUMBER AS percenttargetvalue,
                data_payload:ClaimTier::NUMBER AS claimtier,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ClaimMetricType::NUMBER AS claimmetrictype,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ClaimMetricCategory::NUMBER AS claimmetriccategory,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:MetricUnit::NUMBER AS metricunit,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:MoneyYellowValue AS NUMBER(18,2)) AS moneyyellowvalue,
                data_payload:IntegerRedValue::NUMBER AS integerredvalue,
                data_payload:IntegerYellowValue::NUMBER AS integeryellowvalue,
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
            FROM {{ source('gwcc', 'cc_claimmetriclimit') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:moneyredvalue AS NUMBER(18,2)) AS moneyredvalue,
                CAST($1:moneytargetvalue AS NUMBER(18,2)) AS moneytargetvalue,
                $1:createdgeneration::NUMBER AS createdgeneration,
                $1:retiredgeneration::NUMBER AS retiredgeneration,
                $1:integertargetvalue::NUMBER AS integertargetvalue,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:percentredvalue::NUMBER AS percentredvalue,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:ascendinglimitorder::BOOLEAN AS ascendinglimitorder,
                CAST($1:decimalyellowvalue AS NUMBER(20,4)) AS decimalyellowvalue,
                CAST($1:decimaltargetvalue AS NUMBER(20,4)) AS decimaltargetvalue,
                $1:policytypemetriclimitsid::NUMBER AS policytypemetriclimitsid,
                CAST($1:decimalredvalue AS NUMBER(20,4)) AS decimalredvalue,
                $1:currency::NUMBER AS currency,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:retireddate::TIMESTAMP_TZ AS retireddate,
                $1:percentyellowvalue::NUMBER AS percentyellowvalue,
                $1:id::NUMBER AS id,
                $1:percenttargetvalue::NUMBER AS percenttargetvalue,
                $1:claimtier::NUMBER AS claimtier,
                $1:createuserid::NUMBER AS createuserid,
                $1:claimmetrictype::NUMBER AS claimmetrictype,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:claimmetriccategory::NUMBER AS claimmetriccategory,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:metricunit::NUMBER AS metricunit,
                $1:subtype::NUMBER AS subtype,
                CAST($1:moneyyellowvalue AS NUMBER(18,2)) AS moneyyellowvalue,
                $1:integerredvalue::NUMBER AS integerredvalue,
                $1:integeryellowvalue::NUMBER AS integeryellowvalue,
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
            FROM {{ source('gwcc', 'cc_claimmetriclimit') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS claimmetriclimit_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'moneyredvalue',
                        'moneytargetvalue',
                        'createdgeneration',
                        'retiredgeneration',
                        'integertargetvalue',
                        'publicid',
                        'percentredvalue',
                        'createtime',
                        'ascendinglimitorder',
                        'decimalyellowvalue',
                        'decimaltargetvalue',
                        'policytypemetriclimitsid',
                        'decimalredvalue',
                        'currency',
                        'updatetime',
                        'retireddate',
                        'percentyellowvalue',
                        'percenttargetvalue',
                        'claimtier',
                        'createuserid',
                        'claimmetrictype',
                        'beanversion',
                        'retired',
                        'claimmetriccategory',
                        'updateuserid',
                        'metricunit',
                        'subtype',
                        'moneyyellowvalue',
                        'integerredvalue',
                        'integeryellowvalue'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date
