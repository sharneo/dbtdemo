{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_servicerequestmetriclimit.
                                                servicerequestmetriclimit_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "cc_servicerequestmetriclimit"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ServiceRequestMetricType::NUMBER AS servicerequestmetrictype,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:DecimalYellowValue AS NUMBER(20,4)) AS decimalyellowvalue,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:DecimalTargetValue AS NUMBER(20,4)) AS decimaltargetvalue,
                data_payload:Currency::NUMBER AS currency,
                CAST(data_payload:DecimalRedValue AS NUMBER(20,4)) AS decimalredvalue,
                data_payload:CategoryServiceID::NUMBER AS categoryserviceid,
                data_payload:MetricUnit::NUMBER AS metricunit,
                data_payload:LimitType::NUMBER AS limittype,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:CustomerServiceTier::NUMBER AS customerservicetier,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                data_payload:ServiceRequestTier::NUMBER AS servicerequesttier,
                data_payload:SpecialistServiceID::NUMBER AS specialistserviceid,
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
            FROM {{ source('gwcc', 'cc_servicerequestmetriclimit') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:servicerequestmetrictype::NUMBER AS servicerequestmetrictype,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:decimalyellowvalue AS NUMBER(20,4)) AS decimalyellowvalue,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:decimaltargetvalue AS NUMBER(20,4)) AS decimaltargetvalue,
                $1:currency::NUMBER AS currency,
                CAST($1:decimalredvalue AS NUMBER(20,4)) AS decimalredvalue,
                $1:categoryserviceid::NUMBER AS categoryserviceid,
                $1:metricunit::NUMBER AS metricunit,
                $1:limittype::NUMBER AS limittype,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:customerservicetier::NUMBER AS customerservicetier,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                $1:servicerequesttier::NUMBER AS servicerequesttier,
                $1:specialistserviceid::NUMBER AS specialistserviceid,
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
            FROM {{ source('gwcc', 'cc_servicerequestmetriclimit') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS servicerequestmetriclimit_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'createuserid',
                        'publicid',
                        'servicerequestmetrictype',
                        'beanversion',
                        'createtime',
                        'decimalyellowvalue',
                        'updateuserid',
                        'decimaltargetvalue',
                        'currency',
                        'decimalredvalue',
                        'categoryserviceid',
                        'metricunit',
                        'limittype',
                        'updatetime',
                        'customerservicetier',
                        'subtype',
                        'servicerequesttier',
                        'specialistserviceid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}