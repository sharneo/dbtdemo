{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_csponpolicy_ext.
                                                csponpolicy_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "non_business_critical", "pcx_csponpolicy_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:CSPEndDate::NUMBER/1000) AS cspenddate,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:CSPName::TEXT AS VARCHAR(40)) AS cspname,
                CAST(data_payload:CSPCode::TEXT AS VARCHAR(40)) AS cspcode,
                data_payload:PolicyID::NUMBER AS policyid,
                TO_TIMESTAMP_TZ(data_payload:AgencySelectedDate::NUMBER/1000) AS agencyselecteddate,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:CSPStartDate::NUMBER/1000) AS cspstartdate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:IsRemoved::BOOLEAN AS isremoved,
                data_payload:AgencyChosen::BOOLEAN AS agencychosen,
                data_payload:CSPType::NUMBER AS csptype,
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
                'AVRO' as file_type,
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_csponpolicy_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:cspenddate::TIMESTAMP_TZ AS cspenddate,
                $1:retired::NUMBER AS retired,
                CAST($1:cspname::TEXT AS VARCHAR(40)) AS cspname,
                CAST($1:cspcode::TEXT AS VARCHAR(40)) AS cspcode,
                $1:policyid::NUMBER AS policyid,
                $1:agencyselecteddate::TIMESTAMP_TZ AS agencyselecteddate,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:cspstartdate::TIMESTAMP_TZ AS cspstartdate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:isremoved::BOOLEAN AS isremoved,
                $1:agencychosen::BOOLEAN AS agencychosen,
                $1:csptype::NUMBER AS csptype,
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
                'PARQUET' as file_type,
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_csponpolicy_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS csponpolicy_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'cspenddate',
                        'retired',
                        'cspname',
                        'cspcode',
                        'policyid',
                        'agencyselecteddate',
                        'updateuserid',
                        'cspstartdate',
                        'updatetime',
                        'isremoved',
                        'agencychosen',
                        'csptype'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}