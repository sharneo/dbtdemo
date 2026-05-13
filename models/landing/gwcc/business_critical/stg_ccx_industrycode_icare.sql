{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_industrycode_icare.
                                                industrycode_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "ccx_industrycode_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:IndustryCode::TEXT AS VARCHAR(4)) AS industrycode,
                CAST(data_payload:GroupCode::TEXT AS VARCHAR(255)) AS groupcode,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:IndustryDesc::TEXT AS VARCHAR(255)) AS industrydesc,
                CAST(data_payload:GroupDesc::TEXT AS VARCHAR(255)) AS groupdesc,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Version::TEXT AS VARCHAR(255)) AS version,
                CAST(data_payload:DivisionCode::TEXT AS VARCHAR(255)) AS divisioncode,
                CAST(data_payload:SubdivisionCode::TEXT AS VARCHAR(255)) AS subdivisioncode,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                CAST(data_payload:DivisionDesc::TEXT AS VARCHAR(255)) AS divisiondesc,
                CAST(data_payload:SubdivisionDesc::TEXT AS VARCHAR(255)) AS subdivisiondesc,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_industrycode_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:industrycode::TEXT AS VARCHAR(4)) AS industrycode,
                CAST($1:groupcode::TEXT AS VARCHAR(255)) AS groupcode,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:industrydesc::TEXT AS VARCHAR(255)) AS industrydesc,
                CAST($1:groupdesc::TEXT AS VARCHAR(255)) AS groupdesc,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:version::TEXT AS VARCHAR(255)) AS version,
                CAST($1:divisioncode::TEXT AS VARCHAR(255)) AS divisioncode,
                CAST($1:subdivisioncode::TEXT AS VARCHAR(255)) AS subdivisioncode,
                $1:id::NUMBER AS id,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                CAST($1:divisiondesc::TEXT AS VARCHAR(255)) AS divisiondesc,
                CAST($1:subdivisiondesc::TEXT AS VARCHAR(255)) AS subdivisiondesc,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_industrycode_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS industrycode_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'beanversion',
                        'createtime',
                        'retired',
                        'industrycode',
                        'groupcode',
                        'updateuserid',
                        'industrydesc',
                        'groupdesc',
                        'effectivedate',
                        'updatetime',
                        'version',
                        'divisioncode',
                        'subdivisioncode',
                        'expirationdate',
                        'divisiondesc',
                        'subdivisiondesc'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}