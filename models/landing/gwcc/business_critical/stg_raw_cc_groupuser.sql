{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_groupuser.
                                                groupuser_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "cc_groupuser"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:TextField1::TEXT AS VARCHAR(255)) AS textfield1,
                CAST(data_payload:TextField2::TEXT AS VARCHAR(255)) AS textfield2,
                CAST(data_payload:TextField3::TEXT AS VARCHAR(255)) AS textfield3,
                data_payload:UserID::NUMBER AS userid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Manager::BOOLEAN AS manager,
                data_payload:LoadFactorType::NUMBER AS loadfactortype,
                data_payload:LoadFactor::NUMBER AS loadfactor,
                data_payload:Member::BOOLEAN AS member,
                data_payload:GroupID::NUMBER AS groupid,
                data_payload:ID::NUMBER AS id,
                data_payload:GroupUserWorkloadID::NUMBER AS groupuserworkloadid,
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
            FROM {{ source('gwcc', 'cc_groupuser') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:textfield1::TEXT AS VARCHAR(255)) AS textfield1,
                CAST($1:textfield2::TEXT AS VARCHAR(255)) AS textfield2,
                CAST($1:textfield3::TEXT AS VARCHAR(255)) AS textfield3,
                $1:userid::NUMBER AS userid,
                $1:beanversion::NUMBER AS beanversion,
                $1:manager::BOOLEAN AS manager,
                $1:loadfactortype::NUMBER AS loadfactortype,
                $1:loadfactor::NUMBER AS loadfactor,
                $1:member::BOOLEAN AS member,
                $1:groupid::NUMBER AS groupid,
                $1:id::NUMBER AS id,
                $1:groupuserworkloadid::NUMBER AS groupuserworkloadid,
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
            FROM {{ source('gwcc', 'cc_groupuser') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS groupuser_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'textfield1',
                        'textfield2',
                        'textfield3',
                        'userid',
                        'beanversion',
                        'manager',
                        'loadfactortype',
                        'loadfactor',
                        'member',
                        'groupid',
                        'groupuserworkloadid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}