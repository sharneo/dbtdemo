{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_agencycostcentre_ext.
                                                agencycostcentre_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "non_business_critical", "pcx_agencycostcentre_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:Level9Name::TEXT AS VARCHAR(255)) AS level9name,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:AccountID::NUMBER AS accountid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:CostCentreCode::TEXT AS VARCHAR(255)) AS costcentrecode,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Level2Code::TEXT AS VARCHAR(255)) AS level2code,
                CAST(data_payload:Level3Code::TEXT AS VARCHAR(255)) AS level3code,
                CAST(data_payload:Level4Code::TEXT AS VARCHAR(255)) AS level4code,
                CAST(data_payload:Level5Code::TEXT AS VARCHAR(255)) AS level5code,
                CAST(data_payload:Level6Code::TEXT AS VARCHAR(255)) AS level6code,
                CAST(data_payload:Level7Code::TEXT AS VARCHAR(255)) AS level7code,
                CAST(data_payload:Level8Code::TEXT AS VARCHAR(255)) AS level8code,
                CAST(data_payload:Level9Code::TEXT AS VARCHAR(255)) AS level9code,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:CostCentreName::TEXT AS VARCHAR(255)) AS costcentrename,
                CAST(data_payload:Level2Name::TEXT AS VARCHAR(255)) AS level2name,
                CAST(data_payload:Level3Name::TEXT AS VARCHAR(255)) AS level3name,
                CAST(data_payload:Level4Name::TEXT AS VARCHAR(255)) AS level4name,
                CAST(data_payload:Level5Name::TEXT AS VARCHAR(255)) AS level5name,
                CAST(data_payload:Level6Name::TEXT AS VARCHAR(255)) AS level6name,
                CAST(data_payload:Level7Name::TEXT AS VARCHAR(255)) AS level7name,
                CAST(data_payload:Level8Name::TEXT AS VARCHAR(255)) AS level8name,
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
            FROM {{ source('gwpc', 'pcx_agencycostcentre_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:level9name::TEXT AS VARCHAR(255)) AS level9name,
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:accountid::NUMBER AS accountid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:costcentrecode::TEXT AS VARCHAR(255)) AS costcentrecode,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                CAST($1:level2code::TEXT AS VARCHAR(255)) AS level2code,
                CAST($1:level3code::TEXT AS VARCHAR(255)) AS level3code,
                CAST($1:level4code::TEXT AS VARCHAR(255)) AS level4code,
                CAST($1:level5code::TEXT AS VARCHAR(255)) AS level5code,
                CAST($1:level6code::TEXT AS VARCHAR(255)) AS level6code,
                CAST($1:level7code::TEXT AS VARCHAR(255)) AS level7code,
                CAST($1:level8code::TEXT AS VARCHAR(255)) AS level8code,
                CAST($1:level9code::TEXT AS VARCHAR(255)) AS level9code,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:costcentrename::TEXT AS VARCHAR(255)) AS costcentrename,
                CAST($1:level2name::TEXT AS VARCHAR(255)) AS level2name,
                CAST($1:level3name::TEXT AS VARCHAR(255)) AS level3name,
                CAST($1:level4name::TEXT AS VARCHAR(255)) AS level4name,
                CAST($1:level5name::TEXT AS VARCHAR(255)) AS level5name,
                CAST($1:level6name::TEXT AS VARCHAR(255)) AS level6name,
                CAST($1:level7name::TEXT AS VARCHAR(255)) AS level7name,
                CAST($1:level8name::TEXT AS VARCHAR(255)) AS level8name,
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
            FROM {{ source('gwpc', 'pcx_agencycostcentre_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS agencycostcentre_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'level9name',
                        'loadcommandid',
                        'publicid',
                        'accountid',
                        'createtime',
                        'costcentrecode',
                        'updatetime',
                        'level2code',
                        'level3code',
                        'level4code',
                        'level5code',
                        'level6code',
                        'level7code',
                        'level8code',
                        'level9code',
                        'createuserid',
                        'beanversion',
                        'archivepartition',
                        'updateuserid',
                        'costcentrename',
                        'level2name',
                        'level3name',
                        'level4name',
                        'level5name',
                        'level6name',
                        'level7name',
                        'level8name'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}