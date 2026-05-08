{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_liabilityreview.
                                                liabilityreview_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_liabilityreview"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:ReviewerID::NUMBER AS reviewerid,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ClaimWCID::NUMBER AS claimwcid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:ReviewDate::NUMBER/1000) AS reviewdate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:RefId::TEXT AS VARCHAR(6)) AS refid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:ReviewType::NUMBER AS reviewtype,
                data_payload:Outcome::NUMBER AS outcome,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:RequesterID::NUMBER AS requesterid,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:RequestDate::NUMBER/1000) AS requestdate,
                CAST(data_payload:Comment::TEXT AS VARCHAR(255)) AS comment,
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
            FROM {{ source('gwcc', 'ccx_liabilityreview') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:reviewerid::NUMBER AS reviewerid,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:claimwcid::NUMBER AS claimwcid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:reviewdate::TIMESTAMP_TZ AS reviewdate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:refid::TEXT AS VARCHAR(6)) AS refid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:reviewtype::NUMBER AS reviewtype,
                $1:outcome::NUMBER AS outcome,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:requesterid::NUMBER AS requesterid,
                $1:id::NUMBER AS id,
                $1:requestdate::TIMESTAMP_TZ AS requestdate,
                CAST($1:comment::TEXT AS VARCHAR(255)) AS comment,
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
            FROM {{ source('gwcc', 'ccx_liabilityreview') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS liabilityreview_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'reviewerid',
                        'loadcommandid',
                        'createuserid',
                        'claimwcid',
                        'publicid',
                        'reviewdate',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'refid',
                        'updateuserid',
                        'reviewtype',
                        'outcome',
                        'updatetime',
                        'requesterid',
                        'requestdate',
                        'comment'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}