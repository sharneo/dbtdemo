{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_fundstransfer.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwbc", "billing_centre", "non_business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:TargetProducer::NUMBER AS targetproducer,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Reason::NUMBER AS reason,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:SourceUnappliedFundID::NUMBER AS sourceunappliedfundid,
                data_payload:SourceProducer::NUMBER AS sourceproducer,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Currency::NUMBER AS currency,
                TO_TIMESTAMP_TZ(data_payload:ApprovalDate::NUMBER/1000) AS approvaldate,
                data_payload:TargetUnappliedFundID::NUMBER AS targetunappliedfundid,
                data_payload:ApprovalStatus::NUMBER AS approvalstatus,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Amount AS NUMBER(18,2)) AS amount,
                TO_TIMESTAMP_TZ(data_payload:TransferDate::NUMBER/1000) AS transferdate,
                data_payload:Amount_cur::NUMBER AS amount_cur,
                data_payload:RequestingUserID::NUMBER AS requestinguserid,
                data_payload:ID::NUMBER AS id,
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
                'GWBC' as source_system
            FROM {{ source('gwbc', 'bc_fundstransfer') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:targetproducer::NUMBER AS targetproducer,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:reason::NUMBER AS reason,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:sourceunappliedfundid::NUMBER AS sourceunappliedfundid,
                $1:sourceproducer::NUMBER AS sourceproducer,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:currency::NUMBER AS currency,
                $1:approvaldate::TIMESTAMP_TZ AS approvaldate,
                $1:targetunappliedfundid::NUMBER AS targetunappliedfundid,
                $1:approvalstatus::NUMBER AS approvalstatus,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:amount AS NUMBER(18,2)) AS amount,
                $1:transferdate::TIMESTAMP_TZ AS transferdate,
                $1:amount_cur::NUMBER AS amount_cur,
                $1:requestinguserid::NUMBER AS requestinguserid,
                $1:id::NUMBER AS id,
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
                'GWBC' as source_system
            FROM {{ source('gwbc', 'bc_fundstransfer') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS fundstransfer_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'targetproducer',
                        'publicid',
                        'reason',
                        'beanversion',
                        'createtime',
                        'retired',
                        'sourceunappliedfundid',
                        'sourceproducer',
                        'updateuserid',
                        'currency',
                        'approvaldate',
                        'targetunappliedfundid',
                        'approvalstatus',
                        'updatetime',
                        'amount',
                        'transferdate',
                        'amount_cur',
                        'requestinguserid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date
