{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_pain002odsworkitem_icare.
                                                pain002odsworkitem_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_pain002odsworkitem_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:ProcessHistoryID::NUMBER AS processhistoryid,
                CAST(data_payload:ProcessStatus::TEXT AS VARCHAR(255)) AS processstatus,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Priority::NUMBER AS priority,
                data_payload:Attempts::NUMBER AS attempts,
                CAST(data_payload:OriginalPaymentInfID::TEXT AS VARCHAR(255)) AS originalpaymentinfid,
                TO_TIMESTAMP_TZ(data_payload:CreateDate::NUMBER/1000) AS createdate,
                TO_TIMESTAMP_TZ(data_payload:CreationTime::NUMBER/1000) AS creationtime,
                TO_TIMESTAMP_TZ(data_payload:LastUpdateTime::NUMBER/1000) AS lastupdatetime,
                CAST(data_payload:RejectionReason::TEXT AS VARCHAR(255)) AS rejectionreason,
                CAST(data_payload:Exception::TEXT AS VARCHAR(16777216)) AS exception,
                CAST(data_payload:TransactionStatus::TEXT AS VARCHAR(255)) AS transactionstatus,
                data_payload:AvailableSince::NUMBER AS availablesince,
                CAST(data_payload:OriginalEndToEndID::TEXT AS VARCHAR(255)) AS originalendtoendid,
                data_payload:Status::NUMBER AS status,
                CAST(data_payload:ControlRecordMessageID::TEXT AS VARCHAR(255)) AS controlrecordmessageid,
                CAST(data_payload:OriginalInstrID::TEXT AS VARCHAR(255)) AS originalinstrid,
                TO_TIMESTAMP_TZ(data_payload:LastUpdateDateTime::NUMBER/1000) AS lastupdatedatetime,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:CheckedOutBy::TEXT AS VARCHAR(50)) AS checkedoutby,
                CAST(data_payload:Lvl3StatusID::TEXT AS VARCHAR(255)) AS lvl3statusid,
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
            FROM {{ source('gwcc', 'ccx_pain002odsworkitem_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:processhistoryid::NUMBER AS processhistoryid,
                CAST($1:processstatus::TEXT AS VARCHAR(255)) AS processstatus,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:priority::NUMBER AS priority,
                $1:attempts::NUMBER AS attempts,
                CAST($1:originalpaymentinfid::TEXT AS VARCHAR(255)) AS originalpaymentinfid,
                $1:createdate::TIMESTAMP_TZ AS createdate,
                $1:creationtime::TIMESTAMP_TZ AS creationtime,
                $1:lastupdatetime::TIMESTAMP_TZ AS lastupdatetime,
                CAST($1:rejectionreason::TEXT AS VARCHAR(255)) AS rejectionreason,
                CAST($1:exception::TEXT AS VARCHAR(16777216)) AS exception,
                CAST($1:transactionstatus::TEXT AS VARCHAR(255)) AS transactionstatus,
                $1:availablesince::NUMBER AS availablesince,
                CAST($1:originalendtoendid::TEXT AS VARCHAR(255)) AS originalendtoendid,
                $1:status::NUMBER AS status,
                CAST($1:controlrecordmessageid::TEXT AS VARCHAR(255)) AS controlrecordmessageid,
                CAST($1:originalinstrid::TEXT AS VARCHAR(255)) AS originalinstrid,
                $1:lastupdatedatetime::TIMESTAMP_TZ AS lastupdatedatetime,
                $1:id::NUMBER AS id,
                CAST($1:checkedoutby::TEXT AS VARCHAR(50)) AS checkedoutby,
                CAST($1:lvl3statusid::TEXT AS VARCHAR(255)) AS lvl3statusid,
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
            FROM {{ source('gwcc', 'ccx_pain002odsworkitem_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS pain002odsworkitem_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'processhistoryid',
                        'processstatus',
                        'publicid',
                        'priority',
                        'attempts',
                        'originalpaymentinfid',
                        'createdate',
                        'creationtime',
                        'lastupdatetime',
                        'rejectionreason',
                        'exception',
                        'transactionstatus',
                        'availablesince',
                        'originalendtoendid',
                        'status',
                        'controlrecordmessageid',
                        'originalinstrid',
                        'lastupdatedatetime',
                        'checkedoutby',
                        'lvl3statusid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}