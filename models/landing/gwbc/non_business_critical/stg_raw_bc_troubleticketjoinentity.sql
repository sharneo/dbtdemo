{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_troubleticketjoinentity.
                                                troubleticketjoinentity_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_billing_centre", "billing_centre", "non_business_critical", "bc_troubleticketjoinentity"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:ArchivedTransactionPublicID::TEXT AS VARCHAR(64)) AS archivedtransactionpublicid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:AccountID::NUMBER AS accountid,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:PolicyID::NUMBER AS policyid,
                data_payload:ProducerID::NUMBER AS producerid,
                data_payload:PolicyPeriodID::NUMBER AS policyperiodid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:TransactionID::NUMBER AS transactionid,
                data_payload:ID::NUMBER AS id,
                data_payload:TroubleTicketID::NUMBER AS troubleticketid,
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
            FROM {{ source('gwbc', 'bc_troubleticketjoinentity') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:archivedtransactionpublicid::TEXT AS VARCHAR(64)) AS archivedtransactionpublicid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:accountid::NUMBER AS accountid,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:policyid::NUMBER AS policyid,
                $1:producerid::NUMBER AS producerid,
                $1:policyperiodid::NUMBER AS policyperiodid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:transactionid::NUMBER AS transactionid,
                $1:id::NUMBER AS id,
                $1:troubleticketid::NUMBER AS troubleticketid,
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
            FROM {{ source('gwbc', 'bc_troubleticketjoinentity') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS troubleticketjoinentity_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'archivedtransactionpublicid',
                        'createuserid',
                        'publicid',
                        'beanversion',
                        'accountid',
                        'retired',
                        'createtime',
                        'policyid',
                        'producerid',
                        'policyperiodid',
                        'updateuserid',
                        'updatetime',
                        'transactionid',
                        'troubleticketid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}