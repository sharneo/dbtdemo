{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-06-01      0.0                             Incremental staging model for ccx_recoveryinvoice_ext.
                                                recoveryinvoice_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "ccx_recoveryinvoice_ext"]
) }}

WITH cte_source_data AS
(
            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                CAST(data_payload:InvoiceNumber::TEXT AS VARCHAR(50)) AS invoicenumber,
                CAST(data_payload:Amount AS NUMBER(18,2)) AS amount,
                CAST(data_payload:PaidAmount AS NUMBER(18,2)) AS paidamount,
                CAST(data_payload:WriteOffAmount AS NUMBER(18,2)) AS writeoffamount,
                TO_TIMESTAMP_TZ(data_payload:DueDate::NUMBER/1000) AS duedate,
                TO_TIMESTAMP_TZ(data_payload:InvoiceDate::NUMBER/1000) AS invoicedate,
                data_payload:DeductibleID::NUMBER AS deductibleid,
                CAST(data_payload:RecoveryReservePublicID::TEXT AS VARCHAR(64)) AS recoveryreservepublicid,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:Retired::NUMBER AS retired,
                CAST(NULL AS TIMESTAMP_LTZ) AS gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) AS gwcbi_lsn,
                CAST(NULL AS NUMBER) AS gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) AS gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) AS gwcbi_seqval,
                CAST(NULL AS STRING) AS gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) AS gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'AVRO' AS file_type,
                'GWCC' AS source_system
            FROM {{ source('gwcc', 'ccx_recoveryinvoice_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'

            UNION ALL

            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                CAST($1:invoicenumber::TEXT AS VARCHAR(50)) AS invoicenumber,
                CAST($1:amount AS NUMBER(18,2)) AS amount,
                CAST($1:paidamount AS NUMBER(18,2)) AS paidamount,
                CAST($1:writeoffamount AS NUMBER(18,2)) AS writeoffamount,
                $1:duedate::TIMESTAMP_TZ AS duedate,
                $1:invoicedate::TIMESTAMP_TZ AS invoicedate,
                $1:deductibleid::NUMBER AS deductibleid,
                CAST($1:recoveryreservepublicid::TEXT AS VARCHAR(64)) AS recoveryreservepublicid,
                $1:subtype::NUMBER AS subtype,
                $1:retired::NUMBER AS retired,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) AS gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER AS gwcbi_lsn,
                $1:gwcbi___operation::NUMBER AS gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) AS gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER AS gwcbi_seqval,
                $1:gwcbi___seqval_hex::STRING AS gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER AS gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'PARQUET' AS file_type,
                'GWCC' AS source_system
            FROM {{ source('gwcc', 'ccx_recoveryinvoice_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key(['id']) }} AS VARCHAR(150)) AS recoveryinvoice_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
            'loadcommandid',
            'createuserid',
            'publicid',
            'createtime',
            'updateuserid',
            'updatetime',
            'beanversion',
            'archivepartition',
            'invoicenumber',
            'amount',
            'paidamount',
            'writeoffamount',
            'duedate',
            'invoicedate',
            'deductibleid',
            'recoveryreservepublicid',
            'subtype',
            'retired'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ID
        ORDER BY record_insertion_date DESC 
    ) = 1
)

SELECT * FROM cte_transformed
{%- if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{%- endif %}
