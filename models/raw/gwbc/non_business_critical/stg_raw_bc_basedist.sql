{{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_basedist.
                                                basedist_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}}   

{{ config(
    materialized='incremental',
    transient=True,
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=["raw_layer", "raw_billing_centre", "billing_centre", "non_business_critical", "bc_basedist"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:BankRefDetail_icare::TEXT AS VARCHAR(255)) AS bankrefdetail_icare,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:WriteOffAmount AS NUMBER(18,2)) AS writeoffamount,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:FrozenByArchiving::BOOLEAN AS frozenbyarchiving,
                data_payload:WriteOffAmount_cur::NUMBER AS writeoffamount_cur,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:AppliedDate::NUMBER/1000) AS applieddate,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:InvoiceNumber_icare::TEXT AS VARCHAR(255)) AS invoicenumber_icare,
                TO_TIMESTAMP_TZ(data_payload:DistributedDate::NUMBER/1000) AS distributeddate,
                data_payload:Currency::NUMBER AS currency,
                data_payload:NetDistToInvoiceItems_cur::NUMBER AS netdisttoinvoiceitems_cur,
                CAST(data_payload:NetDistributedToInvoiceItems AS NUMBER(18,2)) AS netdistributedtoinvoiceitems,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:ReversalDate::NUMBER/1000) AS reversaldate,
                CAST(data_payload:NetInSuspense AS NUMBER(18,2)) AS netinsuspense,
                data_payload:NetInSuspense_cur::NUMBER AS netinsuspense_cur,
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
            FROM {{ source('gwbc', 'bc_basedist') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:bankrefdetail_icare::TEXT AS VARCHAR(255)) AS bankrefdetail_icare,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:writeoffamount AS NUMBER(18,2)) AS writeoffamount,
                $1:beanversion::NUMBER AS beanversion,
                $1:frozenbyarchiving::BOOLEAN AS frozenbyarchiving,
                $1:writeoffamount_cur::NUMBER AS writeoffamount_cur,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:applieddate::TIMESTAMP_TZ AS applieddate,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:invoicenumber_icare::TEXT AS VARCHAR(255)) AS invoicenumber_icare,
                $1:distributeddate::TIMESTAMP_TZ AS distributeddate,
                $1:currency::NUMBER AS currency,
                $1:netdisttoinvoiceitems_cur::NUMBER AS netdisttoinvoiceitems_cur,
                CAST($1:netdistributedtoinvoiceitems AS NUMBER(18,2)) AS netdistributedtoinvoiceitems,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                $1:reversaldate::TIMESTAMP_TZ AS reversaldate,
                CAST($1:netinsuspense AS NUMBER(18,2)) AS netinsuspense,
                $1:netinsuspense_cur::NUMBER AS netinsuspense_cur,
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
            FROM {{ source('gwbc', 'bc_basedist') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS basedist_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'bankrefdetail_icare',
                        'publicid',
                        'writeoffamount',
                        'beanversion',
                        'frozenbyarchiving',
                        'writeoffamount_cur',
                        'retired',
                        'createtime',
                        'applieddate',
                        'updateuserid',
                        'invoicenumber_icare',
                        'distributeddate',
                        'currency',
                        'netdisttoinvoiceitems_cur',
                        'netdistributedtoinvoiceitems',
                        'updatetime',
                        'subtype',
                        'reversaldate',
                        'netinsuspense',
                        'netinsuspense_cur'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}