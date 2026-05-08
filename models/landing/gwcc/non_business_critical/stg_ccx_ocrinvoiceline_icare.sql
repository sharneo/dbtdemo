{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_ocrinvoiceline_icare.
                                                ocrinvoiceline_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_ocrinvoiceline_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:GSTAmount AS NUMBER(18,2)) AS gstamount,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:ServiceProviderID::TEXT AS VARCHAR(255)) AS serviceproviderid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:Servicedate::NUMBER/1000) AS servicedate,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:PaymentLineAmountNet AS NUMBER(18,2)) AS paymentlineamountnet,
                CAST(data_payload:ABN::TEXT AS VARCHAR(255)) AS abn,
                CAST(data_payload:HICProviderID::TEXT AS VARCHAR(255)) AS hicproviderid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:OCRInvoice::NUMBER AS ocrinvoice,
                CAST(data_payload:ServiceProviderName::TEXT AS VARCHAR(255)) AS serviceprovidername,
                CAST(data_payload:PaymentLineAmountGross AS NUMBER(18,2)) AS paymentlineamountgross,
                data_payload:PaymentClassificationNumber::NUMBER AS paymentclassificationnumber,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:SequenceNumber::NUMBER AS sequencenumber,
                data_payload:Quantity::NUMBER AS quantity,
                CAST(data_payload:ServiceProviderCRMId::TEXT AS VARCHAR(255)) AS serviceprovidercrmid,
                CAST(data_payload:PaycodeDesc::TEXT AS VARCHAR(255)) AS paycodedesc,
                CAST(data_payload:MBSCode::TEXT AS VARCHAR(40)) AS mbscode,
                data_payload:AltCodeType::NUMBER AS altcodetype,
                CAST(data_payload:AltCode::TEXT AS VARCHAR(10)) AS altcode,
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
            FROM {{ source('gwcc', 'ccx_ocrinvoiceline_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:gstamount AS NUMBER(18,2)) AS gstamount,
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:serviceproviderid::TEXT AS VARCHAR(255)) AS serviceproviderid,
                $1:createuserid::NUMBER AS createuserid,
                $1:servicedate::TIMESTAMP_TZ AS servicedate,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:paymentlineamountnet AS NUMBER(18,2)) AS paymentlineamountnet,
                CAST($1:abn::TEXT AS VARCHAR(255)) AS abn,
                CAST($1:hicproviderid::TEXT AS VARCHAR(255)) AS hicproviderid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:ocrinvoice::NUMBER AS ocrinvoice,
                CAST($1:serviceprovidername::TEXT AS VARCHAR(255)) AS serviceprovidername,
                CAST($1:paymentlineamountgross AS NUMBER(18,2)) AS paymentlineamountgross,
                $1:paymentclassificationnumber::NUMBER AS paymentclassificationnumber,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:sequencenumber::NUMBER AS sequencenumber,
                $1:quantity::NUMBER AS quantity,
                CAST($1:serviceprovidercrmid::TEXT AS VARCHAR(255)) AS serviceprovidercrmid,
                CAST($1:paycodedesc::TEXT AS VARCHAR(255)) AS paycodedesc,
                CAST($1:mbscode::TEXT AS VARCHAR(40)) AS mbscode,
                $1:altcodetype::NUMBER AS altcodetype,
                CAST($1:altcode::TEXT AS VARCHAR(10)) AS altcode,
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
            FROM {{ source('gwcc', 'ccx_ocrinvoiceline_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS ocrinvoiceline_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'gstamount',
                        'loadcommandid',
                        'serviceproviderid',
                        'createuserid',
                        'servicedate',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'paymentlineamountnet',
                        'abn',
                        'hicproviderid',
                        'updateuserid',
                        'ocrinvoice',
                        'serviceprovidername',
                        'paymentlineamountgross',
                        'paymentclassificationnumber',
                        'updatetime',
                        'sequencenumber',
                        'quantity',
                        'serviceprovidercrmid',
                        'paycodedesc',
                        'mbscode',
                        'altcodetype',
                        'altcode'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}