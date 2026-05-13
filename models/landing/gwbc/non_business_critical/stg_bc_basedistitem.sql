{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_basedistitem.
                                                basedistitem_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwbc", "billing_centre", "non_business_critical", "bc_basedistitem"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:ReversedDistID::NUMBER AS reverseddistid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ActiveDistID::NUMBER AS activedistid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:PolicyPeriodID::NUMBER AS policyperiodid,
                data_payload:Currency::NUMBER AS currency,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ProducerCodeID::NUMBER AS producercodeid,
                data_payload:InvoiceItemID::NUMBER AS invoiceitemid,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:AppliedDate::NUMBER/1000) AS applieddate,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:ReversedDate::NUMBER/1000) AS reverseddate,
                CAST(data_payload:CommissionAmountToApply AS NUMBER(18,2)) AS commissionamounttoapply,
                data_payload:CommissionAmountToApply_cur::NUMBER AS commissionamounttoapply_cur,
                CAST(data_payload:GrossAmountToApply AS NUMBER(18,2)) AS grossamounttoapply,
                data_payload:Disposition::NUMBER AS disposition,
                data_payload:GrossAmountToApply_cur::NUMBER AS grossamounttoapply_cur,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:PaymentComments::TEXT AS VARCHAR(255)) AS paymentcomments,
                TO_TIMESTAMP_TZ(data_payload:ExecutedDate::NUMBER/1000) AS executeddate,
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
                'GWBC' as source_system
            FROM {{ source('gwbc', 'bc_basedistitem') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:reverseddistid::NUMBER AS reverseddistid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:activedistid::NUMBER AS activedistid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:policyperiodid::NUMBER AS policyperiodid,
                $1:currency::NUMBER AS currency,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:producercodeid::NUMBER AS producercodeid,
                $1:invoiceitemid::NUMBER AS invoiceitemid,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:applieddate::TIMESTAMP_TZ AS applieddate,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:reverseddate::TIMESTAMP_TZ AS reverseddate,
                CAST($1:commissionamounttoapply AS NUMBER(18,2)) AS commissionamounttoapply,
                $1:commissionamounttoapply_cur::NUMBER AS commissionamounttoapply_cur,
                CAST($1:grossamounttoapply AS NUMBER(18,2)) AS grossamounttoapply,
                $1:disposition::NUMBER AS disposition,
                $1:grossamounttoapply_cur::NUMBER AS grossamounttoapply_cur,
                $1:subtype::NUMBER AS subtype,
                CAST($1:paymentcomments::TEXT AS VARCHAR(255)) AS paymentcomments,
                $1:executeddate::TIMESTAMP_TZ AS executeddate,
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
                'GWBC' as source_system
            FROM {{ source('gwbc', 'bc_basedistitem') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS basedistitem_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'reverseddistid',
                        'publicid',
                        'activedistid',
                        'createtime',
                        'policyperiodid',
                        'currency',
                        'updatetime',
                        'producercodeid',
                        'invoiceitemid',
                        'createuserid',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'applieddate',
                        'updateuserid',
                        'reverseddate',
                        'commissionamounttoapply',
                        'commissionamounttoapply_cur',
                        'grossamounttoapply',
                        'disposition',
                        'grossamounttoapply_cur',
                        'subtype',
                        'paymentcomments',
                        'executeddate'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}