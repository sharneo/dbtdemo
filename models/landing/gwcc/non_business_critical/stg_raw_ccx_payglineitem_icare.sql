{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_payglineitem_icare.
                                                payglineitem_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "ccx_payglineitem_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PayCodeName::TEXT AS VARCHAR(1333)) AS paycodename,
                CAST(data_payload:GarnisheeAmount AS NUMBER(18,2)) AS garnisheeamount,
                CAST(data_payload:PostGarnisheeAmount AS NUMBER(18,2)) AS postgarnisheeamount,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:FinancialYear::TEXT AS VARCHAR(32)) AS financialyear,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:DateTo::NUMBER/1000) AS dateto,
                CAST(data_payload:PAYGTax AS NUMBER(18,2)) AS paygtax,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:IsLumpSumE::BOOLEAN AS islumpsume,
                data_payload:LineCategory::NUMBER AS linecategory,
                CAST(data_payload:PayCode::TEXT AS VARCHAR(128)) AS paycode,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:TransactionAmount AS NUMBER(18,2)) AS transactionamount,
                TO_TIMESTAMP_TZ(data_payload:DateFrom::NUMBER/1000) AS datefrom,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:LumpSumEAmount AS NUMBER(18,2)) AS lumpsumeamount,
                data_payload:PAYGCheck_icareID::NUMBER AS paygcheck_icareid,
                data_payload:IsReimbursementLineItem::BOOLEAN AS isreimbursementlineitem,
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
            FROM {{ source('gwcc', 'ccx_payglineitem_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:paycodename::TEXT AS VARCHAR(1333)) AS paycodename,
                CAST($1:garnisheeamount AS NUMBER(18,2)) AS garnisheeamount,
                CAST($1:postgarnisheeamount AS NUMBER(18,2)) AS postgarnisheeamount,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:financialyear::TEXT AS VARCHAR(32)) AS financialyear,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:dateto::TIMESTAMP_TZ AS dateto,
                CAST($1:paygtax AS NUMBER(18,2)) AS paygtax,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:islumpsume::BOOLEAN AS islumpsume,
                $1:linecategory::NUMBER AS linecategory,
                CAST($1:paycode::TEXT AS VARCHAR(128)) AS paycode,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:transactionamount AS NUMBER(18,2)) AS transactionamount,
                $1:datefrom::TIMESTAMP_TZ AS datefrom,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                CAST($1:lumpsumeamount AS NUMBER(18,2)) AS lumpsumeamount,
                $1:paygcheck_icareid::NUMBER AS paygcheck_icareid,
                $1:isreimbursementlineitem::BOOLEAN AS isreimbursementlineitem,
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
            FROM {{ source('gwcc', 'ccx_payglineitem_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS payglineitem_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'paycodename',
                        'garnisheeamount',
                        'postgarnisheeamount',
                        'createuserid',
                        'publicid',
                        'financialyear',
                        'beanversion',
                        'createtime',
                        'retired',
                        'dateto',
                        'paygtax',
                        'updateuserid',
                        'islumpsume',
                        'linecategory',
                        'paycode',
                        'updatetime',
                        'transactionamount',
                        'datefrom',
                        'subtype',
                        'lumpsumeamount',
                        'paygcheck_icareid',
                        'isreimbursementlineitem'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}