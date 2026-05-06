{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_crtransaction_icare.
                                                crtransaction_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_policy_centre", "policy_centre", "non_business_critical", "pcx_crtransaction_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:WrittenDate::NUMBER/1000) AS writtendate,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:ExpDate::NUMBER/1000) AS expdate,
                data_payload:FixedID::NUMBER AS fixedid,
                TO_TIMESTAMP_TZ(data_payload:EffDate::NUMBER/1000) AS effdate,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Written::BOOLEAN AS written,
                CAST(data_payload:Amount AS NUMBER(18,2)) AS amount,
                data_payload:Amount_cur::NUMBER AS amount_cur,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:AmountBilling AS NUMBER(18,2)) AS amountbilling,
                data_payload:PolicyFXRate::NUMBER AS policyfxrate,
                data_payload:AmountBilling_cur::NUMBER AS amountbilling_cur,
                data_payload:ToBeAccrued::BOOLEAN AS tobeaccrued,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ChangeType::NUMBER AS changetype,
                TO_TIMESTAMP_TZ(data_payload:PostedDate::NUMBER/1000) AS posteddate,
                data_payload:BasedOnID::NUMBER AS basedonid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Charged::BOOLEAN AS charged,
                data_payload:CRCost::NUMBER AS crcost,
                data_payload:BranchID::NUMBER AS branchid,
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
            FROM {{ source('gwpc', 'pcx_crtransaction_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:writtendate::TIMESTAMP_TZ AS writtendate,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:expdate::TIMESTAMP_TZ AS expdate,
                $1:fixedid::NUMBER AS fixedid,
                $1:effdate::TIMESTAMP_TZ AS effdate,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:written::BOOLEAN AS written,
                CAST($1:amount AS NUMBER(18,2)) AS amount,
                $1:amount_cur::NUMBER AS amount_cur,
                $1:id::NUMBER AS id,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:amountbilling AS NUMBER(18,2)) AS amountbilling,
                $1:policyfxrate::NUMBER AS policyfxrate,
                $1:amountbilling_cur::NUMBER AS amountbilling_cur,
                $1:tobeaccrued::BOOLEAN AS tobeaccrued,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:changetype::NUMBER AS changetype,
                $1:posteddate::TIMESTAMP_TZ AS posteddate,
                $1:basedonid::NUMBER AS basedonid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:charged::BOOLEAN AS charged,
                $1:crcost::NUMBER AS crcost,
                $1:branchid::NUMBER AS branchid,
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
            FROM {{ source('gwpc', 'pcx_crtransaction_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS crtransaction_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'publicid',
                        'writtendate',
                        'createtime',
                        'expdate',
                        'fixedid',
                        'effdate',
                        'effectivedate',
                        'updatetime',
                        'written',
                        'amount',
                        'amount_cur',
                        'expirationdate',
                        'createuserid',
                        'amountbilling',
                        'policyfxrate',
                        'amountbilling_cur',
                        'tobeaccrued',
                        'archivepartition',
                        'beanversion',
                        'changetype',
                        'posteddate',
                        'basedonid',
                        'updateuserid',
                        'charged',
                        'crcost',
                        'branchid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}