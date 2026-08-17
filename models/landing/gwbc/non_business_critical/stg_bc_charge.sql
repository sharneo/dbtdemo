{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_charge.
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
                data_payload:SkipInvoiceItemCreation::BOOLEAN AS skipinvoiceitemcreation,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:Reversed::BOOLEAN AS reversed,
                data_payload:TAccountContainerID::NUMBER AS taccountcontainerid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:WrittenDate::NUMBER/1000) AS writtendate,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:ChargeGroup::TEXT AS VARCHAR(255)) AS chargegroup,
                data_payload:BillingInstructionID::NUMBER AS billinginstructionid,
                data_payload:Currency::NUMBER AS currency,
                TO_TIMESTAMP_TZ(data_payload:HoldReleaseDate::NUMBER/1000) AS holdreleasedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Amount AS NUMBER(18,2)) AS amount,
                data_payload:Amount_cur::NUMBER AS amount_cur,
                data_payload:ID::NUMBER AS id,
                data_payload:TotalInstallments::NUMBER AS totalinstallments,
                data_payload:OverridingInvoiceStreamID::NUMBER AS overridinginvoicestreamid,
                TO_TIMESTAMP_TZ(data_payload:ChargeDate::NUMBER/1000) AS chargedate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:OverridingPrimaryCmsnRcvrID::NUMBER AS overridingprimarycmsnrcvrid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:OverridingPayerContainerID::NUMBER AS overridingpayercontainerid,
                data_payload:HoldStatus::NUMBER AS holdstatus,
                CAST(data_payload:FeeDescription_icare::TEXT AS VARCHAR(255)) AS feedescription_icare,
                data_payload:ChargePatternID::NUMBER AS chargepatternid,
                data_payload:DoubtfulDebtAmount_cur::NUMBER AS doubtfuldebtamount_cur,
                CAST(data_payload:DoubtfulDebtAmount_amt AS NUMBER(18,2)) AS doubtfuldebtamount_amt,
                data_payload:ClaimRecoveryDetails_icareID::NUMBER AS claimrecoverydetails_icareid,
                data_payload:DoubtfulDebt::NUMBER AS doubtfuldebt,
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
            FROM {{ source('gwbc', 'bc_charge') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:skipinvoiceitemcreation::BOOLEAN AS skipinvoiceitemcreation,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:reversed::BOOLEAN AS reversed,
                $1:taccountcontainerid::NUMBER AS taccountcontainerid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:writtendate::TIMESTAMP_TZ AS writtendate,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:chargegroup::TEXT AS VARCHAR(255)) AS chargegroup,
                $1:billinginstructionid::NUMBER AS billinginstructionid,
                $1:currency::NUMBER AS currency,
                $1:holdreleasedate::TIMESTAMP_TZ AS holdreleasedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:amount AS NUMBER(18,2)) AS amount,
                $1:amount_cur::NUMBER AS amount_cur,
                $1:id::NUMBER AS id,
                $1:totalinstallments::NUMBER AS totalinstallments,
                $1:overridinginvoicestreamid::NUMBER AS overridinginvoicestreamid,
                $1:chargedate::TIMESTAMP_TZ AS chargedate,
                $1:createuserid::NUMBER AS createuserid,
                $1:overridingprimarycmsnrcvrid::NUMBER AS overridingprimarycmsnrcvrid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:overridingpayercontainerid::NUMBER AS overridingpayercontainerid,
                $1:holdstatus::NUMBER AS holdstatus,
                CAST($1:feedescription_icare::TEXT AS VARCHAR(255)) AS feedescription_icare,
                $1:chargepatternid::NUMBER AS chargepatternid,
                $1:doubtfuldebtamount_cur::NUMBER AS doubtfuldebtamount_cur,
                CAST($1:doubtfuldebtamount_amt AS NUMBER(18,2)) AS doubtfuldebtamount_amt,
                $1:claimrecoverydetails_icareid::NUMBER AS claimrecoverydetails_icareid,
                $1:doubtfuldebt::NUMBER AS doubtfuldebt,
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
            FROM {{ source('gwbc', 'bc_charge') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS charge_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'skipinvoiceitemcreation',
                        'loadcommandid',
                        'reversed',
                        'taccountcontainerid',
                        'publicid',
                        'writtendate',
                        'createtime',
                        'chargegroup',
                        'billinginstructionid',
                        'currency',
                        'holdreleasedate',
                        'updatetime',
                        'amount',
                        'amount_cur',
                        'totalinstallments',
                        'overridinginvoicestreamid',
                        'chargedate',
                        'createuserid',
                        'overridingprimarycmsnrcvrid',
                        'beanversion',
                        'archivepartition',
                        'updateuserid',
                        'overridingpayercontainerid',
                        'holdstatus',
                        'feedescription_icare',
                        'chargepatternid',
                        'doubtfuldebtamount_cur',
                        'doubtfuldebtamount_amt',
                        'claimrecoverydetails_icareid',
                        'doubtfuldebt'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date
