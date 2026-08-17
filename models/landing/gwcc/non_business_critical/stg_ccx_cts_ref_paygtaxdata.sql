{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_cts_ref_paygtaxdata.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwcc", "claim_centre", "non_business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:SpouseOnly AS NUMBER(18,2)) AS spouseonly,
                CAST(data_payload:FromAmount AS NUMBER(18,2)) AS fromamount,
                CAST(data_payload:Fraction AS NUMBER(5,4)) AS fraction,
                CAST(data_payload:AmountClaimed AS NUMBER(18,2)) AS amountclaimed,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:DependentNumber::NUMBER AS dependentnumber,
                CAST(data_payload:Name::TEXT AS VARCHAR(50)) AS name,
                data_payload:DepNbrLimit::NUMBER AS depnbrlimit,
                CAST(data_payload:ToAmount AS NUMBER(18,2)) AS toamount,
                CAST(data_payload:Child1 AS NUMBER(18,2)) AS child1,
                CAST(data_payload:Child2 AS NUMBER(18,2)) AS child2,
                CAST(data_payload:Child3 AS NUMBER(18,2)) AS child3,
                CAST(data_payload:EarningExtendedLimit AS NUMBER(18,2)) AS earningextendedlimit,
                TO_TIMESTAMP_TZ(data_payload:FromDate::NUMBER/1000) AS fromdate,
                CAST(data_payload:Child4 AS NUMBER(18,2)) AS child4,
                CAST(data_payload:AmtFrEchDep AS NUMBER(18,2)) AS amtfrechdep,
                CAST(data_payload:Child5 AS NUMBER(18,2)) AS child5,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:BaseAmount AS NUMBER(18,2)) AS baseamount,
                TO_TIMESTAMP_TZ(data_payload:ToDate::NUMBER/1000) AS todate,
                CAST(data_payload:Earnings AS NUMBER(18,2)) AS earnings,
                data_payload:DomesticPayee::BOOLEAN AS domesticpayee,
                data_payload:Value::NUMBER AS value,
                CAST(data_payload:EarningLimit AS NUMBER(18,2)) AS earninglimit,
                CAST(data_payload:NoTaxFreeThreshold AS NUMBER(18,2)) AS notaxfreethreshold,
                data_payload:TaxFreeThrsholdAppl::BOOLEAN AS taxfreethrsholdappl,
                data_payload:RuleType::NUMBER AS ruletype,
                CAST(data_payload:WithTaxFreeThreshold AS NUMBER(18,2)) AS withtaxfreethreshold,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:TaxOffset AS NUMBER(18,2)) AS taxoffset,
                CAST(data_payload:PayCycle::TEXT AS VARCHAR(25)) AS paycycle,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_cts_ref_paygtaxdata') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:spouseonly AS NUMBER(18,2)) AS spouseonly,
                CAST($1:fromamount AS NUMBER(18,2)) AS fromamount,
                CAST($1:fraction AS NUMBER(5,4)) AS fraction,
                CAST($1:amountclaimed AS NUMBER(18,2)) AS amountclaimed,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:dependentnumber::NUMBER AS dependentnumber,
                CAST($1:name::TEXT AS VARCHAR(50)) AS name,
                $1:depnbrlimit::NUMBER AS depnbrlimit,
                CAST($1:toamount AS NUMBER(18,2)) AS toamount,
                CAST($1:child1 AS NUMBER(18,2)) AS child1,
                CAST($1:child2 AS NUMBER(18,2)) AS child2,
                CAST($1:child3 AS NUMBER(18,2)) AS child3,
                CAST($1:earningextendedlimit AS NUMBER(18,2)) AS earningextendedlimit,
                $1:fromdate::TIMESTAMP_TZ AS fromdate,
                CAST($1:child4 AS NUMBER(18,2)) AS child4,
                CAST($1:amtfrechdep AS NUMBER(18,2)) AS amtfrechdep,
                CAST($1:child5 AS NUMBER(18,2)) AS child5,
                $1:id::NUMBER AS id,
                CAST($1:baseamount AS NUMBER(18,2)) AS baseamount,
                $1:todate::TIMESTAMP_TZ AS todate,
                CAST($1:earnings AS NUMBER(18,2)) AS earnings,
                $1:domesticpayee::BOOLEAN AS domesticpayee,
                $1:value::NUMBER AS value,
                CAST($1:earninglimit AS NUMBER(18,2)) AS earninglimit,
                CAST($1:notaxfreethreshold AS NUMBER(18,2)) AS notaxfreethreshold,
                $1:taxfreethrsholdappl::BOOLEAN AS taxfreethrsholdappl,
                $1:ruletype::NUMBER AS ruletype,
                CAST($1:withtaxfreethreshold AS NUMBER(18,2)) AS withtaxfreethreshold,
                $1:subtype::NUMBER AS subtype,
                CAST($1:taxoffset AS NUMBER(18,2)) AS taxoffset,
                CAST($1:paycycle::TEXT AS VARCHAR(25)) AS paycycle,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_cts_ref_paygtaxdata') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS cts_ref_paygtaxdata_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'spouseonly',
                        'fromamount',
                        'fraction',
                        'amountclaimed',
                        'publicid',
                        'dependentnumber',
                        'name',
                        'depnbrlimit',
                        'toamount',
                        'child1',
                        'child2',
                        'child3',
                        'earningextendedlimit',
                        'fromdate',
                        'child4',
                        'amtfrechdep',
                        'child5',
                        'baseamount',
                        'todate',
                        'earnings',
                        'domesticpayee',
                        'value',
                        'earninglimit',
                        'notaxfreethreshold',
                        'taxfreethrsholdappl',
                        'ruletype',
                        'withtaxfreethreshold',
                        'subtype',
                        'taxoffset',
                        'paycycle'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date
