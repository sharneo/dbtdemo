{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_payginput_icare.
                                                payginput_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "ccx_payginput_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:EntitlementAmount AS NUMBER(18,2)) AS entitlementamount,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:TFNReported::BOOLEAN AS tfnreported,
                data_payload:DependentSpouse::BOOLEAN AS dependentspouse,
                data_payload:AccumulatedHELPDebt::BOOLEAN AS accumulatedhelpdebt,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:AccumulatedFSDebt::BOOLEAN AS accumulatedfsdebt,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:CheckID::NUMBER AS checkid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:MedicareLevy::BOOLEAN AS medicarelevy,
                data_payload:DomesticResident::BOOLEAN AS domesticresident,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:TaxFreeThreshold::BOOLEAN AS taxfreethreshold,
                CAST(data_payload:CurrentEntitlementAmt AS NUMBER(18,2)) AS currententitlementamt,
                data_payload:CurrentYear::BOOLEAN AS currentyear,
                data_payload:PayeeAge::NUMBER AS payeeage,
                data_payload:NbrDepChldrnForMedcrLevy::NUMBER AS nbrdepchldrnformedcrlevy,
                CAST(data_payload:EntlmntAmntAftrDdctns AS NUMBER(18,2)) AS entlmntamntaftrddctns,
                CAST(data_payload:TaxOffset AS NUMBER(18,2)) AS taxoffset,
                CAST(data_payload:PayCycle::TEXT AS VARCHAR(512)) AS paycycle,
                TO_TIMESTAMP_TZ(data_payload:WageBenefitStartDate::NUMBER/1000) AS wagebenefitstartdate,
                data_payload:MedicareExemption::NUMBER AS medicareexemption,
                data_payload:PAYGScale::NUMBER AS paygscale,
                TO_TIMESTAMP_TZ(data_payload:WageBenefitEndDate::NUMBER/1000) AS wagebenefitenddate,
                data_payload:SeniorPnsnrStatus::NUMBER AS seniorpnsnrstatus,
                TO_TIMESTAMP_TZ(data_payload:TransactionDate_icare::NUMBER/1000) AS transactiondate_icare,
                TO_TIMESTAMP_TZ(data_payload:EffectiveFromVariation_icare::NUMBER/1000) AS effectivefromvariation_icare,
                CAST(data_payload:UpwardVariationPercent_icare AS NUMBER(10,2)) AS upwardvariationpercent_icare,
                CAST(data_payload:UpwardVariationAmt_icare AS NUMBER(18,2)) AS upwardvariationamt_icare,
                CAST(data_payload:DownwardVariationPercent_icare AS NUMBER(10,2)) AS downwardvariationpercent_icare,
                TO_TIMESTAMP_TZ(data_payload:EffectiveToVariation_icare::NUMBER/1000) AS effectivetovariation_icare,
                CAST(data_payload:DownwardVariationAmt_icare AS NUMBER(18,2)) AS downwardvariationamt_icare,
                data_payload:WithholdingVariation_icare::BOOLEAN AS withholdingvariation_icare,
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
            FROM {{ source('gwcc', 'ccx_payginput_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:entitlementamount AS NUMBER(18,2)) AS entitlementamount,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:tfnreported::BOOLEAN AS tfnreported,
                $1:dependentspouse::BOOLEAN AS dependentspouse,
                $1:accumulatedhelpdebt::BOOLEAN AS accumulatedhelpdebt,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:accumulatedfsdebt::BOOLEAN AS accumulatedfsdebt,
                $1:createuserid::NUMBER AS createuserid,
                $1:checkid::NUMBER AS checkid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:medicarelevy::BOOLEAN AS medicarelevy,
                $1:domesticresident::BOOLEAN AS domesticresident,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:taxfreethreshold::BOOLEAN AS taxfreethreshold,
                CAST($1:currententitlementamt AS NUMBER(18,2)) AS currententitlementamt,
                $1:currentyear::BOOLEAN AS currentyear,
                $1:payeeage::NUMBER AS payeeage,
                $1:nbrdepchldrnformedcrlevy::NUMBER AS nbrdepchldrnformedcrlevy,
                CAST($1:entlmntamntaftrddctns AS NUMBER(18,2)) AS entlmntamntaftrddctns,
                CAST($1:taxoffset AS NUMBER(18,2)) AS taxoffset,
                CAST($1:paycycle::TEXT AS VARCHAR(512)) AS paycycle,
                $1:wagebenefitstartdate::TIMESTAMP_TZ AS wagebenefitstartdate,
                $1:medicareexemption::NUMBER AS medicareexemption,
                $1:paygscale::NUMBER AS paygscale,
                $1:wagebenefitenddate::TIMESTAMP_TZ AS wagebenefitenddate,
                $1:seniorpnsnrstatus::NUMBER AS seniorpnsnrstatus,
                $1:transactiondate_icare::TIMESTAMP_TZ AS transactiondate_icare,
                $1:effectivefromvariation_icare::TIMESTAMP_TZ AS effectivefromvariation_icare,
                CAST($1:upwardvariationpercent_icare AS NUMBER(10,2)) AS upwardvariationpercent_icare,
                CAST($1:upwardvariationamt_icare AS NUMBER(18,2)) AS upwardvariationamt_icare,
                CAST($1:downwardvariationpercent_icare AS NUMBER(10,2)) AS downwardvariationpercent_icare,
                $1:effectivetovariation_icare::TIMESTAMP_TZ AS effectivetovariation_icare,
                CAST($1:downwardvariationamt_icare AS NUMBER(18,2)) AS downwardvariationamt_icare,
                $1:withholdingvariation_icare::BOOLEAN AS withholdingvariation_icare,
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
            FROM {{ source('gwcc', 'ccx_payginput_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS payginput_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'entitlementamount',
                        'createtime',
                        'tfnreported',
                        'dependentspouse',
                        'accumulatedhelpdebt',
                        'updatetime',
                        'accumulatedfsdebt',
                        'createuserid',
                        'checkid',
                        'archivepartition',
                        'beanversion',
                        'medicarelevy',
                        'domesticresident',
                        'updateuserid',
                        'taxfreethreshold',
                        'currententitlementamt',
                        'currentyear',
                        'payeeage',
                        'nbrdepchldrnformedcrlevy',
                        'entlmntamntaftrddctns',
                        'taxoffset',
                        'paycycle',
                        'wagebenefitstartdate',
                        'medicareexemption',
                        'paygscale',
                        'wagebenefitenddate',
                        'seniorpnsnrstatus',
                        'transactiondate_icare',
                        'effectivefromvariation_icare',
                        'upwardvariationpercent_icare',
                        'upwardvariationamt_icare',
                        'downwardvariationpercent_icare',
                        'effectivetovariation_icare',
                        'downwardvariationamt_icare',
                        'withholdingvariation_icare'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}