{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bcx_bizruleoverrides_sp.
                                                bizruleoverrides_sp_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_billing_centre", "billing_centre", "non_business_critical", "bcx_bizruleoverrides_sp"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:FeeType::NUMBER AS feetype,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:OverrideBit::BOOLEAN AS overridebit,
                data_payload:Jurisdiction::NUMBER AS jurisdiction,
                data_payload:InUse::BOOLEAN AS inuse,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:MinAppAmount_Ext AS NUMBER(18,2)) AS minappamount_ext,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:MinAppAmount_Ext_cur::NUMBER AS minappamount_ext_cur,
                CAST(data_payload:MaximumPremiumAmount AS NUMBER(18,2)) AS maximumpremiumamount,
                data_payload:MaximumPremiumAmount_cur::NUMBER AS maximumpremiumamount_cur,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:DayUnitType::NUMBER AS dayunittype,
                data_payload:UWCompany::NUMBER AS uwcompany,
                data_payload:ID::NUMBER AS id,
                data_payload:BillingMethod::NUMBER AS billingmethod,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:PaymentRevReason::NUMBER AS paymentrevreason,
                CAST(data_payload:MinimumPremiumAmount AS NUMBER(18,2)) AS minimumpremiumamount,
                data_payload:OfferingType_Ext::NUMBER AS offeringtype_ext,
                data_payload:MinimumPremiumAmount_cur::NUMBER AS minimumpremiumamount_cur,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:LeadTimeType::NUMBER AS leadtimetype,
                CAST(data_payload:OverrideAmount AS NUMBER(18,2)) AS overrideamount,
                data_payload:DisbursementReason::NUMBER AS disbursementreason,
                data_payload:ThresholdType::NUMBER AS thresholdtype,
                data_payload:OverrideAmount_cur::NUMBER AS overrideamount_cur,
                data_payload:IsBackDated_Ext::BOOLEAN AS isbackdated_ext,
                data_payload:PaymentMethod::NUMBER AS paymentmethod,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:LowBalanceMethod::NUMBER AS lowbalancemethod,
                data_payload:Retired::NUMBER AS retired,
                data_payload:OverrideIntValue::NUMBER AS overrideintvalue,
                data_payload:PaymentPlan::NUMBER AS paymentplan,
                data_payload:Product::NUMBER AS product,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:WriteoffReason::NUMBER AS writeoffreason,
                data_payload:DelinquencyReason::NUMBER AS delinquencyreason,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ShouldCreateActivity::BOOLEAN AS shouldcreateactivity,
                data_payload:ProducerCode::NUMBER AS producercode,
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
            FROM {{ source('gwbc', 'bcx_bizruleoverrides_sp') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:feetype::NUMBER AS feetype,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:overridebit::BOOLEAN AS overridebit,
                $1:jurisdiction::NUMBER AS jurisdiction,
                $1:inuse::BOOLEAN AS inuse,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:minappamount_ext AS NUMBER(18,2)) AS minappamount_ext,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:minappamount_ext_cur::NUMBER AS minappamount_ext_cur,
                CAST($1:maximumpremiumamount AS NUMBER(18,2)) AS maximumpremiumamount,
                $1:maximumpremiumamount_cur::NUMBER AS maximumpremiumamount_cur,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:dayunittype::NUMBER AS dayunittype,
                $1:uwcompany::NUMBER AS uwcompany,
                $1:id::NUMBER AS id,
                $1:billingmethod::NUMBER AS billingmethod,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:paymentrevreason::NUMBER AS paymentrevreason,
                CAST($1:minimumpremiumamount AS NUMBER(18,2)) AS minimumpremiumamount,
                $1:offeringtype_ext::NUMBER AS offeringtype_ext,
                $1:minimumpremiumamount_cur::NUMBER AS minimumpremiumamount_cur,
                $1:createuserid::NUMBER AS createuserid,
                $1:leadtimetype::NUMBER AS leadtimetype,
                CAST($1:overrideamount AS NUMBER(18,2)) AS overrideamount,
                $1:disbursementreason::NUMBER AS disbursementreason,
                $1:thresholdtype::NUMBER AS thresholdtype,
                $1:overrideamount_cur::NUMBER AS overrideamount_cur,
                $1:isbackdated_ext::BOOLEAN AS isbackdated_ext,
                $1:paymentmethod::NUMBER AS paymentmethod,
                $1:beanversion::NUMBER AS beanversion,
                $1:lowbalancemethod::NUMBER AS lowbalancemethod,
                $1:retired::NUMBER AS retired,
                $1:overrideintvalue::NUMBER AS overrideintvalue,
                $1:paymentplan::NUMBER AS paymentplan,
                $1:product::NUMBER AS product,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:writeoffreason::NUMBER AS writeoffreason,
                $1:delinquencyreason::NUMBER AS delinquencyreason,
                $1:subtype::NUMBER AS subtype,
                $1:shouldcreateactivity::BOOLEAN AS shouldcreateactivity,
                $1:producercode::NUMBER AS producercode,
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
            FROM {{ source('gwbc', 'bcx_bizruleoverrides_sp') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS bizruleoverrides_sp_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'feetype',
                        'loadcommandid',
                        'overridebit',
                        'jurisdiction',
                        'inuse',
                        'publicid',
                        'minappamount_ext',
                        'createtime',
                        'minappamount_ext_cur',
                        'maximumpremiumamount',
                        'maximumpremiumamount_cur',
                        'effectivedate',
                        'updatetime',
                        'dayunittype',
                        'uwcompany',
                        'billingmethod',
                        'expirationdate',
                        'paymentrevreason',
                        'minimumpremiumamount',
                        'offeringtype_ext',
                        'minimumpremiumamount_cur',
                        'createuserid',
                        'leadtimetype',
                        'overrideamount',
                        'disbursementreason',
                        'thresholdtype',
                        'overrideamount_cur',
                        'isbackdated_ext',
                        'paymentmethod',
                        'beanversion',
                        'lowbalancemethod',
                        'retired',
                        'overrideintvalue',
                        'paymentplan',
                        'product',
                        'updateuserid',
                        'writeoffreason',
                        'delinquencyreason',
                        'subtype',
                        'shouldcreateactivity',
                        'producercode'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}