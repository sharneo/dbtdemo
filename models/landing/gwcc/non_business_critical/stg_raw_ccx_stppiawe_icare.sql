{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_stppiawe_icare.
                                                stppiawe_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "ccx_stppiawe_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:PecuniaryBenefits::BOOLEAN AS pecuniarybenefits,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:RAAmountReasonablyPaid AS NUMBER(18,2)) AS raamountreasonablypaid,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(19)) AS claimnumber,
                data_payload:TypeofLeave::BOOLEAN AS typeofleave,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:HIFringeBenefitValue::NUMBER AS hifringebenefitvalue,
                data_payload:WCBenefitwithin52weeks::BOOLEAN AS wcbenefitwithin52weeks,
                data_payload:ResidentialAccommodation::BOOLEAN AS residentialaccommodation,
                data_payload:RANumberofWeeks::NUMBER AS ranumberofweeks,
                CAST(data_payload:OrdinaryHoursWorkedperweek AS NUMBER(9,6)) AS ordinaryhoursworkedperweek,
                data_payload:EducationFees::BOOLEAN AS educationfees,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:MVNumberofWeeks::NUMBER AS mvnumberofweeks,
                CAST(data_payload:HIAmountReasonablyPaid AS NUMBER(18,2)) AS hiamountreasonablypaid,
                CAST(data_payload:DocUID::TEXT AS VARCHAR(20)) AS docuid,
                data_payload:WorkerDuetobePromoted::BOOLEAN AS workerduetobepromoted,
                data_payload:UseofaMotorVehicle::BOOLEAN AS useofamotorvehicle,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:ShiftAllowances AS NUMBER(18,2)) AS shiftallowances,
                data_payload:EFNumberofWeeks::NUMBER AS efnumberofweeks,
                TO_TIMESTAMP_TZ(data_payload:HIDateCommenced::NUMBER/1000) AS hidatecommenced,
                data_payload:Leavewithin52weeks::BOOLEAN AS leavewithin52weeks,
                CAST(data_payload:Overtime AS NUMBER(18,2)) AS overtime,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PieceRates AS NUMBER(18,2)) AS piecerates,
                CAST(data_payload:OrdinaryGrossEarningsPerWeek AS NUMBER(18,2)) AS ordinarygrossearningsperweek,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:EFFringeBenefitValue::NUMBER AS effringebenefitvalue,
                CAST(data_payload:Commission AS NUMBER(18,2)) AS commission,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:RADateCommenced::NUMBER/1000) AS radatecommenced,
                data_payload:MVFringeBenefitValue::NUMBER AS mvfringebenefitvalue,
                TO_TIMESTAMP_TZ(data_payload:DateofChange::NUMBER/1000) AS dateofchange,
                data_payload:ChgtoWorkhrsRateofPay::BOOLEAN AS chgtoworkhrsrateofpay,
                TO_TIMESTAMP_TZ(data_payload:MVDateCommenced::NUMBER/1000) AS mvdatecommenced,
                CAST(data_payload:EFAmountReasonablyPaid AS NUMBER(18,2)) AS efamountreasonablypaid,
                data_payload:RAFringeBenefitValue::NUMBER AS rafringebenefitvalue,
                data_payload:OtherEmployment::BOOLEAN AS otheremployment,
                data_payload:UnpaidWeekswithin52weeks::BOOLEAN AS unpaidweekswithin52weeks,
                TO_TIMESTAMP_TZ(data_payload:EFDateCommenced::NUMBER/1000) AS efdatecommenced,
                data_payload:HINumberofWeeks::NUMBER AS hinumberofweeks,
                data_payload:HealthInsurance::BOOLEAN AS healthinsurance,
                TO_TIMESTAMP_TZ(data_payload:DateofPromotion::NUMBER/1000) AS dateofpromotion,
                CAST(data_payload:MVAmountReasonablyPaid AS NUMBER(18,2)) AS mvamountreasonablypaid,
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
            FROM {{ source('gwcc', 'ccx_stppiawe_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:pecuniarybenefits::BOOLEAN AS pecuniarybenefits,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:raamountreasonablypaid AS NUMBER(18,2)) AS raamountreasonablypaid,
                CAST($1:claimnumber::TEXT AS VARCHAR(19)) AS claimnumber,
                $1:typeofleave::BOOLEAN AS typeofleave,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:hifringebenefitvalue::NUMBER AS hifringebenefitvalue,
                $1:wcbenefitwithin52weeks::BOOLEAN AS wcbenefitwithin52weeks,
                $1:residentialaccommodation::BOOLEAN AS residentialaccommodation,
                $1:ranumberofweeks::NUMBER AS ranumberofweeks,
                CAST($1:ordinaryhoursworkedperweek AS NUMBER(9,6)) AS ordinaryhoursworkedperweek,
                $1:educationfees::BOOLEAN AS educationfees,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:mvnumberofweeks::NUMBER AS mvnumberofweeks,
                CAST($1:hiamountreasonablypaid AS NUMBER(18,2)) AS hiamountreasonablypaid,
                CAST($1:docuid::TEXT AS VARCHAR(20)) AS docuid,
                $1:workerduetobepromoted::BOOLEAN AS workerduetobepromoted,
                $1:useofamotorvehicle::BOOLEAN AS useofamotorvehicle,
                $1:id::NUMBER AS id,
                CAST($1:shiftallowances AS NUMBER(18,2)) AS shiftallowances,
                $1:efnumberofweeks::NUMBER AS efnumberofweeks,
                $1:hidatecommenced::TIMESTAMP_TZ AS hidatecommenced,
                $1:leavewithin52weeks::BOOLEAN AS leavewithin52weeks,
                CAST($1:overtime AS NUMBER(18,2)) AS overtime,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:piecerates AS NUMBER(18,2)) AS piecerates,
                CAST($1:ordinarygrossearningsperweek AS NUMBER(18,2)) AS ordinarygrossearningsperweek,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:effringebenefitvalue::NUMBER AS effringebenefitvalue,
                CAST($1:commission AS NUMBER(18,2)) AS commission,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:radatecommenced::TIMESTAMP_TZ AS radatecommenced,
                $1:mvfringebenefitvalue::NUMBER AS mvfringebenefitvalue,
                $1:dateofchange::TIMESTAMP_TZ AS dateofchange,
                $1:chgtoworkhrsrateofpay::BOOLEAN AS chgtoworkhrsrateofpay,
                $1:mvdatecommenced::TIMESTAMP_TZ AS mvdatecommenced,
                CAST($1:efamountreasonablypaid AS NUMBER(18,2)) AS efamountreasonablypaid,
                $1:rafringebenefitvalue::NUMBER AS rafringebenefitvalue,
                $1:otheremployment::BOOLEAN AS otheremployment,
                $1:unpaidweekswithin52weeks::BOOLEAN AS unpaidweekswithin52weeks,
                $1:efdatecommenced::TIMESTAMP_TZ AS efdatecommenced,
                $1:hinumberofweeks::NUMBER AS hinumberofweeks,
                $1:healthinsurance::BOOLEAN AS healthinsurance,
                $1:dateofpromotion::TIMESTAMP_TZ AS dateofpromotion,
                CAST($1:mvamountreasonablypaid AS NUMBER(18,2)) AS mvamountreasonablypaid,
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
            FROM {{ source('gwcc', 'ccx_stppiawe_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS stppiawe_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'pecuniarybenefits',
                        'publicid',
                        'raamountreasonablypaid',
                        'claimnumber',
                        'typeofleave',
                        'createtime',
                        'hifringebenefitvalue',
                        'wcbenefitwithin52weeks',
                        'residentialaccommodation',
                        'ranumberofweeks',
                        'ordinaryhoursworkedperweek',
                        'educationfees',
                        'updatetime',
                        'mvnumberofweeks',
                        'hiamountreasonablypaid',
                        'docuid',
                        'workerduetobepromoted',
                        'useofamotorvehicle',
                        'shiftallowances',
                        'efnumberofweeks',
                        'hidatecommenced',
                        'leavewithin52weeks',
                        'overtime',
                        'createuserid',
                        'piecerates',
                        'ordinarygrossearningsperweek',
                        'beanversion',
                        'retired',
                        'effringebenefitvalue',
                        'commission',
                        'updateuserid',
                        'radatecommenced',
                        'mvfringebenefitvalue',
                        'dateofchange',
                        'chgtoworkhrsrateofpay',
                        'mvdatecommenced',
                        'efamountreasonablypaid',
                        'rafringebenefitvalue',
                        'otheremployment',
                        'unpaidweekswithin52weeks',
                        'efdatecommenced',
                        'hinumberofweeks',
                        'healthinsurance',
                        'dateofpromotion',
                        'mvamountreasonablypaid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}