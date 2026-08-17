{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_liabilitystatushist_icare.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwcc", "claim_centre", "business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:LiabilityStatusDecisionDate::NUMBER/1000) AS liabilitystatusdecisiondate,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:LiabilityStatusDate::NUMBER/1000) AS liabilitystatusdate,
                CAST(data_payload:RefID::TEXT AS VARCHAR(255)) AS refid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:WeeklyBenefitEndDate::NUMBER/1000) AS weeklybenefitenddate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimWorkCompID::NUMBER AS claimworkcompid,
                data_payload:NoticePeriod::NUMBER AS noticeperiod,
                data_payload:ID::NUMBER AS id,
                data_payload:LiabilityStatus::NUMBER AS liabilitystatus,
                data_payload:ProvisionalWeeks::NUMBER AS provisionalweeks,
                TO_TIMESTAMP_TZ(data_payload:CTMLiabilityStatusDecisionDate::NUMBER/1000) AS ctmliabilitystatusdecisiondate,
                TO_TIMESTAMP_TZ(data_payload:MedicalBenefitEndDate::NUMBER/1000) AS medicalbenefitenddate,
                data_payload:PostalRule::NUMBER AS postalrule,
                TO_TIMESTAMP_TZ(data_payload:LegacyCreateTime::NUMBER/1000) AS legacycreatetime,
                TO_TIMESTAMP_TZ(data_payload:LegacyUpdateTime::NUMBER/1000) AS legacyupdatetime,
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
            FROM {{ source('gwcc', 'ccx_liabilitystatushist_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:liabilitystatusdecisiondate::TIMESTAMP_TZ AS liabilitystatusdecisiondate,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:liabilitystatusdate::TIMESTAMP_TZ AS liabilitystatusdate,
                CAST($1:refid::TEXT AS VARCHAR(255)) AS refid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:weeklybenefitenddate::TIMESTAMP_TZ AS weeklybenefitenddate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimworkcompid::NUMBER AS claimworkcompid,
                $1:noticeperiod::NUMBER AS noticeperiod,
                $1:id::NUMBER AS id,
                $1:liabilitystatus::NUMBER AS liabilitystatus,
                $1:provisionalweeks::NUMBER AS provisionalweeks,
                $1:ctmliabilitystatusdecisiondate::TIMESTAMP_TZ AS ctmliabilitystatusdecisiondate,
                $1:medicalbenefitenddate::TIMESTAMP_TZ AS medicalbenefitenddate,
                $1:postalrule::NUMBER AS postalrule,
                $1:legacycreatetime::TIMESTAMP_TZ AS legacycreatetime,
                $1:legacyupdatetime::TIMESTAMP_TZ AS legacyupdatetime,
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
            FROM {{ source('gwcc', 'ccx_liabilitystatushist_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS liabilitystatushist_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'liabilitystatusdecisiondate',
                        'retired',
                        'createtime',
                        'liabilitystatusdate',
                        'refid',
                        'updateuserid',
                        'weeklybenefitenddate',
                        'updatetime',
                        'claimworkcompid',
                        'noticeperiod',
                        'liabilitystatus',
                        'provisionalweeks',
                        'ctmliabilitystatusdecisiondate',
                        'medicalbenefitenddate',
                        'postalrule',
                        'legacycreatetime',
                        'legacyupdatetime'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date
