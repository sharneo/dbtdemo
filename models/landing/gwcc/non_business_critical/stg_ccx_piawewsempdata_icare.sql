{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_piawewsempdata_icare.
                                                piawewsempdata_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_piawewsempdata_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:MaterialChangeDate::NUMBER/1000) AS materialchangedate,
                CAST(data_payload:MaterialChangeDescription::TEXT AS VARCHAR(16777216)) AS materialchangedescription,
                CAST(data_payload:EmployerName::TEXT AS VARCHAR(255)) AS employername,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:AvgWeeklyHoursWorked AS NUMBER(10,2)) AS avgweeklyhoursworked,
                CAST(data_payload:UnpaidLeaveWeeks AS NUMBER(10,2)) AS unpaidleaveweeks,
                CAST(data_payload:RelevantEarningPeriod AS NUMBER(10,2)) AS relevantearningperiod,
                CAST(data_payload:GrossPIAWE AS NUMBER(18,2)) AS grosspiawe,
                TO_TIMESTAMP_TZ(data_payload:LastPPEPriorInjury::NUMBER/1000) AS lastppepriorinjury,
                TO_TIMESTAMP_TZ(data_payload:RPStartDate::NUMBER/1000) AS rpstartdate,
                data_payload:IsPrimaryEmployer::BOOLEAN AS isprimaryemployer,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:MaterialChange::NUMBER AS materialchange,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:EmploymentStartDate::NUMBER/1000) AS employmentstartdate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:PIAWE_icareID::NUMBER AS piawe_icareid,
                CAST(data_payload:GrossEarnings AS NUMBER(18,2)) AS grossearnings,
                CAST(data_payload:TotalEarnings AS NUMBER(18,2)) AS totalearnings,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:MissingPaySlips::NUMBER AS missingpayslips,
                data_payload:EmploymentDataID::NUMBER AS employmentdataid,
                CAST(data_payload:ExcludedEarnings AS NUMBER(18,2)) AS excludedearnings,
                CAST(data_payload:TotalWeeks AS NUMBER(10,2)) AS totalweeks,
                data_payload:PayCycle::NUMBER AS paycycle,
                data_payload:WorksheetManualEntry::BOOLEAN AS worksheetmanualentry,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_piawewsempdata_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:materialchangedate::TIMESTAMP_TZ AS materialchangedate,
                CAST($1:materialchangedescription::TEXT AS VARCHAR(16777216)) AS materialchangedescription,
                CAST($1:employername::TEXT AS VARCHAR(255)) AS employername,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:avgweeklyhoursworked AS NUMBER(10,2)) AS avgweeklyhoursworked,
                CAST($1:unpaidleaveweeks AS NUMBER(10,2)) AS unpaidleaveweeks,
                CAST($1:relevantearningperiod AS NUMBER(10,2)) AS relevantearningperiod,
                CAST($1:grosspiawe AS NUMBER(18,2)) AS grosspiawe,
                $1:lastppepriorinjury::TIMESTAMP_TZ AS lastppepriorinjury,
                $1:rpstartdate::TIMESTAMP_TZ AS rpstartdate,
                $1:isprimaryemployer::BOOLEAN AS isprimaryemployer,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:materialchange::NUMBER AS materialchange,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                $1:employmentstartdate::TIMESTAMP_TZ AS employmentstartdate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:piawe_icareid::NUMBER AS piawe_icareid,
                CAST($1:grossearnings AS NUMBER(18,2)) AS grossearnings,
                CAST($1:totalearnings AS NUMBER(18,2)) AS totalearnings,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:missingpayslips::NUMBER AS missingpayslips,
                $1:employmentdataid::NUMBER AS employmentdataid,
                CAST($1:excludedearnings AS NUMBER(18,2)) AS excludedearnings,
                CAST($1:totalweeks AS NUMBER(10,2)) AS totalweeks,
                $1:paycycle::NUMBER AS paycycle,
                $1:worksheetmanualentry::BOOLEAN AS worksheetmanualentry,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_piawewsempdata_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS piawewsempdata_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'materialchangedate',
                        'materialchangedescription',
                        'employername',
                        'createtime',
                        'avgweeklyhoursworked',
                        'unpaidleaveweeks',
                        'relevantearningperiod',
                        'grosspiawe',
                        'lastppepriorinjury',
                        'rpstartdate',
                        'isprimaryemployer',
                        'updatetime',
                        'materialchange',
                        'createuserid',
                        'employmentstartdate',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'piawe_icareid',
                        'grossearnings',
                        'totalearnings',
                        'updateuserid',
                        'missingpayslips',
                        'employmentdataid',
                        'excludedearnings',
                        'totalweeks',
                        'paycycle',
                        'worksheetmanualentry'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}