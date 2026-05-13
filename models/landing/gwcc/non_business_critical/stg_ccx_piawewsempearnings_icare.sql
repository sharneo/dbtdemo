{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_piawewsempearnings_icare.
                                                piawewsempearnings_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_piawewsempearnings_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                TO_TIMESTAMP_TZ(data_payload:PayPeriodEndDate::NUMBER/1000) AS payperiodenddate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:PayPeriodCount::NUMBER AS payperiodcount,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:TotalEarnings AS NUMBER(18,2)) AS totalearnings,
                CAST(data_payload:GrossEarnings AS NUMBER(18,2)) AS grossearnings,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:CompulsorySuperannuation AS NUMBER(18,2)) AS compulsorysuperannuation,
                CAST(data_payload:Other AS NUMBER(18,2)) AS other,
                CAST(data_payload:WorkersCompPayments AS NUMBER(18,2)) AS workerscomppayments,
                CAST(data_payload:HoursWorkedAndPaidLeave AS NUMBER(10,2)) AS hoursworkedandpaidleave,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:PIAWEWSEmpData_icareID::NUMBER AS piawewsempdata_icareid,
                CAST(data_payload:DiscretionaryPayments AS NUMBER(18,2)) AS discretionarypayments,
                data_payload:ID::NUMBER AS id,
                data_payload:PayPeriodStatus::NUMBER AS payperiodstatus,
                CAST(data_payload:UnpaidLeaveHours AS NUMBER(10,2)) AS unpaidleavehours,
                CAST(data_payload:RelevantPeriod AS NUMBER(15,7)) AS relevantperiod,
                TO_TIMESTAMP_TZ(data_payload:UnpaidLeaveEndDate::NUMBER/1000) AS unpaidleaveenddate,
                TO_TIMESTAMP_TZ(data_payload:UnpaidLeaveStartDate::NUMBER/1000) AS unpaidleavestartdate,
                data_payload:UnpaidLeaveDayCount::NUMBER AS unpaidleavedaycount,
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
            FROM {{ source('gwcc', 'ccx_piawewsempearnings_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:payperiodenddate::TIMESTAMP_TZ AS payperiodenddate,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:payperiodcount::NUMBER AS payperiodcount,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:totalearnings AS NUMBER(18,2)) AS totalearnings,
                CAST($1:grossearnings AS NUMBER(18,2)) AS grossearnings,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:compulsorysuperannuation AS NUMBER(18,2)) AS compulsorysuperannuation,
                CAST($1:other AS NUMBER(18,2)) AS other,
                CAST($1:workerscomppayments AS NUMBER(18,2)) AS workerscomppayments,
                CAST($1:hoursworkedandpaidleave AS NUMBER(10,2)) AS hoursworkedandpaidleave,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:piawewsempdata_icareid::NUMBER AS piawewsempdata_icareid,
                CAST($1:discretionarypayments AS NUMBER(18,2)) AS discretionarypayments,
                $1:id::NUMBER AS id,
                $1:payperiodstatus::NUMBER AS payperiodstatus,
                CAST($1:unpaidleavehours AS NUMBER(10,2)) AS unpaidleavehours,
                CAST($1:relevantperiod AS NUMBER(15,7)) AS relevantperiod,
                $1:unpaidleaveenddate::TIMESTAMP_TZ AS unpaidleaveenddate,
                $1:unpaidleavestartdate::TIMESTAMP_TZ AS unpaidleavestartdate,
                $1:unpaidleavedaycount::NUMBER AS unpaidleavedaycount,
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
            FROM {{ source('gwcc', 'ccx_piawewsempearnings_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS piawewsempearnings_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'payperiodenddate',
                        'createuserid',
                        'publicid',
                        'payperiodcount',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'totalearnings',
                        'grossearnings',
                        'updateuserid',
                        'compulsorysuperannuation',
                        'other',
                        'workerscomppayments',
                        'hoursworkedandpaidleave',
                        'updatetime',
                        'piawewsempdata_icareid',
                        'discretionarypayments',
                        'payperiodstatus',
                        'unpaidleavehours',
                        'relevantperiod',
                        'unpaidleaveenddate',
                        'unpaidleavestartdate',
                        'unpaidleavedaycount'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}