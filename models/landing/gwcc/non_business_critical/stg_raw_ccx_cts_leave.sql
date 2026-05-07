{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_cts_leave.
                                                cts_leave_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "ccx_cts_leave"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:EndDate::NUMBER/1000) AS enddate,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:OrdinaryEarnings AS NUMBER(18,2)) AS ordinaryearnings,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:StartDate::NUMBER/1000) AS startdate,
                data_payload:LeaveType::NUMBER AS leavetype,
                data_payload:EmploymentDataID::NUMBER AS employmentdataid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:ShiftAllowance AS NUMBER(18,2)) AS shiftallowance,
                CAST(data_payload:OvertimeAmount AS NUMBER(18,2)) AS overtimeamount,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                data_payload:Weeks::NUMBER AS weeks,
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
            FROM {{ source('gwcc', 'ccx_cts_leave') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:enddate::TIMESTAMP_TZ AS enddate,
                $1:retired::NUMBER AS retired,
                CAST($1:ordinaryearnings AS NUMBER(18,2)) AS ordinaryearnings,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:startdate::TIMESTAMP_TZ AS startdate,
                $1:leavetype::NUMBER AS leavetype,
                $1:employmentdataid::NUMBER AS employmentdataid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:shiftallowance AS NUMBER(18,2)) AS shiftallowance,
                CAST($1:overtimeamount AS NUMBER(18,2)) AS overtimeamount,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                $1:weeks::NUMBER AS weeks,
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
            FROM {{ source('gwcc', 'ccx_cts_leave') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS cts_leave_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'enddate',
                        'retired',
                        'ordinaryearnings',
                        'updateuserid',
                        'startdate',
                        'leavetype',
                        'employmentdataid',
                        'updatetime',
                        'shiftallowance',
                        'overtimeamount',
                        'subtype',
                        'weeks'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}