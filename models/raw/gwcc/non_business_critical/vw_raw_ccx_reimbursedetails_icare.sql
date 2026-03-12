
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This Converts Parquet or AVRO Data Loaded in the Variant Column in the RAW DB into Flattend Views
                                                This also creates a HASH_KEY for Incremental Tables for the Curated Layer 
                                                Additional CDA Files are Null in the AVRO but not in CDA .
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    tags=["raw_gwcc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ExposureID::NUMBER AS exposureid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:EndDate::NUMBER/1000) AS enddate,
                data_payload:NoEarning::BOOLEAN AS noearning,
                CAST(data_payload:OrdinaryEarnings AS NUMBER(18,2)) AS ordinaryearnings,
                data_payload:ReimburseSchedule_icare::NUMBER AS reimburseschedule_icare,
                CAST(data_payload:Deductions AS NUMBER(18,2)) AS deductions,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:StartDate::NUMBER/1000) AS startdate,
                CAST(data_payload:ReimbursementAmount AS NUMBER(18,2)) AS reimbursementamount,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:ShiftAndOvertime AS NUMBER(7,2)) AS shiftandovertime,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:HoursWorked AS NUMBER(5,2)) AS hoursworked,
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
            FROM {{ source('gwcc', 'ccx_reimbursedetails_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:exposureid::NUMBER AS exposureid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:enddate::TIMESTAMP_TZ AS enddate,
                $1:noearning::BOOLEAN AS noearning,
                CAST($1:ordinaryearnings AS NUMBER(18,2)) AS ordinaryearnings,
                $1:reimburseschedule_icare::NUMBER AS reimburseschedule_icare,
                CAST($1:deductions AS NUMBER(18,2)) AS deductions,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:startdate::TIMESTAMP_TZ AS startdate,
                CAST($1:reimbursementamount AS NUMBER(18,2)) AS reimbursementamount,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:shiftandovertime AS NUMBER(7,2)) AS shiftandovertime,
                $1:id::NUMBER AS id,
                CAST($1:hoursworked AS NUMBER(5,2)) AS hoursworked,
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
            FROM {{ source('gwcc', 'ccx_reimbursedetails_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),
{#-
    Driving CTE Over 
    Transformed CTE is To Create the HASH_KEY Based on the Right Combination
-#}   
cte_transformed AS (
    SELECT
        *,
        CASE
             WHEN file_type = 'AVRO' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'loadcommandid',
                        'createuserid',
                        'exposureid',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'enddate',
                        'noearning',
                        'ordinaryearnings',
                        'reimburseschedule_icare',
                        'deductions',
                        'updateuserid',
                        'startdate',
                        'reimbursementamount',
                        'updatetime',
                        'shiftandovertime',
                        'id',
                        'hoursworked'
                        ]) }}
            WHEN file_type = 'PARQUET' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'id',
                        'gwcbi_seqval'
                        ]) }}
        END AS hash_key    
    FROM cte_source_data
)
SELECT * FROM cte_transformed
        