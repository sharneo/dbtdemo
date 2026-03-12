
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
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                CAST(data_payload:CompanyName::TEXT AS VARCHAR(255)) AS companyname,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:EndDate::NUMBER/1000) AS enddate,
                CAST(data_payload:WeeklyWage AS NUMBER(18,2)) AS weeklywage,
                CAST(data_payload:ContactNumber::TEXT AS VARCHAR(60)) AS contactnumber,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:StartDate::NUMBER/1000) AS startdate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:IncludedInCalc::BOOLEAN AS includedincalc,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:NumHoursWorked::NUMBER AS numhoursworked,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:JobTitle::TEXT AS VARCHAR(60)) AS jobtitle,
                data_payload:FullTime::BOOLEAN AS fulltime,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
                CAST(data_payload:NoOfHoursWorked_icare AS NUMBER(10,2)) AS noofhoursworked_icare,
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
            FROM {{ source('gwcc', 'cc_concurrentempl') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                CAST($1:companyname::TEXT AS VARCHAR(255)) AS companyname,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:enddate::TIMESTAMP_TZ AS enddate,
                CAST($1:weeklywage AS NUMBER(18,2)) AS weeklywage,
                CAST($1:contactnumber::TEXT AS VARCHAR(60)) AS contactnumber,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:startdate::TIMESTAMP_TZ AS startdate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:includedincalc::BOOLEAN AS includedincalc,
                $1:claimid::NUMBER AS claimid,
                $1:numhoursworked::NUMBER AS numhoursworked,
                $1:id::NUMBER AS id,
                CAST($1:jobtitle::TEXT AS VARCHAR(60)) AS jobtitle,
                $1:fulltime::BOOLEAN AS fulltime,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
                CAST($1:noofhoursworked_icare AS NUMBER(10,2)) AS noofhoursworked_icare,
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
            FROM {{ source('gwcc', 'cc_concurrentempl') }}
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
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'companyname',
                        'createtime',
                        'retired',
                        'enddate',
                        'weeklywage',
                        'contactnumber',
                        'updateuserid',
                        'startdate',
                        'updatetime',
                        'includedincalc',
                        'claimid',
                        'numhoursworked',
                        'id',
                        'jobtitle',
                        'fulltime',
                        'description',
                        'noofhoursworked_icare'
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
        