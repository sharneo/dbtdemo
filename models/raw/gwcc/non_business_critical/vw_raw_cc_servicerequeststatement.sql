
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
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:CheckID::NUMBER AS checkid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:ApprovedByID::NUMBER AS approvedbyid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:ApprovalDate::NUMBER/1000) AS approvaldate,
                CAST(data_payload:DeclinedReason::TEXT AS VARCHAR(16777216)) AS declinedreason,
                TO_TIMESTAMP_TZ(data_payload:PaymentDate::NUMBER/1000) AS paymentdate,
                CAST(data_payload:ReferenceNumber::TEXT AS VARCHAR(255)) AS referencenumber,
                data_payload:ServiceRequestID::NUMBER AS servicerequestid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:StatementCreationTime::NUMBER/1000) AS statementcreationtime,
                data_payload:Source::NUMBER AS source,
                CAST(data_payload:Description::TEXT AS VARCHAR(16777216)) AS description,
                data_payload:PaidByID::NUMBER AS paidbyid,
                data_payload:ExpectedDaysToPerformService::NUMBER AS expecteddaystoperformservice,
                data_payload:InvoiceStatus::NUMBER AS invoicestatus,
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
            FROM {{ source('gwcc', 'cc_servicerequeststatement') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:createuserid::NUMBER AS createuserid,
                $1:checkid::NUMBER AS checkid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:approvedbyid::NUMBER AS approvedbyid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:approvaldate::TIMESTAMP_TZ AS approvaldate,
                CAST($1:declinedreason::TEXT AS VARCHAR(16777216)) AS declinedreason,
                $1:paymentdate::TIMESTAMP_TZ AS paymentdate,
                CAST($1:referencenumber::TEXT AS VARCHAR(255)) AS referencenumber,
                $1:servicerequestid::NUMBER AS servicerequestid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                $1:statementcreationtime::TIMESTAMP_TZ AS statementcreationtime,
                $1:source::NUMBER AS source,
                CAST($1:description::TEXT AS VARCHAR(16777216)) AS description,
                $1:paidbyid::NUMBER AS paidbyid,
                $1:expecteddaystoperformservice::NUMBER AS expecteddaystoperformservice,
                $1:invoicestatus::NUMBER AS invoicestatus,
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
            FROM {{ source('gwcc', 'cc_servicerequeststatement') }}
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
                                'createuserid',
                        'checkid',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'approvedbyid',
                        'createtime',
                        'updateuserid',
                        'approvaldate',
                        'declinedreason',
                        'paymentdate',
                        'referencenumber',
                        'servicerequestid',
                        'updatetime',
                        'subtype',
                        'id',
                        'statementcreationtime',
                        'source',
                        'description',
                        'paidbyid',
                        'expecteddaystoperformservice',
                        'invoicestatus'
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
        