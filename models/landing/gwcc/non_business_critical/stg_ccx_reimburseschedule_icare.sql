{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_reimburseschedule_icare.
                                                reimburseschedule_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_reimburseschedule_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:DocumentIdentifier::TEXT AS VARCHAR(255)) AS documentidentifier,
                data_payload:Exposure::NUMBER AS exposure,
                data_payload:ReimburseStatus_icare::NUMBER AS reimbursestatus_icare,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:ReceivedDate::NUMBER/1000) AS receiveddate,
                CAST(data_payload:EarningsInPeriod AS NUMBER(7,2)) AS earningsinperiod,
                CAST(data_payload:NonPecuniaryBenefits AS NUMBER(7,2)) AS nonpecuniarybenefits,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:NormalHoursWorkedPerWeek AS NUMBER(5,2)) AS normalhoursworkedperweek,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:RejectReason::TEXT AS VARCHAR(255)) AS rejectreason,
                CAST(data_payload:RejectionComments::TEXT AS VARCHAR(100)) AS rejectioncomments,
                CAST(data_payload:WRSInvoiceNumber::TEXT AS VARCHAR(255)) AS wrsinvoicenumber,
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
            FROM {{ source('gwcc', 'ccx_reimburseschedule_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:documentidentifier::TEXT AS VARCHAR(255)) AS documentidentifier,
                $1:exposure::NUMBER AS exposure,
                $1:reimbursestatus_icare::NUMBER AS reimbursestatus_icare,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:receiveddate::TIMESTAMP_TZ AS receiveddate,
                CAST($1:earningsinperiod AS NUMBER(7,2)) AS earningsinperiod,
                CAST($1:nonpecuniarybenefits AS NUMBER(7,2)) AS nonpecuniarybenefits,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:normalhoursworkedperweek AS NUMBER(5,2)) AS normalhoursworkedperweek,
                $1:id::NUMBER AS id,
                CAST($1:rejectreason::TEXT AS VARCHAR(255)) AS rejectreason,
                CAST($1:rejectioncomments::TEXT AS VARCHAR(100)) AS rejectioncomments,
                CAST($1:wrsinvoicenumber::TEXT AS VARCHAR(255)) AS wrsinvoicenumber,
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
            FROM {{ source('gwcc', 'ccx_reimburseschedule_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS reimburseschedule_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'documentidentifier',
                        'exposure',
                        'reimbursestatus_icare',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'createtime',
                        'updateuserid',
                        'receiveddate',
                        'earningsinperiod',
                        'nonpecuniarybenefits',
                        'updatetime',
                        'normalhoursworkedperweek',
                        'rejectreason',
                        'rejectioncomments',
                        'wrsinvoicenumber'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}