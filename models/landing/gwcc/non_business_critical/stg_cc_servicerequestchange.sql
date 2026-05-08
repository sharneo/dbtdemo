{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_servicerequestchange.
                                                servicerequestchange_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "cc_servicerequestchange"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:Sequence::NUMBER AS sequence,
                TO_TIMESTAMP_TZ(data_payload:new_expservicecompletiondate::NUMBER/1000) AS new_expservicecompletiondate,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:new_expquotecompletiondate::NUMBER/1000) AS new_expquotecompletiondate,
                data_payload:NewInstructionID::NUMBER AS newinstructionid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:InitiatorID::NUMBER AS initiatorid,
                data_payload:New_QuoteStatus::NUMBER AS new_quotestatus,
                data_payload:Progress_Chg::BOOLEAN AS progress_chg,
                data_payload:QuoteStatus_Chg::BOOLEAN AS quotestatus_chg,
                data_payload:ServiceRequestID::NUMBER AS servicerequestid,
                TO_TIMESTAMP_TZ(data_payload:Timestamp::NUMBER/1000) AS timestamp,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:expservicecompletiondate_chg::BOOLEAN AS expservicecompletiondate_chg,
                data_payload:Operation::NUMBER AS operation,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:New_Progress::NUMBER AS new_progress,
                data_payload:expquotecompletiondate_chg::BOOLEAN AS expquotecompletiondate_chg,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Instruction_Chg::BOOLEAN AS instruction_chg,
                CAST(data_payload:Description::TEXT AS VARCHAR(16777216)) AS description,
                data_payload:RelatedStatementID::NUMBER AS relatedstatementid,
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
            FROM {{ source('gwcc', 'cc_servicerequestchange') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:sequence::NUMBER AS sequence,
                $1:new_expservicecompletiondate::TIMESTAMP_TZ AS new_expservicecompletiondate,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:new_expquotecompletiondate::TIMESTAMP_TZ AS new_expquotecompletiondate,
                $1:newinstructionid::NUMBER AS newinstructionid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:initiatorid::NUMBER AS initiatorid,
                $1:new_quotestatus::NUMBER AS new_quotestatus,
                $1:progress_chg::BOOLEAN AS progress_chg,
                $1:quotestatus_chg::BOOLEAN AS quotestatus_chg,
                $1:servicerequestid::NUMBER AS servicerequestid,
                $1:timestamp::TIMESTAMP_TZ AS timestamp,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                $1:expservicecompletiondate_chg::BOOLEAN AS expservicecompletiondate_chg,
                $1:operation::NUMBER AS operation,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:new_progress::NUMBER AS new_progress,
                $1:expquotecompletiondate_chg::BOOLEAN AS expquotecompletiondate_chg,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:instruction_chg::BOOLEAN AS instruction_chg,
                CAST($1:description::TEXT AS VARCHAR(16777216)) AS description,
                $1:relatedstatementid::NUMBER AS relatedstatementid,
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
            FROM {{ source('gwcc', 'cc_servicerequestchange') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS servicerequestchange_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'sequence',
                        'new_expservicecompletiondate',
                        'publicid',
                        'new_expquotecompletiondate',
                        'newinstructionid',
                        'createtime',
                        'initiatorid',
                        'new_quotestatus',
                        'progress_chg',
                        'quotestatus_chg',
                        'servicerequestid',
                        'timestamp',
                        'updatetime',
                        'createuserid',
                        'expservicecompletiondate_chg',
                        'operation',
                        'archivepartition',
                        'beanversion',
                        'new_progress',
                        'expquotecompletiondate_chg',
                        'updateuserid',
                        'instruction_chg',
                        'description',
                        'relatedstatementid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}