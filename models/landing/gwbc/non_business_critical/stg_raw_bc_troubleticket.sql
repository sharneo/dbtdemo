{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_troubleticket.
                                                troubleticket_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_billing_centre", "billing_centre", "non_business_critical", "bc_troubleticket"]
) }}


WITH cte_source_data AS 
(

            SELECT
                TO_TIMESTAMP_TZ(data_payload:EscalationDate::NUMBER/1000) AS escalationdate,
                data_payload:PreviousGroupID::NUMBER AS previousgroupid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:TicketType::NUMBER AS tickettype,
                CAST(data_payload:TroubleTicketNumberDenorm::TEXT AS VARCHAR(255)) AS troubleticketnumberdenorm,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:AssignedByUserID::NUMBER AS assignedbyuserid,
                CAST(data_payload:TroubleTicketNumber::TEXT AS VARCHAR(255)) AS troubleticketnumber,
                CAST(data_payload:DetailedDescription::TEXT AS VARCHAR(1333)) AS detaileddescription,
                data_payload:AssignedGroupID::NUMBER AS assignedgroupid,
                CAST(data_payload:TitleDenorm::TEXT AS VARCHAR(255)) AS titledenorm,
                data_payload:PreviousQueueID::NUMBER AS previousqueueid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Title::TEXT AS VARCHAR(255)) AS title,
                data_payload:ID::NUMBER AS id,
                data_payload:PreviousUserID::NUMBER AS previoususerid,
                data_payload:CloseUserID::NUMBER AS closeuserid,
                data_payload:AssignedQueueID::NUMBER AS assignedqueueid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:Priority::NUMBER AS priority,
                TO_TIMESTAMP_TZ(data_payload:CloseDate::NUMBER/1000) AS closedate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Escalated::BOOLEAN AS escalated,
                data_payload:AssignedUserID::NUMBER AS assigneduserid,
                TO_TIMESTAMP_TZ(data_payload:AssignmentDate::NUMBER/1000) AS assignmentdate,
                TO_TIMESTAMP_TZ(data_payload:TargetDate::NUMBER/1000) AS targetdate,
                data_payload:AssignmentStatus::NUMBER AS assignmentstatus,
                data_payload:ReviewDelinquencyProcess_EXT::BOOLEAN AS reviewdelinquencyprocess_ext,
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
            FROM {{ source('gwbc', 'bc_troubleticket') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:escalationdate::TIMESTAMP_TZ AS escalationdate,
                $1:previousgroupid::NUMBER AS previousgroupid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:tickettype::NUMBER AS tickettype,
                CAST($1:troubleticketnumberdenorm::TEXT AS VARCHAR(255)) AS troubleticketnumberdenorm,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:assignedbyuserid::NUMBER AS assignedbyuserid,
                CAST($1:troubleticketnumber::TEXT AS VARCHAR(255)) AS troubleticketnumber,
                CAST($1:detaileddescription::TEXT AS VARCHAR(1333)) AS detaileddescription,
                $1:assignedgroupid::NUMBER AS assignedgroupid,
                CAST($1:titledenorm::TEXT AS VARCHAR(255)) AS titledenorm,
                $1:previousqueueid::NUMBER AS previousqueueid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:title::TEXT AS VARCHAR(255)) AS title,
                $1:id::NUMBER AS id,
                $1:previoususerid::NUMBER AS previoususerid,
                $1:closeuserid::NUMBER AS closeuserid,
                $1:assignedqueueid::NUMBER AS assignedqueueid,
                $1:createuserid::NUMBER AS createuserid,
                $1:priority::NUMBER AS priority,
                $1:closedate::TIMESTAMP_TZ AS closedate,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:escalated::BOOLEAN AS escalated,
                $1:assigneduserid::NUMBER AS assigneduserid,
                $1:assignmentdate::TIMESTAMP_TZ AS assignmentdate,
                $1:targetdate::TIMESTAMP_TZ AS targetdate,
                $1:assignmentstatus::NUMBER AS assignmentstatus,
                $1:reviewdelinquencyprocess_ext::BOOLEAN AS reviewdelinquencyprocess_ext,
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
            FROM {{ source('gwbc', 'bc_troubleticket') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS troubleticket_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'escalationdate',
                        'previousgroupid',
                        'publicid',
                        'tickettype',
                        'troubleticketnumberdenorm',
                        'createtime',
                        'assignedbyuserid',
                        'troubleticketnumber',
                        'detaileddescription',
                        'assignedgroupid',
                        'titledenorm',
                        'previousqueueid',
                        'updatetime',
                        'title',
                        'previoususerid',
                        'closeuserid',
                        'assignedqueueid',
                        'createuserid',
                        'priority',
                        'closedate',
                        'beanversion',
                        'retired',
                        'updateuserid',
                        'escalated',
                        'assigneduserid',
                        'assignmentdate',
                        'targetdate',
                        'assignmentstatus',
                        'reviewdelinquencyprocess_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}