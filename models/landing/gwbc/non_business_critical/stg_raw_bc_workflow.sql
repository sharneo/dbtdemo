{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_workflow.
                                                workflow_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_billing_centre", "billing_centre", "non_business_critical", "bc_workflow"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:PreviousGroupID::NUMBER AS previousgroupid,
                TO_TIMESTAMP_TZ(data_payload:EnteredStep::NUMBER/1000) AS enteredstep,
                CAST(data_payload:PreviousStep::TEXT AS VARCHAR(255)) AS previousstep,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:CurrentStep::TEXT AS VARCHAR(255)) AS currentstep,
                data_payload:TopLevelDelinquencyProcessID::NUMBER AS topleveldelinquencyprocessid,
                data_payload:MessageHistoryID::NUMBER AS messagehistoryid,
                data_payload:ProcessVersion::NUMBER AS processversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Handler::NUMBER AS handler,
                data_payload:AssignedByUserID::NUMBER AS assignedbyuserid,
                data_payload:ActiveState::NUMBER AS activestate,
                data_payload:AssignedGroupID::NUMBER AS assignedgroupid,
                data_payload:LogEntryCounter::NUMBER AS logentrycounter,
                data_payload:State::NUMBER AS state,
                data_payload:PreviousQueueID::NUMBER AS previousqueueid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:CurrentBranch::TEXT AS VARCHAR(255)) AS currentbranch,
                data_payload:PreviousUserID::NUMBER AS previoususerid,
                data_payload:AssignedQueueID::NUMBER AS assignedqueueid,
                TO_TIMESTAMP_TZ(data_payload:TestTime::NUMBER/1000) AS testtime,
                data_payload:MessageID::NUMBER AS messageid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:CloseDate::NUMBER/1000) AS closedate,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:AgencyCycleProcessID::NUMBER AS agencycycleprocessid,
                data_payload:Retired::NUMBER AS retired,
                data_payload:StepExecTime::NUMBER AS stepexectime,
                TO_TIMESTAMP_TZ(data_payload:TimeoutTime::NUMBER/1000) AS timeouttime,
                data_payload:CurrentAction::NUMBER AS currentaction,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:AssignedUserID::NUMBER AS assigneduserid,
                data_payload:TriggerInvoked::NUMBER AS triggerinvoked,
                data_payload:DelinquencyProcessID::NUMBER AS delinquencyprocessid,
                CAST(data_payload:ForceTimeoutBranch::TEXT AS VARCHAR(255)) AS forcetimeoutbranch,
                data_payload:Subtype::NUMBER AS subtype,
                TO_TIMESTAMP_TZ(data_payload:AssignmentDate::NUMBER/1000) AS assignmentdate,
                data_payload:AssignmentStatus::NUMBER AS assignmentstatus,
                data_payload:CAMTODSID::NUMBER AS camtodsid,
                data_payload:DirectDebitID::NUMBER AS directdebitid,
                data_payload:RefundID::NUMBER AS refundid,
                data_payload:Account::NUMBER AS account,
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
            FROM {{ source('gwbc', 'bc_workflow') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:previousgroupid::NUMBER AS previousgroupid,
                $1:enteredstep::TIMESTAMP_TZ AS enteredstep,
                CAST($1:previousstep::TEXT AS VARCHAR(255)) AS previousstep,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:currentstep::TEXT AS VARCHAR(255)) AS currentstep,
                $1:topleveldelinquencyprocessid::NUMBER AS topleveldelinquencyprocessid,
                $1:messagehistoryid::NUMBER AS messagehistoryid,
                $1:processversion::NUMBER AS processversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:handler::NUMBER AS handler,
                $1:assignedbyuserid::NUMBER AS assignedbyuserid,
                $1:activestate::NUMBER AS activestate,
                $1:assignedgroupid::NUMBER AS assignedgroupid,
                $1:logentrycounter::NUMBER AS logentrycounter,
                $1:state::NUMBER AS state,
                $1:previousqueueid::NUMBER AS previousqueueid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                CAST($1:currentbranch::TEXT AS VARCHAR(255)) AS currentbranch,
                $1:previoususerid::NUMBER AS previoususerid,
                $1:assignedqueueid::NUMBER AS assignedqueueid,
                $1:testtime::TIMESTAMP_TZ AS testtime,
                $1:messageid::NUMBER AS messageid,
                $1:createuserid::NUMBER AS createuserid,
                $1:closedate::TIMESTAMP_TZ AS closedate,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:agencycycleprocessid::NUMBER AS agencycycleprocessid,
                $1:retired::NUMBER AS retired,
                $1:stepexectime::NUMBER AS stepexectime,
                $1:timeouttime::TIMESTAMP_TZ AS timeouttime,
                $1:currentaction::NUMBER AS currentaction,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:assigneduserid::NUMBER AS assigneduserid,
                $1:triggerinvoked::NUMBER AS triggerinvoked,
                $1:delinquencyprocessid::NUMBER AS delinquencyprocessid,
                CAST($1:forcetimeoutbranch::TEXT AS VARCHAR(255)) AS forcetimeoutbranch,
                $1:subtype::NUMBER AS subtype,
                $1:assignmentdate::TIMESTAMP_TZ AS assignmentdate,
                $1:assignmentstatus::NUMBER AS assignmentstatus,
                $1:camtodsid::NUMBER AS camtodsid,
                $1:directdebitid::NUMBER AS directdebitid,
                $1:refundid::NUMBER AS refundid,
                $1:account::NUMBER AS account,
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
            FROM {{ source('gwbc', 'bc_workflow') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS workflow_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'previousgroupid',
                        'enteredstep',
                        'previousstep',
                        'publicid',
                        'currentstep',
                        'topleveldelinquencyprocessid',
                        'messagehistoryid',
                        'processversion',
                        'createtime',
                        'handler',
                        'assignedbyuserid',
                        'activestate',
                        'assignedgroupid',
                        'logentrycounter',
                        'state',
                        'previousqueueid',
                        'updatetime',
                        'currentbranch',
                        'previoususerid',
                        'assignedqueueid',
                        'testtime',
                        'messageid',
                        'createuserid',
                        'closedate',
                        'archivepartition',
                        'beanversion',
                        'agencycycleprocessid',
                        'retired',
                        'stepexectime',
                        'timeouttime',
                        'currentaction',
                        'updateuserid',
                        'assigneduserid',
                        'triggerinvoked',
                        'delinquencyprocessid',
                        'forcetimeoutbranch',
                        'subtype',
                        'assignmentdate',
                        'assignmentstatus',
                        'camtodsid',
                        'directdebitid',
                        'refundid',
                        'account'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}