{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_workflow.
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "cc_workflow"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:WorkCapacityDecisionID::NUMBER AS workcapacitydecisionid,
                TO_TIMESTAMP_TZ(data_payload:EnteredStep::NUMBER/1000) AS enteredstep,
                CAST(data_payload:PreviousStep::TEXT AS VARCHAR(255)) AS previousstep,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:CurrentStep::TEXT AS VARCHAR(255)) AS currentstep,
                data_payload:MessageHistoryID::NUMBER AS messagehistoryid,
                data_payload:ProcessVersion::NUMBER AS processversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:AbsoluteTimeOut::NUMBER/1000) AS absolutetimeout,
                data_payload:Handler::NUMBER AS handler,
                data_payload:ActiveState::NUMBER AS activestate,
                data_payload:painOdsWorkItem::NUMBER AS painodsworkitem,
                data_payload:LogEntryCounter::NUMBER AS logentrycounter,
                data_payload:State::NUMBER AS state,
                TO_TIMESTAMP_TZ(data_payload:TriggerTriageDate_icare::NUMBER/1000) AS triggertriagedate_icare,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:CurrentBranch::TEXT AS VARCHAR(255)) AS currentbranch,
                CAST(data_payload:NextActivityId::TEXT AS VARCHAR(16777216)) AS nextactivityid,
                data_payload:CamtMappingAction::NUMBER AS camtmappingaction,
                TO_TIMESTAMP_TZ(data_payload:TestTime::NUMBER/1000) AS testtime,
                data_payload:odsWorkItem::NUMBER AS odsworkitem,
                data_payload:MessageID::NUMBER AS messageid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ReasonForUnknown::NUMBER AS reasonforunknown,
                data_payload:TriggerTriage_icare::BOOLEAN AS triggertriage_icare,
                data_payload:MetroReportID::NUMBER AS metroreportid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:StepExecTime::NUMBER AS stepexectime,
                TO_TIMESTAMP_TZ(data_payload:TimeoutTime::NUMBER/1000) AS timeouttime,
                data_payload:CurrentAction::NUMBER AS currentaction,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:RelatedClaim::NUMBER AS relatedclaim,
                data_payload:TriggerInvoked::NUMBER AS triggerinvoked,
                data_payload:TimedOut::BOOLEAN AS timedout,
                CAST(data_payload:ForceTimeoutBranch::TEXT AS VARCHAR(255)) AS forcetimeoutbranch,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:Claim::NUMBER AS claim,
                data_payload:RelatedCheck::NUMBER AS relatedcheck,
                data_payload:PendingCAMTWorkflowFlag::BOOLEAN AS pendingcamtworkflowflag,
                data_payload:OCRInvoiceID::NUMBER AS ocrinvoiceid,
                data_payload:CSPID::NUMBER AS cspid,
                data_payload:WRAActive::NUMBER AS wraactive,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(40)) AS policynumber,
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
            FROM {{ source('gwcc', 'cc_workflow') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:workcapacitydecisionid::NUMBER AS workcapacitydecisionid,
                $1:enteredstep::TIMESTAMP_TZ AS enteredstep,
                CAST($1:previousstep::TEXT AS VARCHAR(255)) AS previousstep,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:currentstep::TEXT AS VARCHAR(255)) AS currentstep,
                $1:messagehistoryid::NUMBER AS messagehistoryid,
                $1:processversion::NUMBER AS processversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:absolutetimeout::TIMESTAMP_TZ AS absolutetimeout,
                $1:handler::NUMBER AS handler,
                $1:activestate::NUMBER AS activestate,
                $1:painodsworkitem::NUMBER AS painodsworkitem,
                $1:logentrycounter::NUMBER AS logentrycounter,
                $1:state::NUMBER AS state,
                $1:triggertriagedate_icare::TIMESTAMP_TZ AS triggertriagedate_icare,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:id::NUMBER AS id,
                CAST($1:currentbranch::TEXT AS VARCHAR(255)) AS currentbranch,
                CAST($1:nextactivityid::TEXT AS VARCHAR(16777216)) AS nextactivityid,
                $1:camtmappingaction::NUMBER AS camtmappingaction,
                $1:testtime::TIMESTAMP_TZ AS testtime,
                $1:odsworkitem::NUMBER AS odsworkitem,
                $1:messageid::NUMBER AS messageid,
                $1:createuserid::NUMBER AS createuserid,
                $1:reasonforunknown::NUMBER AS reasonforunknown,
                $1:triggertriage_icare::BOOLEAN AS triggertriage_icare,
                $1:metroreportid::NUMBER AS metroreportid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:stepexectime::NUMBER AS stepexectime,
                $1:timeouttime::TIMESTAMP_TZ AS timeouttime,
                $1:currentaction::NUMBER AS currentaction,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:relatedclaim::NUMBER AS relatedclaim,
                $1:triggerinvoked::NUMBER AS triggerinvoked,
                $1:timedout::BOOLEAN AS timedout,
                CAST($1:forcetimeoutbranch::TEXT AS VARCHAR(255)) AS forcetimeoutbranch,
                $1:subtype::NUMBER AS subtype,
                $1:claim::NUMBER AS claim,
                $1:relatedcheck::NUMBER AS relatedcheck,
                $1:pendingcamtworkflowflag::BOOLEAN AS pendingcamtworkflowflag,
                $1:ocrinvoiceid::NUMBER AS ocrinvoiceid,
                $1:cspid::NUMBER AS cspid,
                $1:wraactive::NUMBER AS wraactive,
                CAST($1:policynumber::TEXT AS VARCHAR(40)) AS policynumber,
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
            FROM {{ source('gwcc', 'cc_workflow') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS workflow_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'workcapacitydecisionid',
                        'enteredstep',
                        'previousstep',
                        'publicid',
                        'currentstep',
                        'messagehistoryid',
                        'processversion',
                        'createtime',
                        'absolutetimeout',
                        'handler',
                        'activestate',
                        'painodsworkitem',
                        'logentrycounter',
                        'state',
                        'triggertriagedate_icare',
                        'updatetime',
                        'claimid',
                        'currentbranch',
                        'nextactivityid',
                        'camtmappingaction',
                        'testtime',
                        'odsworkitem',
                        'messageid',
                        'createuserid',
                        'reasonforunknown',
                        'triggertriage_icare',
                        'metroreportid',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'stepexectime',
                        'timeouttime',
                        'currentaction',
                        'updateuserid',
                        'relatedclaim',
                        'triggerinvoked',
                        'timedout',
                        'forcetimeoutbranch',
                        'subtype',
                        'claim',
                        'relatedcheck',
                        'pendingcamtworkflowflag',
                        'ocrinvoiceid',
                        'cspid',
                        'wraactive',
                        'policynumber'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}