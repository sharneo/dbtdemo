{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_activitypattern.
                                                activitypattern_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "cc_activitypattern"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:EscBusCalLocPath::TEXT AS VARCHAR(255)) AS escbuscallocpath,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:ActivityClass::NUMBER AS activityclass,
                CAST(data_payload:Command::TEXT AS VARCHAR(1333)) AS command,
                data_payload:ExternallyOwned::BOOLEAN AS externallyowned,
                data_payload:TargetIncludeDays::NUMBER AS targetincludedays,
                CAST(data_payload:DocumentTemplate::TEXT AS VARCHAR(255)) AS documenttemplate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:EmailTemplate::TEXT AS VARCHAR(255)) AS emailtemplate,
                data_payload:EscalationBusCalTag::NUMBER AS escalationbuscaltag,
                data_payload:ClaimLossType::NUMBER AS claimlosstype,
                data_payload:Mandatory::BOOLEAN AS mandatory,
                data_payload:EscalationHours::NUMBER AS escalationhours,
                data_payload:ID::NUMBER AS id,
                data_payload:TargetBusCalTag::NUMBER AS targetbuscaltag,
                data_payload:AutomatedOnly::BOOLEAN AS automatedonly,
                data_payload:Recurring::BOOLEAN AS recurring,
                data_payload:TargetHours::NUMBER AS targethours,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:Priority::NUMBER AS priority,
                CAST(data_payload:TargetBusCalLocPath::TEXT AS VARCHAR(255)) AS targetbuscallocpath,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:TargetDays::NUMBER AS targetdays,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:Subject::TEXT AS VARCHAR(255)) AS subject,
                data_payload:EscalationDays::NUMBER AS escalationdays,
                CAST(data_payload:Code::TEXT AS VARCHAR(60)) AS code,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:ShortSubject::TEXT AS VARCHAR(10)) AS shortsubject,
                data_payload:EscalationStartPt::NUMBER AS escalationstartpt,
                data_payload:ClosedClaimAvlble::BOOLEAN AS closedclaimavlble,
                data_payload:Importance::NUMBER AS importance,
                data_payload:Type::NUMBER AS type,
                data_payload:EscalationInclDays::NUMBER AS escalationincldays,
                CAST(data_payload:Description::TEXT AS VARCHAR(1333)) AS description,
                data_payload:Category::NUMBER AS category,
                data_payload:TargetStartPoint::NUMBER AS targetstartpoint,
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
            FROM {{ source('gwcc', 'cc_activitypattern') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:escbuscallocpath::TEXT AS VARCHAR(255)) AS escbuscallocpath,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:activityclass::NUMBER AS activityclass,
                CAST($1:command::TEXT AS VARCHAR(1333)) AS command,
                $1:externallyowned::BOOLEAN AS externallyowned,
                $1:targetincludedays::NUMBER AS targetincludedays,
                CAST($1:documenttemplate::TEXT AS VARCHAR(255)) AS documenttemplate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:emailtemplate::TEXT AS VARCHAR(255)) AS emailtemplate,
                $1:escalationbuscaltag::NUMBER AS escalationbuscaltag,
                $1:claimlosstype::NUMBER AS claimlosstype,
                $1:mandatory::BOOLEAN AS mandatory,
                $1:escalationhours::NUMBER AS escalationhours,
                $1:id::NUMBER AS id,
                $1:targetbuscaltag::NUMBER AS targetbuscaltag,
                $1:automatedonly::BOOLEAN AS automatedonly,
                $1:recurring::BOOLEAN AS recurring,
                $1:targethours::NUMBER AS targethours,
                $1:createuserid::NUMBER AS createuserid,
                $1:priority::NUMBER AS priority,
                CAST($1:targetbuscallocpath::TEXT AS VARCHAR(255)) AS targetbuscallocpath,
                $1:beanversion::NUMBER AS beanversion,
                $1:targetdays::NUMBER AS targetdays,
                $1:retired::NUMBER AS retired,
                CAST($1:subject::TEXT AS VARCHAR(255)) AS subject,
                $1:escalationdays::NUMBER AS escalationdays,
                CAST($1:code::TEXT AS VARCHAR(60)) AS code,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:shortsubject::TEXT AS VARCHAR(10)) AS shortsubject,
                $1:escalationstartpt::NUMBER AS escalationstartpt,
                $1:closedclaimavlble::BOOLEAN AS closedclaimavlble,
                $1:importance::NUMBER AS importance,
                $1:type::NUMBER AS type,
                $1:escalationincldays::NUMBER AS escalationincldays,
                CAST($1:description::TEXT AS VARCHAR(1333)) AS description,
                $1:category::NUMBER AS category,
                $1:targetstartpoint::NUMBER AS targetstartpoint,
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
            FROM {{ source('gwcc', 'cc_activitypattern') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS activitypattern_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'escbuscallocpath',
                        'publicid',
                        'createtime',
                        'activityclass',
                        'command',
                        'externallyowned',
                        'targetincludedays',
                        'documenttemplate',
                        'updatetime',
                        'emailtemplate',
                        'escalationbuscaltag',
                        'claimlosstype',
                        'mandatory',
                        'escalationhours',
                        'targetbuscaltag',
                        'automatedonly',
                        'recurring',
                        'targethours',
                        'createuserid',
                        'priority',
                        'targetbuscallocpath',
                        'beanversion',
                        'targetdays',
                        'retired',
                        'subject',
                        'escalationdays',
                        'code',
                        'updateuserid',
                        'shortsubject',
                        'escalationstartpt',
                        'closedclaimavlble',
                        'importance',
                        'type',
                        'escalationincldays',
                        'description',
                        'category',
                        'targetstartpoint'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}