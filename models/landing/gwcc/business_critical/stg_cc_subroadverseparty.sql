{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_subroadverseparty.
                                                subroadverseparty_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "cc_subroadverseparty"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:MainContactType::NUMBER AS maincontacttype,
                CAST(data_payload:MainContact_icare::TEXT AS VARCHAR(50)) AS maincontact_icare,
                TO_TIMESTAMP_TZ(data_payload:ActivityWorflowDate_icare::NUMBER/1000) AS activityworflowdate_icare,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(60)) AS claimnumber,
                CAST(data_payload:ClosingComment_icare::TEXT AS VARCHAR(255)) AS closingcomment_icare,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:DocumentLinkableID::NUMBER AS documentlinkableid,
                TO_TIMESTAMP_TZ(data_payload:NoteSent::NUMBER/1000) AS notesent,
                data_payload:RecoveryType_icare::NUMBER AS recoverytype_icare,
                CAST(data_payload:ExpectedRecovery AS NUMBER(4,1)) AS expectedrecovery,
                data_payload:Outcome_icare::NUMBER AS outcome_icare,
                data_payload:AdversePartyID::NUMBER AS adversepartyid,
                CAST(data_payload:MainContactsNumber_icare::TEXT AS VARCHAR(15)) AS maincontactsnumber_icare,
                TO_TIMESTAMP_TZ(data_payload:NoteReceived::NUMBER/1000) AS notereceived,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:SubrogationStatus_icare::NUMBER AS subrogationstatus_icare,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:DueDate_icare::NUMBER/1000) AS duedate_icare,
                CAST(data_payload:ExpectedRecoveryAmount_icare AS NUMBER(18,2)) AS expectedrecoveryamount_icare,
                TO_TIMESTAMP_TZ(data_payload:RecoveryCommencedDate_icare::NUMBER/1000) AS recoverycommenceddate_icare,
                TO_TIMESTAMP_TZ(data_payload:CloseDate_icare::NUMBER/1000) AS closedate_icare,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:Fault AS NUMBER(4,1)) AS fault,
                data_payload:Waiver::BOOLEAN AS waiver,
                data_payload:ScheduleRecoveryType::NUMBER AS schedulerecoverytype,
                data_payload:Classification::NUMBER AS classification,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                CAST(data_payload:PayeeInstructions_icare::TEXT AS VARCHAR(250)) AS payeeinstructions_icare,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ScheduleRecovery::BOOLEAN AS schedulerecovery,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Strategy::NUMBER AS strategy,
                data_payload:SubroGovernmentInvolved::NUMBER AS subrogovernmentinvolved,
                data_payload:SubrogationSummaryID::NUMBER AS subrogationsummaryid,
                CAST(data_payload:CourtAwardedInterest_icare AS NUMBER(18,2)) AS courtawardedinterest_icare,
                TO_TIMESTAMP_TZ(data_payload:NextCollectionDate_icare::NUMBER/1000) AS nextcollectiondate_icare,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(60)) AS policynumber,
                TO_TIMESTAMP_TZ(data_payload:ServiceDate_Ext::NUMBER/1000) AS servicedate_ext,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS STRING) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'AVRO' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_subroadverseparty') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:maincontacttype::NUMBER AS maincontacttype,
                CAST($1:maincontact_icare::TEXT AS VARCHAR(50)) AS maincontact_icare,
                $1:activityworflowdate_icare::TIMESTAMP_TZ AS activityworflowdate_icare,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:claimnumber::TEXT AS VARCHAR(60)) AS claimnumber,
                CAST($1:closingcomment_icare::TEXT AS VARCHAR(255)) AS closingcomment_icare,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:documentlinkableid::NUMBER AS documentlinkableid,
                $1:notesent::TIMESTAMP_TZ AS notesent,
                $1:recoverytype_icare::NUMBER AS recoverytype_icare,
                CAST($1:expectedrecovery AS NUMBER(4,1)) AS expectedrecovery,
                $1:outcome_icare::NUMBER AS outcome_icare,
                $1:adversepartyid::NUMBER AS adversepartyid,
                CAST($1:maincontactsnumber_icare::TEXT AS VARCHAR(15)) AS maincontactsnumber_icare,
                $1:notereceived::TIMESTAMP_TZ AS notereceived,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:subrogationstatus_icare::NUMBER AS subrogationstatus_icare,
                $1:id::NUMBER AS id,
                $1:duedate_icare::TIMESTAMP_TZ AS duedate_icare,
                CAST($1:expectedrecoveryamount_icare AS NUMBER(18,2)) AS expectedrecoveryamount_icare,
                $1:recoverycommenceddate_icare::TIMESTAMP_TZ AS recoverycommenceddate_icare,
                $1:closedate_icare::TIMESTAMP_TZ AS closedate_icare,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:fault AS NUMBER(4,1)) AS fault,
                $1:waiver::BOOLEAN AS waiver,
                $1:schedulerecoverytype::NUMBER AS schedulerecoverytype,
                $1:classification::NUMBER AS classification,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                CAST($1:payeeinstructions_icare::TEXT AS VARCHAR(250)) AS payeeinstructions_icare,
                $1:retired::NUMBER AS retired,
                $1:schedulerecovery::BOOLEAN AS schedulerecovery,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:strategy::NUMBER AS strategy,
                $1:subrogovernmentinvolved::NUMBER AS subrogovernmentinvolved,
                $1:subrogationsummaryid::NUMBER AS subrogationsummaryid,
                CAST($1:courtawardedinterest_icare AS NUMBER(18,2)) AS courtawardedinterest_icare,
                $1:nextcollectiondate_icare::TIMESTAMP_TZ AS nextcollectiondate_icare,
                CAST($1:policynumber::TEXT AS VARCHAR(60)) AS policynumber,
                $1:servicedate_ext::TIMESTAMP_TZ AS servicedate_ext,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::STRING as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'PARQUET' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_subroadverseparty') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS subroadverseparty_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'maincontacttype',
                        'maincontact_icare',
                        'activityworflowdate_icare',
                        'publicid',
                        'claimnumber',
                        'closingcomment_icare',
                        'createtime',
                        'documentlinkableid',
                        'notesent',
                        'recoverytype_icare',
                        'expectedrecovery',
                        'outcome_icare',
                        'adversepartyid',
                        'maincontactsnumber_icare',
                        'notereceived',
                        'updatetime',
                        'subrogationstatus_icare',
                        'duedate_icare',
                        'expectedrecoveryamount_icare',
                        'recoverycommenceddate_icare',
                        'closedate_icare',
                        'createuserid',
                        'fault',
                        'waiver',
                        'schedulerecoverytype',
                        'classification',
                        'beanversion',
                        'archivepartition',
                        'payeeinstructions_icare',
                        'retired',
                        'schedulerecovery',
                        'updateuserid',
                        'strategy',
                        'subrogovernmentinvolved',
                        'subrogationsummaryid',
                        'courtawardedinterest_icare',
                        'nextcollectiondate_icare',
                        'policynumber',
                        'servicedate_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}