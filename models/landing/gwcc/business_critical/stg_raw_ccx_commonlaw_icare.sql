{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_commonlaw_icare.
                                                commonlaw_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "ccx_commonlaw_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CTMResolutionType::NUMBER AS ctmresolutiontype,
                TO_TIMESTAMP_TZ(data_payload:Section74NoticeDate_icare::NUMBER/1000) AS section74noticedate_icare,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:DateParticRece_icare::NUMBER/1000) AS dateparticrece_icare,
                TO_TIMESTAMP_TZ(data_payload:NoCDateReceived_icare::NUMBER/1000) AS nocdatereceived_icare,
                TO_TIMESTAMP_TZ(data_payload:DateParticRequ_icare::NUMBER/1000) AS dateparticrequ_icare,
                data_payload:Outcome_icare::NUMBER AS outcome_icare,
                TO_TIMESTAMP_TZ(data_payload:InitialDateReceived_icare::NUMBER/1000) AS initialdatereceived_icare,
                data_payload:TypeOfDispute_icare::NUMBER AS typeofdispute_icare,
                TO_TIMESTAMP_TZ(data_payload:SettlementDate_icare::NUMBER/1000) AS settlementdate_icare,
                data_payload:InitialNoticeOfIntention_icare::BOOLEAN AS initialnoticeofintention_icare,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimDisputed_icare::BOOLEAN AS claimdisputed_icare,
                data_payload:NoticeOfIntention_icare::BOOLEAN AS noticeofintention_icare,
                CAST(data_payload:MediationCertificate_icare::TEXT AS VARCHAR(16777216)) AS mediationcertificate_icare,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:NameReference_icare::TEXT AS VARCHAR(16777216)) AS namereference_icare,
                CAST(data_payload:ReasonReopened_icare::TEXT AS VARCHAR(16777216)) AS reasonreopened_icare,
                TO_TIMESTAMP_TZ(data_payload:PreFilingDefenceDate_icare::NUMBER/1000) AS prefilingdefencedate_icare,
                TO_TIMESTAMP_TZ(data_payload:DefectiveNoticeSentDate_icare::NUMBER/1000) AS defectivenoticesentdate_icare,
                data_payload:LitigationStarted::BOOLEAN AS litigationstarted,
                TO_TIMESTAMP_TZ(data_payload:MediationDate_icare::NUMBER/1000) AS mediationdate_icare,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:NoticeOfClaimNotReceived_icare::BOOLEAN AS noticeofclaimnotreceived_icare,
                data_payload:RecoveryPotential_icare::BOOLEAN AS recoverypotential_icare,
                data_payload:Settlement_icare::NUMBER AS settlement_icare,
                TO_TIMESTAMP_TZ(data_payload:DateOfInstruction_icare::NUMBER/1000) AS dateofinstruction_icare,
                data_payload:ValidationLevel_icare::NUMBER AS validationlevel_icare,
                TO_TIMESTAMP_TZ(data_payload:PreFilingDateReceived_icare::NUMBER/1000) AS prefilingdatereceived_icare,
                TO_TIMESTAMP_TZ(data_payload:StatementOfClaimDateRec_icare::NUMBER/1000) AS statementofclaimdaterec_icare,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:NoticeOfClaim_icare::BOOLEAN AS noticeofclaim_icare,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:SettlementCost_icare AS NUMBER(18,2)) AS settlementcost_icare,
                data_payload:InitialNoticeNotRec_icare::BOOLEAN AS initialnoticenotrec_icare,
                CAST(data_payload:OurLegalCosts_icare AS NUMBER(18,2)) AS ourlegalcosts_icare,
                data_payload:MatterType_icare::NUMBER AS mattertype_icare,
                data_payload:Subtype::NUMBER AS subtype,
                TO_TIMESTAMP_TZ(data_payload:TrialDate_icare::NUMBER/1000) AS trialdate_icare,
                data_payload:ResultOfMediation_icare::NUMBER AS resultofmediation_icare,
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
            FROM {{ source('gwcc', 'ccx_commonlaw_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:ctmresolutiontype::NUMBER AS ctmresolutiontype,
                $1:section74noticedate_icare::TIMESTAMP_TZ AS section74noticedate_icare,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:dateparticrece_icare::TIMESTAMP_TZ AS dateparticrece_icare,
                $1:nocdatereceived_icare::TIMESTAMP_TZ AS nocdatereceived_icare,
                $1:dateparticrequ_icare::TIMESTAMP_TZ AS dateparticrequ_icare,
                $1:outcome_icare::NUMBER AS outcome_icare,
                $1:initialdatereceived_icare::TIMESTAMP_TZ AS initialdatereceived_icare,
                $1:typeofdispute_icare::NUMBER AS typeofdispute_icare,
                $1:settlementdate_icare::TIMESTAMP_TZ AS settlementdate_icare,
                $1:initialnoticeofintention_icare::BOOLEAN AS initialnoticeofintention_icare,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimdisputed_icare::BOOLEAN AS claimdisputed_icare,
                $1:noticeofintention_icare::BOOLEAN AS noticeofintention_icare,
                CAST($1:mediationcertificate_icare::TEXT AS VARCHAR(16777216)) AS mediationcertificate_icare,
                $1:id::NUMBER AS id,
                CAST($1:namereference_icare::TEXT AS VARCHAR(16777216)) AS namereference_icare,
                CAST($1:reasonreopened_icare::TEXT AS VARCHAR(16777216)) AS reasonreopened_icare,
                $1:prefilingdefencedate_icare::TIMESTAMP_TZ AS prefilingdefencedate_icare,
                $1:defectivenoticesentdate_icare::TIMESTAMP_TZ AS defectivenoticesentdate_icare,
                $1:litigationstarted::BOOLEAN AS litigationstarted,
                $1:mediationdate_icare::TIMESTAMP_TZ AS mediationdate_icare,
                $1:createuserid::NUMBER AS createuserid,
                $1:noticeofclaimnotreceived_icare::BOOLEAN AS noticeofclaimnotreceived_icare,
                $1:recoverypotential_icare::BOOLEAN AS recoverypotential_icare,
                $1:settlement_icare::NUMBER AS settlement_icare,
                $1:dateofinstruction_icare::TIMESTAMP_TZ AS dateofinstruction_icare,
                $1:validationlevel_icare::NUMBER AS validationlevel_icare,
                $1:prefilingdatereceived_icare::TIMESTAMP_TZ AS prefilingdatereceived_icare,
                $1:statementofclaimdaterec_icare::TIMESTAMP_TZ AS statementofclaimdaterec_icare,
                $1:beanversion::NUMBER AS beanversion,
                $1:noticeofclaim_icare::BOOLEAN AS noticeofclaim_icare,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:settlementcost_icare AS NUMBER(18,2)) AS settlementcost_icare,
                $1:initialnoticenotrec_icare::BOOLEAN AS initialnoticenotrec_icare,
                CAST($1:ourlegalcosts_icare AS NUMBER(18,2)) AS ourlegalcosts_icare,
                $1:mattertype_icare::NUMBER AS mattertype_icare,
                $1:subtype::NUMBER AS subtype,
                $1:trialdate_icare::TIMESTAMP_TZ AS trialdate_icare,
                $1:resultofmediation_icare::NUMBER AS resultofmediation_icare,
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
            FROM {{ source('gwcc', 'ccx_commonlaw_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS commonlaw_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'ctmresolutiontype',
                        'section74noticedate_icare',
                        'publicid',
                        'createtime',
                        'dateparticrece_icare',
                        'nocdatereceived_icare',
                        'dateparticrequ_icare',
                        'outcome_icare',
                        'initialdatereceived_icare',
                        'typeofdispute_icare',
                        'settlementdate_icare',
                        'initialnoticeofintention_icare',
                        'updatetime',
                        'claimdisputed_icare',
                        'noticeofintention_icare',
                        'mediationcertificate_icare',
                        'namereference_icare',
                        'reasonreopened_icare',
                        'prefilingdefencedate_icare',
                        'defectivenoticesentdate_icare',
                        'litigationstarted',
                        'mediationdate_icare',
                        'createuserid',
                        'noticeofclaimnotreceived_icare',
                        'recoverypotential_icare',
                        'settlement_icare',
                        'dateofinstruction_icare',
                        'validationlevel_icare',
                        'prefilingdatereceived_icare',
                        'statementofclaimdaterec_icare',
                        'beanversion',
                        'noticeofclaim_icare',
                        'retired',
                        'updateuserid',
                        'settlementcost_icare',
                        'initialnoticenotrec_icare',
                        'ourlegalcosts_icare',
                        'mattertype_icare',
                        'subtype',
                        'trialdate_icare',
                        'resultofmediation_icare'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}