{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_wpiassessrecord_icare.
                                                wpiassessrecord_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "ccx_wpiassessrecord_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:WPIResult_icare::NUMBER AS wpiresult_icare,
                data_payload:WPIAssessment_icareID::NUMBER AS wpiassessment_icareid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:DocumentLinkableID::NUMBER AS documentlinkableid,
                CAST(data_payload:ClaimedBHLPerventage_icare AS NUMBER(4,1)) AS claimedbhlperventage_icare,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:WPIAssessmentState_icare::BOOLEAN AS wpiassessmentstate_icare,
                data_payload:ID::NUMBER AS id,
                data_payload:SettlementType_icare::NUMBER AS settlementtype_icare,
                CAST(data_payload:OfferedBHLPercentage_icare AS NUMBER(4,1)) AS offeredbhlpercentage_icare,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:LetterOfOfferDate_icare::NUMBER/1000) AS letterofofferdate_icare,
                data_payload:Retired::NUMBER AS retired,
                data_payload:Medicare_icare::NUMBER AS medicare_icare,
                data_payload:OfferedWPIPercentage_icare::NUMBER AS offeredwpipercentage_icare,
                CAST(data_payload:Interest AS NUMBER(18,2)) AS interest,
                TO_TIMESTAMP_TZ(data_payload:RelevantParticularsDate_icare::NUMBER/1000) AS relevantparticularsdate_icare,
                data_payload:ActionType_icare::NUMBER AS actiontype_icare,
                TO_TIMESTAMP_TZ(data_payload:OfferAcceptedDate_icare::NUMBER/1000) AS offeraccepteddate_icare,
                TO_TIMESTAMP_TZ(data_payload:ApprovalDate_icare::NUMBER/1000) AS approvaldate_icare,
                TO_TIMESTAMP_TZ(data_payload:ComplyingAgreementDate_icare::NUMBER/1000) AS complyingagreementdate_icare,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:ClaimedPercentage_icare::NUMBER AS claimedpercentage_icare,
                data_payload:Approved_icare::BOOLEAN AS approved_icare,
                data_payload:HasBackInjury_icare::NUMBER AS hasbackinjury_icare,
                data_payload:MaxMedImprovReached_icare::NUMBER AS maxmedimprovreached_icare,
                CAST(data_payload:SettlementAmount_icare AS NUMBER(18,2)) AS settlementamount_icare,
                TO_TIMESTAMP_TZ(data_payload:S66ReceivedDate_icare::NUMBER/1000) AS s66receiveddate_icare,
                CAST(data_payload:BHLResult_icare AS NUMBER(4,1)) AS bhlresult_icare,
                data_payload:OfferAccepted_icare::NUMBER AS offeraccepted_icare,
                data_payload:AssessedWPIForS66::NUMBER AS assessedwpifors66,
                CAST(data_payload:FinalLumpSumAmount AS NUMBER(18,2)) AS finallumpsumamount,
                TO_TIMESTAMP_TZ(data_payload:DateOffered::NUMBER/1000) AS dateoffered,
                CAST(data_payload:ClaimedS67Amount AS NUMBER(18,2)) AS claimeds67amount,
                CAST(data_payload:OfferedAmount AS NUMBER(18,2)) AS offeredamount,
                CAST(data_payload:LumpSumAmountOffered AS NUMBER(18,2)) AS lumpsumamountoffered,
                CAST(data_payload:S67SettlementAmount AS NUMBER(18,2)) AS s67settlementamount,
                TO_TIMESTAMP_TZ(data_payload:LegacyCreateTime::NUMBER/1000) AS legacycreatetime,
                TO_TIMESTAMP_TZ(data_payload:LegacyUpdateTime::NUMBER/1000) AS legacyupdatetime,
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
            FROM {{ source('gwcc', 'ccx_wpiassessrecord_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:wpiresult_icare::NUMBER AS wpiresult_icare,
                $1:wpiassessment_icareid::NUMBER AS wpiassessment_icareid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:documentlinkableid::NUMBER AS documentlinkableid,
                CAST($1:claimedbhlperventage_icare AS NUMBER(4,1)) AS claimedbhlperventage_icare,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:wpiassessmentstate_icare::BOOLEAN AS wpiassessmentstate_icare,
                $1:id::NUMBER AS id,
                $1:settlementtype_icare::NUMBER AS settlementtype_icare,
                CAST($1:offeredbhlpercentage_icare AS NUMBER(4,1)) AS offeredbhlpercentage_icare,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:letterofofferdate_icare::TIMESTAMP_TZ AS letterofofferdate_icare,
                $1:retired::NUMBER AS retired,
                $1:medicare_icare::NUMBER AS medicare_icare,
                $1:offeredwpipercentage_icare::NUMBER AS offeredwpipercentage_icare,
                CAST($1:interest AS NUMBER(18,2)) AS interest,
                $1:relevantparticularsdate_icare::TIMESTAMP_TZ AS relevantparticularsdate_icare,
                $1:actiontype_icare::NUMBER AS actiontype_icare,
                $1:offeraccepteddate_icare::TIMESTAMP_TZ AS offeraccepteddate_icare,
                $1:approvaldate_icare::TIMESTAMP_TZ AS approvaldate_icare,
                $1:complyingagreementdate_icare::TIMESTAMP_TZ AS complyingagreementdate_icare,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:claimedpercentage_icare::NUMBER AS claimedpercentage_icare,
                $1:approved_icare::BOOLEAN AS approved_icare,
                $1:hasbackinjury_icare::NUMBER AS hasbackinjury_icare,
                $1:maxmedimprovreached_icare::NUMBER AS maxmedimprovreached_icare,
                CAST($1:settlementamount_icare AS NUMBER(18,2)) AS settlementamount_icare,
                $1:s66receiveddate_icare::TIMESTAMP_TZ AS s66receiveddate_icare,
                CAST($1:bhlresult_icare AS NUMBER(4,1)) AS bhlresult_icare,
                $1:offeraccepted_icare::NUMBER AS offeraccepted_icare,
                $1:assessedwpifors66::NUMBER AS assessedwpifors66,
                CAST($1:finallumpsumamount AS NUMBER(18,2)) AS finallumpsumamount,
                $1:dateoffered::TIMESTAMP_TZ AS dateoffered,
                CAST($1:claimeds67amount AS NUMBER(18,2)) AS claimeds67amount,
                CAST($1:offeredamount AS NUMBER(18,2)) AS offeredamount,
                CAST($1:lumpsumamountoffered AS NUMBER(18,2)) AS lumpsumamountoffered,
                CAST($1:s67settlementamount AS NUMBER(18,2)) AS s67settlementamount,
                $1:legacycreatetime::TIMESTAMP_TZ AS legacycreatetime,
                $1:legacyupdatetime::TIMESTAMP_TZ AS legacyupdatetime,
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
            FROM {{ source('gwcc', 'ccx_wpiassessrecord_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS wpiassessrecord_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'wpiresult_icare',
                        'wpiassessment_icareid',
                        'createtime',
                        'documentlinkableid',
                        'claimedbhlperventage_icare',
                        'updatetime',
                        'wpiassessmentstate_icare',
                        'settlementtype_icare',
                        'offeredbhlpercentage_icare',
                        'createuserid',
                        'beanversion',
                        'archivepartition',
                        'letterofofferdate_icare',
                        'retired',
                        'medicare_icare',
                        'offeredwpipercentage_icare',
                        'interest',
                        'relevantparticularsdate_icare',
                        'actiontype_icare',
                        'offeraccepteddate_icare',
                        'approvaldate_icare',
                        'complyingagreementdate_icare',
                        'updateuserid',
                        'claimedpercentage_icare',
                        'approved_icare',
                        'hasbackinjury_icare',
                        'maxmedimprovreached_icare',
                        'settlementamount_icare',
                        's66receiveddate_icare',
                        'bhlresult_icare',
                        'offeraccepted_icare',
                        'assessedwpifors66',
                        'finallumpsumamount',
                        'dateoffered',
                        'claimeds67amount',
                        'offeredamount',
                        'lumpsumamountoffered',
                        's67settlementamount',
                        'legacycreatetime',
                        'legacyupdatetime'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}