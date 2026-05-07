{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_ahrr_icare.
                                                ahrr_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "ccx_ahrr_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                TO_TIMESTAMP_TZ(data_payload:AHRREndDate::NUMBER/1000) AS ahrrenddate,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:CLMPracticeSuburb::TEXT AS VARCHAR(255)) AS clmpracticesuburb,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(255)) AS claimnumber,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:AHRRStartDate::NUMBER/1000) AS ahrrstartdate,
                CAST(data_payload:CLMReferredBy::TEXT AS VARCHAR(255)) AS clmreferredby,
                CAST(data_payload:CLMServiceProviderName::TEXT AS VARCHAR(255)) AS clmserviceprovidername,
                CAST(data_payload:CLMPracticeState::TEXT AS VARCHAR(255)) AS clmpracticestate,
                CAST(data_payload:CLMReferrerPhoneNumber::TEXT AS VARCHAR(255)) AS clmreferrerphonenumber,
                CAST(data_payload:CLMWorkerPhoneNumber::TEXT AS VARCHAR(255)) AS clmworkerphonenumber,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:CLMChiropractor::BOOLEAN AS clmchiropractor,
                data_payload:ClaimID::NUMBER AS claimid,
                CAST(data_payload:CLMPracticePostalCode::TEXT AS VARCHAR(255)) AS clmpracticepostalcode,
                TO_TIMESTAMP_TZ(data_payload:CLMTreatmentCommenced::NUMBER/1000) AS clmtreatmentcommenced,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:CLMPracticeEmail::TEXT AS VARCHAR(255)) AS clmpracticeemail,
                data_payload:CLMNoSessionProvidedToDate::NUMBER AS clmnosessionprovidedtodate,
                data_payload:CLMCounsellor::BOOLEAN AS clmcounsellor,
                data_payload:CLMPhysiotherapist::BOOLEAN AS clmphysiotherapist,
                TO_TIMESTAMP_TZ(data_payload:CLMDateOfInjury::NUMBER/1000) AS clmdateofinjury,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:DateOfRequest::NUMBER/1000) AS dateofrequest,
                CAST(data_payload:CLMPracticeName::TEXT AS VARCHAR(255)) AS clmpracticename,
                CAST(data_payload:CLMOther::TEXT AS VARCHAR(255)) AS clmother,
                CAST(data_payload:CLMWorkerName::TEXT AS VARCHAR(255)) AS clmworkername,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:AHRRNumber::TEXT AS VARCHAR(255)) AS ahrrnumber,
                CAST(data_payload:DocumentIndentifier::TEXT AS VARCHAR(255)) AS documentindentifier,
                CAST(data_payload:CLMSIRANumber::TEXT AS VARCHAR(255)) AS clmsiranumber,
                data_payload:CLMAccExercisePhysiologist::BOOLEAN AS clmaccexercisephysiologist,
                TO_TIMESTAMP_TZ(data_payload:CLMDateOfBirth::NUMBER/1000) AS clmdateofbirth,
                CAST(data_payload:CLMPracticePhoneNumber::TEXT AS VARCHAR(255)) AS clmpracticephonenumber,
                data_payload:CLMOsteopath::BOOLEAN AS clmosteopath,
                data_payload:CLMPsychologist::BOOLEAN AS clmpsychologist,
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
            FROM {{ source('gwcc', 'ccx_ahrr_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:ahrrenddate::TIMESTAMP_TZ AS ahrrenddate,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:clmpracticesuburb::TEXT AS VARCHAR(255)) AS clmpracticesuburb,
                CAST($1:claimnumber::TEXT AS VARCHAR(255)) AS claimnumber,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:ahrrstartdate::TIMESTAMP_TZ AS ahrrstartdate,
                CAST($1:clmreferredby::TEXT AS VARCHAR(255)) AS clmreferredby,
                CAST($1:clmserviceprovidername::TEXT AS VARCHAR(255)) AS clmserviceprovidername,
                CAST($1:clmpracticestate::TEXT AS VARCHAR(255)) AS clmpracticestate,
                CAST($1:clmreferrerphonenumber::TEXT AS VARCHAR(255)) AS clmreferrerphonenumber,
                CAST($1:clmworkerphonenumber::TEXT AS VARCHAR(255)) AS clmworkerphonenumber,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:clmchiropractor::BOOLEAN AS clmchiropractor,
                $1:claimid::NUMBER AS claimid,
                CAST($1:clmpracticepostalcode::TEXT AS VARCHAR(255)) AS clmpracticepostalcode,
                $1:clmtreatmentcommenced::TIMESTAMP_TZ AS clmtreatmentcommenced,
                $1:id::NUMBER AS id,
                CAST($1:clmpracticeemail::TEXT AS VARCHAR(255)) AS clmpracticeemail,
                $1:clmnosessionprovidedtodate::NUMBER AS clmnosessionprovidedtodate,
                $1:clmcounsellor::BOOLEAN AS clmcounsellor,
                $1:clmphysiotherapist::BOOLEAN AS clmphysiotherapist,
                $1:clmdateofinjury::TIMESTAMP_TZ AS clmdateofinjury,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:dateofrequest::TIMESTAMP_TZ AS dateofrequest,
                CAST($1:clmpracticename::TEXT AS VARCHAR(255)) AS clmpracticename,
                CAST($1:clmother::TEXT AS VARCHAR(255)) AS clmother,
                CAST($1:clmworkername::TEXT AS VARCHAR(255)) AS clmworkername,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:ahrrnumber::TEXT AS VARCHAR(255)) AS ahrrnumber,
                CAST($1:documentindentifier::TEXT AS VARCHAR(255)) AS documentindentifier,
                CAST($1:clmsiranumber::TEXT AS VARCHAR(255)) AS clmsiranumber,
                $1:clmaccexercisephysiologist::BOOLEAN AS clmaccexercisephysiologist,
                $1:clmdateofbirth::TIMESTAMP_TZ AS clmdateofbirth,
                CAST($1:clmpracticephonenumber::TEXT AS VARCHAR(255)) AS clmpracticephonenumber,
                $1:clmosteopath::BOOLEAN AS clmosteopath,
                $1:clmpsychologist::BOOLEAN AS clmpsychologist,
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
            FROM {{ source('gwcc', 'ccx_ahrr_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS ahrr_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'ahrrenddate',
                        'publicid',
                        'clmpracticesuburb',
                        'claimnumber',
                        'createtime',
                        'ahrrstartdate',
                        'clmreferredby',
                        'clmserviceprovidername',
                        'clmpracticestate',
                        'clmreferrerphonenumber',
                        'clmworkerphonenumber',
                        'updatetime',
                        'clmchiropractor',
                        'claimid',
                        'clmpracticepostalcode',
                        'clmtreatmentcommenced',
                        'clmpracticeemail',
                        'clmnosessionprovidedtodate',
                        'clmcounsellor',
                        'clmphysiotherapist',
                        'clmdateofinjury',
                        'createuserid',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'dateofrequest',
                        'clmpracticename',
                        'clmother',
                        'clmworkername',
                        'updateuserid',
                        'ahrrnumber',
                        'documentindentifier',
                        'clmsiranumber',
                        'clmaccexercisephysiologist',
                        'clmdateofbirth',
                        'clmpracticephonenumber',
                        'clmosteopath',
                        'clmpsychologist'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}