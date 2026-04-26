{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_workcomp.
                                                workcomp_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "cc_workcomp"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                TO_TIMESTAMP_TZ(data_payload:DateOfEmployeeRepresentation::NUMBER/1000) AS dateofemployeerepresentation,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:MedicalReport::BOOLEAN AS medicalreport,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:EquipmentUsed::TEXT AS VARCHAR(1333)) AS equipmentused,
                data_payload:ReasonableExcuse_icare::NUMBER AS reasonableexcuse_icare,
                CAST(data_payload:ActivityPerformed::TEXT AS VARCHAR(1333)) AS activityperformed,
                data_payload:AccidentPremises::NUMBER AS accidentpremises,
                data_payload:Compensable::NUMBER AS compensable,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:DiscontinuedFringeBenefits AS NUMBER(18,2)) AS discontinuedfringebenefits,
                data_payload:ID::NUMBER AS id,
                data_payload:OtherTriageQuestions_icareID::NUMBER AS othertriagequestions_icareid,
                TO_TIMESTAMP_TZ(data_payload:FullDenialEffectiveDate::NUMBER/1000) AS fulldenialeffectivedate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:TimeLossReport::BOOLEAN AS timelossreport,
                CAST(data_payload:JurisdictionClaimNumber::TEXT AS VARCHAR(25)) AS jurisdictionclaimnumber,
                CAST(data_payload:InsuredReportNumber::TEXT AS VARCHAR(25)) AS insuredreportnumber,
                data_payload:EmpTriageQuestions_icareID::NUMBER AS emptriagequestions_icareid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                CAST(data_payload:TriageQuestionsSummary_icare::TEXT AS VARCHAR(16777216)) AS triagequestionssummary_icare,
                data_payload:DeathReport::BOOLEAN AS deathreport,
                data_payload:Retired::NUMBER AS retired,
                data_payload:InjTriageQuestions_icareID::NUMBER AS injtriagequestions_icareid,
                data_payload:MedRecReleaseAuth::BOOLEAN AS medrecreleaseauth,
                data_payload:PartialDenialReason::NUMBER AS partialdenialreason,
                data_payload:IllnessRelatedToExposure::BOOLEAN AS illnessrelatedtoexposure,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:WaitingPeriodApplied::BOOLEAN AS waitingperiodapplied,
                data_payload:ClassCodeByLocation::BOOLEAN AS classcodebylocation,
                data_payload:EmployerLiability::BOOLEAN AS employerliability,
                data_payload:InitialTreatment::NUMBER AS initialtreatment,
                data_payload:DocTriageQuestions_icareID::NUMBER AS doctriagequestions_icareid,
                data_payload:AccidentLocationType_icare::NUMBER AS accidentlocationtype_icare,
                CAST(data_payload:RelationshipToTheInjured_icare::TEXT AS VARCHAR(40)) AS relationshiptotheinjured_icare,
                data_payload:OverAllRiskRating_Ext::NUMBER AS overallriskrating_ext,
                data_payload:CMTriageQuestions_ExtID::NUMBER AS cmtriagequestions_extid,
                data_payload:PropertyDamageClaim_Ext::BOOLEAN AS propertydamageclaim_ext,
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
            FROM {{ source('gwcc', 'cc_workcomp') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:dateofemployeerepresentation::TIMESTAMP_TZ AS dateofemployeerepresentation,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:medicalreport::BOOLEAN AS medicalreport,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:equipmentused::TEXT AS VARCHAR(1333)) AS equipmentused,
                $1:reasonableexcuse_icare::NUMBER AS reasonableexcuse_icare,
                CAST($1:activityperformed::TEXT AS VARCHAR(1333)) AS activityperformed,
                $1:accidentpremises::NUMBER AS accidentpremises,
                $1:compensable::NUMBER AS compensable,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:discontinuedfringebenefits AS NUMBER(18,2)) AS discontinuedfringebenefits,
                $1:id::NUMBER AS id,
                $1:othertriagequestions_icareid::NUMBER AS othertriagequestions_icareid,
                $1:fulldenialeffectivedate::TIMESTAMP_TZ AS fulldenialeffectivedate,
                $1:createuserid::NUMBER AS createuserid,
                $1:timelossreport::BOOLEAN AS timelossreport,
                CAST($1:jurisdictionclaimnumber::TEXT AS VARCHAR(25)) AS jurisdictionclaimnumber,
                CAST($1:insuredreportnumber::TEXT AS VARCHAR(25)) AS insuredreportnumber,
                $1:emptriagequestions_icareid::NUMBER AS emptriagequestions_icareid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                CAST($1:triagequestionssummary_icare::TEXT AS VARCHAR(16777216)) AS triagequestionssummary_icare,
                $1:deathreport::BOOLEAN AS deathreport,
                $1:retired::NUMBER AS retired,
                $1:injtriagequestions_icareid::NUMBER AS injtriagequestions_icareid,
                $1:medrecreleaseauth::BOOLEAN AS medrecreleaseauth,
                $1:partialdenialreason::NUMBER AS partialdenialreason,
                $1:illnessrelatedtoexposure::BOOLEAN AS illnessrelatedtoexposure,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:waitingperiodapplied::BOOLEAN AS waitingperiodapplied,
                $1:classcodebylocation::BOOLEAN AS classcodebylocation,
                $1:employerliability::BOOLEAN AS employerliability,
                $1:initialtreatment::NUMBER AS initialtreatment,
                $1:doctriagequestions_icareid::NUMBER AS doctriagequestions_icareid,
                $1:accidentlocationtype_icare::NUMBER AS accidentlocationtype_icare,
                CAST($1:relationshiptotheinjured_icare::TEXT AS VARCHAR(40)) AS relationshiptotheinjured_icare,
                $1:overallriskrating_ext::NUMBER AS overallriskrating_ext,
                $1:cmtriagequestions_extid::NUMBER AS cmtriagequestions_extid,
                $1:propertydamageclaim_ext::BOOLEAN AS propertydamageclaim_ext,
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
            FROM {{ source('gwcc', 'cc_workcomp') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS workcomp_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'dateofemployeerepresentation',
                        'publicid',
                        'medicalreport',
                        'createtime',
                        'equipmentused',
                        'reasonableexcuse_icare',
                        'activityperformed',
                        'accidentpremises',
                        'compensable',
                        'updatetime',
                        'discontinuedfringebenefits',
                        'othertriagequestions_icareid',
                        'fulldenialeffectivedate',
                        'createuserid',
                        'timelossreport',
                        'jurisdictionclaimnumber',
                        'insuredreportnumber',
                        'emptriagequestions_icareid',
                        'beanversion',
                        'archivepartition',
                        'triagequestionssummary_icare',
                        'deathreport',
                        'retired',
                        'injtriagequestions_icareid',
                        'medrecreleaseauth',
                        'partialdenialreason',
                        'illnessrelatedtoexposure',
                        'updateuserid',
                        'waitingperiodapplied',
                        'classcodebylocation',
                        'employerliability',
                        'initialtreatment',
                        'doctriagequestions_icareid',
                        'accidentlocationtype_icare',
                        'relationshiptotheinjured_icare',
                        'overallriskrating_ext',
                        'cmtriagequestions_extid',
                        'propertydamageclaim_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}