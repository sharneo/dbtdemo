{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_triagequestions_icare.
                                                triagequestions_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_triagequestions_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:AdmittedToHospitalText::TEXT AS VARCHAR(1000)) AS admittedtohospitaltext,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:ThereIsSupport::BOOLEAN AS thereissupport,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:FeelsInControlOfRecoveryText::TEXT AS VARCHAR(1000)) AS feelsincontrolofrecoverytext,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:InjuryConcerns::BOOLEAN AS injuryconcerns,
                data_payload:PersonReturnEstimation::NUMBER AS personreturnestimation,
                CAST(data_payload:InjuredMotivatedRTWText::TEXT AS VARCHAR(1000)) AS injuredmotivatedrtwtext,
                data_payload:AdmittedToHospital::NUMBER AS admittedtohospital,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:FeelsInControlOfRecovery::BOOLEAN AS feelsincontrolofrecovery,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:IsInjuredCurrentlyWorking::BOOLEAN AS isinjuredcurrentlyworking,
                data_payload:InjuredMotivatedRTW::NUMBER AS injuredmotivatedrtw,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:AdditionalHealthConditionsText::TEXT AS VARCHAR(1000)) AS additionalhealthconditionstext,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:AbleToUseTransportationText::TEXT AS VARCHAR(1000)) AS abletousetransportationtext,
                CAST(data_payload:FactorsAffectingRecoveryText::TEXT AS VARCHAR(1000)) AS factorsaffectingrecoverytext,
                CAST(data_payload:AbleProvideSuitableWorkText::TEXT AS VARCHAR(1000)) AS ableprovidesuitableworktext,
                CAST(data_payload:ThereIsSupportText::TEXT AS VARCHAR(1000)) AS thereissupporttext,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:InjuryConcernsText::TEXT AS VARCHAR(1000)) AS injuryconcernstext,
                data_payload:ConcernsWithEmpOrEnv::BOOLEAN AS concernswithemporenv,
                data_payload:AdditionalHealthConditions::BOOLEAN AS additionalhealthconditions,
                data_payload:AbleToUseTransportation::BOOLEAN AS abletousetransportation,
                data_payload:FactorsAffectingRecovery::NUMBER AS factorsaffectingrecovery,
                data_payload:ClinicalPresentationConsistent::BOOLEAN AS clinicalpresentationconsistent,
                data_payload:AbleProvideSuitableWork::NUMBER AS ableprovidesuitablework,
                CAST(data_payload:NeedWorkplaceRehabSvcText::TEXT AS VARCHAR(1000)) AS needworkplacerehabsvctext,
                CAST(data_payload:NeedSupportForInjWorkerText::TEXT AS VARCHAR(1000)) AS needsupportforinjworkertext,
                data_payload:NeedWorkplaceRehabSvc::NUMBER AS needworkplacerehabsvc,
                data_payload:NeedSupportForInjWorker::NUMBER AS needsupportforinjworker,
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
            FROM {{ source('gwcc', 'ccx_triagequestions_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:admittedtohospitaltext::TEXT AS VARCHAR(1000)) AS admittedtohospitaltext,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:thereissupport::BOOLEAN AS thereissupport,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:feelsincontrolofrecoverytext::TEXT AS VARCHAR(1000)) AS feelsincontrolofrecoverytext,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:injuryconcerns::BOOLEAN AS injuryconcerns,
                $1:personreturnestimation::NUMBER AS personreturnestimation,
                CAST($1:injuredmotivatedrtwtext::TEXT AS VARCHAR(1000)) AS injuredmotivatedrtwtext,
                $1:admittedtohospital::NUMBER AS admittedtohospital,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:feelsincontrolofrecovery::BOOLEAN AS feelsincontrolofrecovery,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                $1:isinjuredcurrentlyworking::BOOLEAN AS isinjuredcurrentlyworking,
                $1:injuredmotivatedrtw::NUMBER AS injuredmotivatedrtw,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:additionalhealthconditionstext::TEXT AS VARCHAR(1000)) AS additionalhealthconditionstext,
                $1:retired::NUMBER AS retired,
                CAST($1:abletousetransportationtext::TEXT AS VARCHAR(1000)) AS abletousetransportationtext,
                CAST($1:factorsaffectingrecoverytext::TEXT AS VARCHAR(1000)) AS factorsaffectingrecoverytext,
                CAST($1:ableprovidesuitableworktext::TEXT AS VARCHAR(1000)) AS ableprovidesuitableworktext,
                CAST($1:thereissupporttext::TEXT AS VARCHAR(1000)) AS thereissupporttext,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:injuryconcernstext::TEXT AS VARCHAR(1000)) AS injuryconcernstext,
                $1:concernswithemporenv::BOOLEAN AS concernswithemporenv,
                $1:additionalhealthconditions::BOOLEAN AS additionalhealthconditions,
                $1:abletousetransportation::BOOLEAN AS abletousetransportation,
                $1:factorsaffectingrecovery::NUMBER AS factorsaffectingrecovery,
                $1:clinicalpresentationconsistent::BOOLEAN AS clinicalpresentationconsistent,
                $1:ableprovidesuitablework::NUMBER AS ableprovidesuitablework,
                CAST($1:needworkplacerehabsvctext::TEXT AS VARCHAR(1000)) AS needworkplacerehabsvctext,
                CAST($1:needsupportforinjworkertext::TEXT AS VARCHAR(1000)) AS needsupportforinjworkertext,
                $1:needworkplacerehabsvc::NUMBER AS needworkplacerehabsvc,
                $1:needsupportforinjworker::NUMBER AS needsupportforinjworker,
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
            FROM {{ source('gwcc', 'ccx_triagequestions_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS triagequestions_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'admittedtohospitaltext',
                        'loadcommandid',
                        'thereissupport',
                        'publicid',
                        'feelsincontrolofrecoverytext',
                        'createtime',
                        'injuryconcerns',
                        'personreturnestimation',
                        'injuredmotivatedrtwtext',
                        'admittedtohospital',
                        'updatetime',
                        'feelsincontrolofrecovery',
                        'createuserid',
                        'isinjuredcurrentlyworking',
                        'injuredmotivatedrtw',
                        'beanversion',
                        'additionalhealthconditionstext',
                        'retired',
                        'abletousetransportationtext',
                        'factorsaffectingrecoverytext',
                        'ableprovidesuitableworktext',
                        'thereissupporttext',
                        'updateuserid',
                        'injuryconcernstext',
                        'concernswithemporenv',
                        'additionalhealthconditions',
                        'abletousetransportation',
                        'factorsaffectingrecovery',
                        'clinicalpresentationconsistent',
                        'ableprovidesuitablework',
                        'needworkplacerehabsvctext',
                        'needsupportforinjworkertext',
                        'needworkplacerehabsvc',
                        'needsupportforinjworker'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}