{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_employmentcapacity_icare.
                                                employmentcapacity_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "ccx_employmentcapacity_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:EngagedInPaidEmployment::BOOLEAN AS engagedinpaidemployment,
                CAST(data_payload:DrivingCapacity::TEXT AS VARCHAR(255)) AS drivingcapacity,
                CAST(data_payload:BendingCapacity::TEXT AS VARCHAR(255)) AS bendingcapacity,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:TreatmentComments1::TEXT AS VARCHAR(255)) AS treatmentcomments1,
                CAST(data_payload:SittingCapacity::TEXT AS VARCHAR(255)) AS sittingcapacity,
                CAST(data_payload:TreatmentComments2::TEXT AS VARCHAR(255)) AS treatmentcomments2,
                CAST(data_payload:StandingCapacity::TEXT AS VARCHAR(255)) AS standingcapacity,
                CAST(data_payload:TreatmentComments3::TEXT AS VARCHAR(255)) AS treatmentcomments3,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:EndDate::NUMBER/1000) AS enddate,
                data_payload:DocumentLinkableID::NUMBER AS documentlinkableid,
                CAST(data_payload:OtherCapacity::TEXT AS VARCHAR(255)) AS othercapacity,
                data_payload:ReferralToRehabProvider::BOOLEAN AS referraltorehabprovider,
                TO_TIMESTAMP_TZ(data_payload:StartDate::NUMBER/1000) AS startdate,
                data_payload:InitialCertificate::BOOLEAN AS initialcertificate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:FactorsDelayingRecovery::TEXT AS VARCHAR(255)) AS factorsdelayingrecovery,
                data_payload:RequestForPositionDescription::BOOLEAN AS requestforpositiondescription,
                CAST(data_payload:DaysPerWeek AS NUMBER(2,1)) AS daysperweek,
                data_payload:ID::NUMBER AS id,
                data_payload:SignatureProvided::BOOLEAN AS signatureprovided,
                CAST(data_payload:Comment::TEXT AS VARCHAR(255)) AS comment,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:TreatmentDuration1::NUMBER AS treatmentduration1,
                data_payload:PreExistingConditions::BOOLEAN AS preexistingconditions,
                CAST(data_payload:TotalHoursPerWeek AS NUMBER(5,2)) AS totalhoursperweek,
                data_payload:TreatmentDuration2::NUMBER AS treatmentduration2,
                data_payload:TreatmentDuration3::NUMBER AS treatmentduration3,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Fitness::NUMBER AS fitness,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:HoursPerDay AS NUMBER(5,2)) AS hoursperday,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:PushingPullingCapacity::TEXT AS VARCHAR(255)) AS pushingpullingcapacity,
                CAST(data_payload:Diagnosis::TEXT AS VARCHAR(255)) AS diagnosis,
                CAST(data_payload:LiftingCapacity::TEXT AS VARCHAR(255)) AS liftingcapacity,
                TO_TIMESTAMP_TZ(data_payload:ConsultationDate::NUMBER/1000) AS consultationdate,
                data_payload:ClaimWorkCompID::NUMBER AS claimworkcompid,
                TO_TIMESTAMP_TZ(data_payload:NextReviewDate::NUMBER/1000) AS nextreviewdate,
                data_payload:InjuryConsistentWithCause::BOOLEAN AS injuryconsistentwithcause,
                data_payload:ContactID::NUMBER AS contactid,
                data_payload:CoCStatus::NUMBER AS cocstatus,
                data_payload:AdditionalInjury::BOOLEAN AS additionalinjury,
                TO_TIMESTAMP_TZ(data_payload:DateCertificateReceived::NUMBER/1000) AS datecertificatereceived,
                data_payload:TreatingMedPractSign::BOOLEAN AS treatingmedpractsign,
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
            FROM {{ source('gwcc', 'ccx_employmentcapacity_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:engagedinpaidemployment::BOOLEAN AS engagedinpaidemployment,
                CAST($1:drivingcapacity::TEXT AS VARCHAR(255)) AS drivingcapacity,
                CAST($1:bendingcapacity::TEXT AS VARCHAR(255)) AS bendingcapacity,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:treatmentcomments1::TEXT AS VARCHAR(255)) AS treatmentcomments1,
                CAST($1:sittingcapacity::TEXT AS VARCHAR(255)) AS sittingcapacity,
                CAST($1:treatmentcomments2::TEXT AS VARCHAR(255)) AS treatmentcomments2,
                CAST($1:standingcapacity::TEXT AS VARCHAR(255)) AS standingcapacity,
                CAST($1:treatmentcomments3::TEXT AS VARCHAR(255)) AS treatmentcomments3,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:enddate::TIMESTAMP_TZ AS enddate,
                $1:documentlinkableid::NUMBER AS documentlinkableid,
                CAST($1:othercapacity::TEXT AS VARCHAR(255)) AS othercapacity,
                $1:referraltorehabprovider::BOOLEAN AS referraltorehabprovider,
                $1:startdate::TIMESTAMP_TZ AS startdate,
                $1:initialcertificate::BOOLEAN AS initialcertificate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:factorsdelayingrecovery::TEXT AS VARCHAR(255)) AS factorsdelayingrecovery,
                $1:requestforpositiondescription::BOOLEAN AS requestforpositiondescription,
                CAST($1:daysperweek AS NUMBER(2,1)) AS daysperweek,
                $1:id::NUMBER AS id,
                $1:signatureprovided::BOOLEAN AS signatureprovided,
                CAST($1:comment::TEXT AS VARCHAR(255)) AS comment,
                $1:createuserid::NUMBER AS createuserid,
                $1:treatmentduration1::NUMBER AS treatmentduration1,
                $1:preexistingconditions::BOOLEAN AS preexistingconditions,
                CAST($1:totalhoursperweek AS NUMBER(5,2)) AS totalhoursperweek,
                $1:treatmentduration2::NUMBER AS treatmentduration2,
                $1:treatmentduration3::NUMBER AS treatmentduration3,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:fitness::NUMBER AS fitness,
                $1:retired::NUMBER AS retired,
                CAST($1:hoursperday AS NUMBER(5,2)) AS hoursperday,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:pushingpullingcapacity::TEXT AS VARCHAR(255)) AS pushingpullingcapacity,
                CAST($1:diagnosis::TEXT AS VARCHAR(255)) AS diagnosis,
                CAST($1:liftingcapacity::TEXT AS VARCHAR(255)) AS liftingcapacity,
                $1:consultationdate::TIMESTAMP_TZ AS consultationdate,
                $1:claimworkcompid::NUMBER AS claimworkcompid,
                $1:nextreviewdate::TIMESTAMP_TZ AS nextreviewdate,
                $1:injuryconsistentwithcause::BOOLEAN AS injuryconsistentwithcause,
                $1:contactid::NUMBER AS contactid,
                $1:cocstatus::NUMBER AS cocstatus,
                $1:additionalinjury::BOOLEAN AS additionalinjury,
                $1:datecertificatereceived::TIMESTAMP_TZ AS datecertificatereceived,
                $1:treatingmedpractsign::BOOLEAN AS treatingmedpractsign,
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
            FROM {{ source('gwcc', 'ccx_employmentcapacity_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS employmentcapacity_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'engagedinpaidemployment',
                        'drivingcapacity',
                        'bendingcapacity',
                        'publicid',
                        'treatmentcomments1',
                        'sittingcapacity',
                        'treatmentcomments2',
                        'standingcapacity',
                        'treatmentcomments3',
                        'createtime',
                        'enddate',
                        'documentlinkableid',
                        'othercapacity',
                        'referraltorehabprovider',
                        'startdate',
                        'initialcertificate',
                        'updatetime',
                        'factorsdelayingrecovery',
                        'requestforpositiondescription',
                        'daysperweek',
                        'signatureprovided',
                        'comment',
                        'createuserid',
                        'treatmentduration1',
                        'preexistingconditions',
                        'totalhoursperweek',
                        'treatmentduration2',
                        'treatmentduration3',
                        'beanversion',
                        'archivepartition',
                        'fitness',
                        'retired',
                        'hoursperday',
                        'updateuserid',
                        'pushingpullingcapacity',
                        'diagnosis',
                        'liftingcapacity',
                        'consultationdate',
                        'claimworkcompid',
                        'nextreviewdate',
                        'injuryconsistentwithcause',
                        'contactid',
                        'cocstatus',
                        'additionalinjury',
                        'datecertificatereceived',
                        'treatingmedpractsign',
                        'legacycreatetime',
                        'legacyupdatetime'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}