{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_medpersontreatment_icare.
                                                medpersontreatment_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "ccx_medpersontreatment_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:AutoCreatedAHRR::BOOLEAN AS autocreatedahrr,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:ICD1ID::NUMBER AS icd1id,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:EndDate::NUMBER/1000) AS enddate,
                data_payload:DocumentLinkableID::NUMBER AS documentlinkableid,
                data_payload:TreatmentQuantityRequested::NUMBER AS treatmentquantityrequested,
                CAST(data_payload:Cost AS NUMBER(18,2)) AS cost,
                CAST(data_payload:Hours AS NUMBER(9,2)) AS hours,
                TO_TIMESTAMP_TZ(data_payload:StartDate::NUMBER/1000) AS startdate,
                CAST(data_payload:TotalCost AS NUMBER(18,2)) AS totalcost,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:ID::NUMBER AS id,
                data_payload:Frequency::NUMBER AS frequency,
                data_payload:IMPInclude::BOOLEAN AS impinclude,
                CAST(data_payload:OriginalRequest::TEXT AS VARCHAR(30)) AS originalrequest,
                data_payload:ODGFlag::NUMBER AS odgflag,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:DateApproved::NUMBER/1000) AS dateapproved,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:Units::NUMBER AS units,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:FailedAHRRDocId::TEXT AS VARCHAR(255)) AS failedahrrdocid,
                data_payload:PaycodeID::NUMBER AS paycodeid,
                CAST(data_payload:TreatmentType::TEXT AS VARCHAR(255)) AS treatmenttype,
                data_payload:ODGMax::NUMBER AS odgmax,
                data_payload:FrequencyCount::NUMBER AS frequencycount,
                data_payload:ApprovalStatus::NUMBER AS approvalstatus,
                data_payload:TreatmentQuantityApproved::NUMBER AS treatmentquantityapproved,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ContactID::NUMBER AS contactid,
                CAST(data_payload:ReviewRequiredReason::TEXT AS VARCHAR(255)) AS reviewrequiredreason,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
                TO_TIMESTAMP_TZ(data_payload:RequestDate::NUMBER/1000) AS requestdate,
                CAST(data_payload:HourlyCost AS NUMBER(18,2)) AS hourlycost,
                data_payload:Category::NUMBER AS category,
                CAST(data_payload:SurgeryCost AS NUMBER(18,2)) AS surgerycost,
                data_payload:SurgeryGroupID::NUMBER AS surgerygroupid,
                CAST(data_payload:HICSIRAHospitalNumber::TEXT AS VARCHAR(255)) AS hicsirahospitalnumber,
                TO_TIMESTAMP_TZ(data_payload:DateOfSurgery::NUMBER/1000) AS dateofsurgery,
                data_payload:NoOfNightsInHospital::NUMBER AS noofnightsinhospital,
                CAST(data_payload:MinutesOfOperatingTime::TEXT AS VARCHAR(255)) AS minutesofoperatingtime,
                data_payload:AutoCalcSurgeryCost::BOOLEAN AS autocalcsurgerycost,
                data_payload:IsSelectedForMSP::BOOLEAN AS isselectedformsp,
                CAST(data_payload:LastEditedBy::TEXT AS VARCHAR(255)) AS lasteditedby,
                CAST(data_payload:ReasonForDecision::TEXT AS VARCHAR(16777216)) AS reasonfordecision,
                CAST(data_payload:MedProviderID::TEXT AS VARCHAR(255)) AS medproviderid,
                data_payload:FractureOrDislocation::BOOLEAN AS fractureordislocation,
                CAST(data_payload:ReasonForCostVarianceSurgery::TEXT AS VARCHAR(16777216)) AS reasonforcostvariancesurgery,
                CAST(data_payload:ReasonForCostVarianceHospital::TEXT AS VARCHAR(16777216)) AS reasonforcostvariancehospital,
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
            FROM {{ source('gwcc', 'ccx_medpersontreatment_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:autocreatedahrr::BOOLEAN AS autocreatedahrr,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:icd1id::NUMBER AS icd1id,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:enddate::TIMESTAMP_TZ AS enddate,
                $1:documentlinkableid::NUMBER AS documentlinkableid,
                $1:treatmentquantityrequested::NUMBER AS treatmentquantityrequested,
                CAST($1:cost AS NUMBER(18,2)) AS cost,
                CAST($1:hours AS NUMBER(9,2)) AS hours,
                $1:startdate::TIMESTAMP_TZ AS startdate,
                CAST($1:totalcost AS NUMBER(18,2)) AS totalcost,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:id::NUMBER AS id,
                $1:frequency::NUMBER AS frequency,
                $1:impinclude::BOOLEAN AS impinclude,
                CAST($1:originalrequest::TEXT AS VARCHAR(30)) AS originalrequest,
                $1:odgflag::NUMBER AS odgflag,
                $1:createuserid::NUMBER AS createuserid,
                $1:dateapproved::TIMESTAMP_TZ AS dateapproved,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:units::NUMBER AS units,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:failedahrrdocid::TEXT AS VARCHAR(255)) AS failedahrrdocid,
                $1:paycodeid::NUMBER AS paycodeid,
                CAST($1:treatmenttype::TEXT AS VARCHAR(255)) AS treatmenttype,
                $1:odgmax::NUMBER AS odgmax,
                $1:frequencycount::NUMBER AS frequencycount,
                $1:approvalstatus::NUMBER AS approvalstatus,
                $1:treatmentquantityapproved::NUMBER AS treatmentquantityapproved,
                $1:subtype::NUMBER AS subtype,
                $1:contactid::NUMBER AS contactid,
                CAST($1:reviewrequiredreason::TEXT AS VARCHAR(255)) AS reviewrequiredreason,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
                $1:requestdate::TIMESTAMP_TZ AS requestdate,
                CAST($1:hourlycost AS NUMBER(18,2)) AS hourlycost,
                $1:category::NUMBER AS category,
                CAST($1:surgerycost AS NUMBER(18,2)) AS surgerycost,
                $1:surgerygroupid::NUMBER AS surgerygroupid,
                CAST($1:hicsirahospitalnumber::TEXT AS VARCHAR(255)) AS hicsirahospitalnumber,
                $1:dateofsurgery::TIMESTAMP_TZ AS dateofsurgery,
                $1:noofnightsinhospital::NUMBER AS noofnightsinhospital,
                CAST($1:minutesofoperatingtime::TEXT AS VARCHAR(255)) AS minutesofoperatingtime,
                $1:autocalcsurgerycost::BOOLEAN AS autocalcsurgerycost,
                $1:isselectedformsp::BOOLEAN AS isselectedformsp,
                CAST($1:lasteditedby::TEXT AS VARCHAR(255)) AS lasteditedby,
                CAST($1:reasonfordecision::TEXT AS VARCHAR(16777216)) AS reasonfordecision,
                CAST($1:medproviderid::TEXT AS VARCHAR(255)) AS medproviderid,
                $1:fractureordislocation::BOOLEAN AS fractureordislocation,
                CAST($1:reasonforcostvariancesurgery::TEXT AS VARCHAR(16777216)) AS reasonforcostvariancesurgery,
                CAST($1:reasonforcostvariancehospital::TEXT AS VARCHAR(16777216)) AS reasonforcostvariancehospital,
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
            FROM {{ source('gwcc', 'ccx_medpersontreatment_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS medpersontreatment_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'autocreatedahrr',
                        'loadcommandid',
                        'icd1id',
                        'publicid',
                        'createtime',
                        'enddate',
                        'documentlinkableid',
                        'treatmentquantityrequested',
                        'cost',
                        'hours',
                        'startdate',
                        'totalcost',
                        'updatetime',
                        'claimid',
                        'frequency',
                        'impinclude',
                        'originalrequest',
                        'odgflag',
                        'createuserid',
                        'dateapproved',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'units',
                        'updateuserid',
                        'failedahrrdocid',
                        'paycodeid',
                        'treatmenttype',
                        'odgmax',
                        'frequencycount',
                        'approvalstatus',
                        'treatmentquantityapproved',
                        'subtype',
                        'contactid',
                        'reviewrequiredreason',
                        'description',
                        'requestdate',
                        'hourlycost',
                        'category',
                        'surgerycost',
                        'surgerygroupid',
                        'hicsirahospitalnumber',
                        'dateofsurgery',
                        'noofnightsinhospital',
                        'minutesofoperatingtime',
                        'autocalcsurgerycost',
                        'isselectedformsp',
                        'lasteditedby',
                        'reasonfordecision',
                        'medproviderid',
                        'fractureordislocation',
                        'reasonforcostvariancesurgery',
                        'reasonforcostvariancehospital'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}