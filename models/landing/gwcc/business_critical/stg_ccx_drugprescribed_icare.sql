{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_drugprescribed_icare.
                                                drugprescribed_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "ccx_drugprescribed_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:CostItem AS NUMBER(18,2)) AS costitem,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:EndDate::NUMBER/1000) AS enddate,
                data_payload:DocumentLinkableID::NUMBER AS documentlinkableid,
                CAST(data_payload:DrugName::TEXT AS VARCHAR(256)) AS drugname,
                CAST(data_payload:DrugQuantity::TEXT AS VARCHAR(60)) AS drugquantity,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:StartDate::NUMBER/1000) AS startdate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:DrugDosage::TEXT AS VARCHAR(60)) AS drugdosage,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:ID::NUMBER AS id,
                data_payload:PrescribingPhysicianID::NUMBER AS prescribingphysicianid,
                data_payload:DrugFrequency::NUMBER AS drugfrequency,
                data_payload:IMPInclude::BOOLEAN AS impinclude,
                data_payload:DrugCategory::NUMBER AS drugcategory,
                TO_TIMESTAMP_TZ(data_payload:DatePrescriptionDispensed::NUMBER/1000) AS dateprescriptiondispensed,
                data_payload:PharmacyID::NUMBER AS pharmacyid,
                data_payload:Repeats::NUMBER AS repeats,
                CAST(data_payload:ScriptNumber::TEXT AS VARCHAR(60)) AS scriptnumber,
                data_payload:PrescriptionType::NUMBER AS prescriptiontype,
                CAST(data_payload:LastEditedBy::TEXT AS VARCHAR(255)) AS lasteditedby,
                CAST(data_payload:MedicationName::TEXT AS VARCHAR(256)) AS medicationname,
                CAST(data_payload:ReasonForDecision::TEXT AS VARCHAR(16777216)) AS reasonfordecision,
                TO_TIMESTAMP_TZ(data_payload:DateApproved::NUMBER/1000) AS dateapproved,
                data_payload:PaycodeID::NUMBER AS paycodeid,
                data_payload:ApprovalStatus::NUMBER AS approvalstatus,
                CAST(data_payload:ReviewRequiredReason::TEXT AS VARCHAR(255)) AS reviewrequiredreason,
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
            FROM {{ source('gwcc', 'ccx_drugprescribed_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:costitem AS NUMBER(18,2)) AS costitem,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:enddate::TIMESTAMP_TZ AS enddate,
                $1:documentlinkableid::NUMBER AS documentlinkableid,
                CAST($1:drugname::TEXT AS VARCHAR(256)) AS drugname,
                CAST($1:drugquantity::TEXT AS VARCHAR(60)) AS drugquantity,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:startdate::TIMESTAMP_TZ AS startdate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:drugdosage::TEXT AS VARCHAR(60)) AS drugdosage,
                $1:claimid::NUMBER AS claimid,
                $1:id::NUMBER AS id,
                $1:prescribingphysicianid::NUMBER AS prescribingphysicianid,
                $1:drugfrequency::NUMBER AS drugfrequency,
                $1:impinclude::BOOLEAN AS impinclude,
                $1:drugcategory::NUMBER AS drugcategory,
                $1:dateprescriptiondispensed::TIMESTAMP_TZ AS dateprescriptiondispensed,
                $1:pharmacyid::NUMBER AS pharmacyid,
                $1:repeats::NUMBER AS repeats,
                CAST($1:scriptnumber::TEXT AS VARCHAR(60)) AS scriptnumber,
                $1:prescriptiontype::NUMBER AS prescriptiontype,
                CAST($1:lasteditedby::TEXT AS VARCHAR(255)) AS lasteditedby,
                CAST($1:medicationname::TEXT AS VARCHAR(256)) AS medicationname,
                CAST($1:reasonfordecision::TEXT AS VARCHAR(16777216)) AS reasonfordecision,
                $1:dateapproved::TIMESTAMP_TZ AS dateapproved,
                $1:paycodeid::NUMBER AS paycodeid,
                $1:approvalstatus::NUMBER AS approvalstatus,
                CAST($1:reviewrequiredreason::TEXT AS VARCHAR(255)) AS reviewrequiredreason,
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
            FROM {{ source('gwcc', 'ccx_drugprescribed_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS drugprescribed_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'costitem',
                        'createuserid',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'enddate',
                        'documentlinkableid',
                        'drugname',
                        'drugquantity',
                        'updateuserid',
                        'startdate',
                        'updatetime',
                        'drugdosage',
                        'claimid',
                        'prescribingphysicianid',
                        'drugfrequency',
                        'impinclude',
                        'drugcategory',
                        'dateprescriptiondispensed',
                        'pharmacyid',
                        'repeats',
                        'scriptnumber',
                        'prescriptiontype',
                        'lasteditedby',
                        'medicationname',
                        'reasonfordecision',
                        'dateapproved',
                        'paycodeid',
                        'approvalstatus',
                        'reviewrequiredreason'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}