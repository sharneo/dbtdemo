{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_servicereqinstructionsvc.
                                                servicereqinstructionsvc_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "cc_servicereqinstructionsvc"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:HostABN_icare::TEXT AS VARCHAR(60)) AS hostabn_icare,
                CAST(data_payload:PractitionerName_icare::TEXT AS VARCHAR(100)) AS practitionername_icare,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:DateTo_icare::NUMBER/1000) AS dateto_icare,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:AppointmentDate_icare::NUMBER/1000) AS appointmentdate_icare,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:ServiceReqSubtype_icare::NUMBER AS servicereqsubtype_icare,
                data_payload:FailedToAttend_icare::BOOLEAN AS failedtoattend_icare,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:ServiceRequestInstructionID::NUMBER AS servicerequestinstructionid,
                TO_TIMESTAMP_TZ(data_payload:DateFrom_icare::NUMBER/1000) AS datefrom_icare,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:TotalAmount_icare AS NUMBER(18,2)) AS totalamount_icare,
                CAST(data_payload:Description_icare::TEXT AS VARCHAR(100)) AS description_icare,
                data_payload:Sessions_icare::NUMBER AS sessions_icare,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                data_payload:SpecialistServiceID::NUMBER AS specialistserviceid,
                data_payload:ReasonForReferral_icare::NUMBER AS reasonforreferral_icare,
                data_payload:ApprovalStatus_Ext::NUMBER AS approvalstatus_ext,
                CAST(data_payload:ReasonForDecision_Ext::TEXT AS VARCHAR(16777216)) AS reasonfordecision_ext,
                TO_TIMESTAMP_TZ(data_payload:DueDate_Ext::NUMBER/1000) AS duedate_ext,
                data_payload:DMServiceRequestInstruction::NUMBER AS dmservicerequestinstruction,
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
            FROM {{ source('gwcc', 'cc_servicereqinstructionsvc') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:hostabn_icare::TEXT AS VARCHAR(60)) AS hostabn_icare,
                CAST($1:practitionername_icare::TEXT AS VARCHAR(100)) AS practitionername_icare,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:dateto_icare::TIMESTAMP_TZ AS dateto_icare,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:appointmentdate_icare::TIMESTAMP_TZ AS appointmentdate_icare,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:servicereqsubtype_icare::NUMBER AS servicereqsubtype_icare,
                $1:failedtoattend_icare::BOOLEAN AS failedtoattend_icare,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:servicerequestinstructionid::NUMBER AS servicerequestinstructionid,
                $1:datefrom_icare::TIMESTAMP_TZ AS datefrom_icare,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:totalamount_icare AS NUMBER(18,2)) AS totalamount_icare,
                CAST($1:description_icare::TEXT AS VARCHAR(100)) AS description_icare,
                $1:sessions_icare::NUMBER AS sessions_icare,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                $1:specialistserviceid::NUMBER AS specialistserviceid,
                $1:reasonforreferral_icare::NUMBER AS reasonforreferral_icare,
                $1:approvalstatus_ext::NUMBER AS approvalstatus_ext,
                CAST($1:reasonfordecision_ext::TEXT AS VARCHAR(16777216)) AS reasonfordecision_ext,
                $1:duedate_ext::TIMESTAMP_TZ AS duedate_ext,
                $1:dmservicerequestinstruction::NUMBER AS dmservicerequestinstruction,
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
            FROM {{ source('gwcc', 'cc_servicereqinstructionsvc') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS servicereqinstructionsvc_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'hostabn_icare',
                        'practitionername_icare',
                        'createuserid',
                        'publicid',
                        'dateto_icare',
                        'beanversion',
                        'archivepartition',
                        'appointmentdate_icare',
                        'createtime',
                        'servicereqsubtype_icare',
                        'failedtoattend_icare',
                        'updateuserid',
                        'servicerequestinstructionid',
                        'datefrom_icare',
                        'updatetime',
                        'totalamount_icare',
                        'description_icare',
                        'sessions_icare',
                        'subtype',
                        'specialistserviceid',
                        'reasonforreferral_icare',
                        'approvalstatus_ext',
                        'reasonfordecision_ext',
                        'duedate_ext',
                        'dmservicerequestinstruction'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}