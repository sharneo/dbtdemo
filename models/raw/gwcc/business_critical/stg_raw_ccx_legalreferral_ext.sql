{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_legalreferral_ext.
                                                legalreferral_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "ccx_legalreferral_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:CostApproved AS NUMBER(18,2)) AS costapproved,
                CAST(data_payload:OtherReferralOutcome::TEXT AS VARCHAR(255)) AS otherreferraloutcome,
                data_payload:ReferrerNameID::NUMBER AS referrernameid,
                data_payload:IsInsurerLSProvider::BOOLEAN AS isinsurerlsprovider,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:ReferralEndDate::NUMBER/1000) AS referralenddate,
                data_payload:ReferralStatus::NUMBER AS referralstatus,
                CAST(data_payload:LRAssignedTo::TEXT AS VARCHAR(255)) AS lrassignedto,
                TO_TIMESTAMP_TZ(data_payload:ReferralStartDate::NUMBER/1000) AS referralstartdate,
                data_payload:ReferralOutcome::NUMBER AS referraloutcome,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:ReferralType::NUMBER AS referraltype,
                data_payload:ID::NUMBER AS id,
                data_payload:IsWorkerLSProvider::BOOLEAN AS isworkerlsprovider,
                data_payload:ReferralReason::NUMBER AS referralreason,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:DateOfReferral::NUMBER/1000) AS dateofreferral,
                CAST(data_payload:CostInvoiced AS NUMBER(18,2)) AS costinvoiced,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:LRAssignedToGroupID::NUMBER AS lrassignedtogroupid,
                data_payload:IsInsurerPrefEmployer::BOOLEAN AS isinsurerprefemployer,
                CAST(data_payload:WorkerLawFirmName::TEXT AS VARCHAR(255)) AS workerlawfirmname,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:OtherReferralReason::TEXT AS VARCHAR(255)) AS otherreferralreason,
                data_payload:InsurerLawFirmName::NUMBER AS insurerlawfirmname,
                data_payload:LRAssignedToUserID::NUMBER AS lrassignedtouserid,
                TO_TIMESTAMP_TZ(data_payload:WorkerDateEngaged::NUMBER/1000) AS workerdateengaged,
                TO_TIMESTAMP_TZ(data_payload:InsurerDateEngaged::NUMBER/1000) AS insurerdateengaged,
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
            FROM {{ source('gwcc', 'ccx_legalreferral_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:costapproved AS NUMBER(18,2)) AS costapproved,
                CAST($1:otherreferraloutcome::TEXT AS VARCHAR(255)) AS otherreferraloutcome,
                $1:referrernameid::NUMBER AS referrernameid,
                $1:isinsurerlsprovider::BOOLEAN AS isinsurerlsprovider,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:referralenddate::TIMESTAMP_TZ AS referralenddate,
                $1:referralstatus::NUMBER AS referralstatus,
                CAST($1:lrassignedto::TEXT AS VARCHAR(255)) AS lrassignedto,
                $1:referralstartdate::TIMESTAMP_TZ AS referralstartdate,
                $1:referraloutcome::NUMBER AS referraloutcome,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:referraltype::NUMBER AS referraltype,
                $1:id::NUMBER AS id,
                $1:isworkerlsprovider::BOOLEAN AS isworkerlsprovider,
                $1:referralreason::NUMBER AS referralreason,
                $1:createuserid::NUMBER AS createuserid,
                $1:dateofreferral::TIMESTAMP_TZ AS dateofreferral,
                CAST($1:costinvoiced AS NUMBER(18,2)) AS costinvoiced,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:lrassignedtogroupid::NUMBER AS lrassignedtogroupid,
                $1:isinsurerprefemployer::BOOLEAN AS isinsurerprefemployer,
                CAST($1:workerlawfirmname::TEXT AS VARCHAR(255)) AS workerlawfirmname,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:otherreferralreason::TEXT AS VARCHAR(255)) AS otherreferralreason,
                $1:insurerlawfirmname::NUMBER AS insurerlawfirmname,
                $1:lrassignedtouserid::NUMBER AS lrassignedtouserid,
                $1:workerdateengaged::TIMESTAMP_TZ AS workerdateengaged,
                $1:insurerdateengaged::TIMESTAMP_TZ AS insurerdateengaged,
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
            FROM {{ source('gwcc', 'ccx_legalreferral_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS legalreferral_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'costapproved',
                        'otherreferraloutcome',
                        'referrernameid',
                        'isinsurerlsprovider',
                        'createtime',
                        'referralenddate',
                        'referralstatus',
                        'lrassignedto',
                        'referralstartdate',
                        'referraloutcome',
                        'updatetime',
                        'claimid',
                        'referraltype',
                        'isworkerlsprovider',
                        'referralreason',
                        'createuserid',
                        'dateofreferral',
                        'costinvoiced',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'lrassignedtogroupid',
                        'isinsurerprefemployer',
                        'workerlawfirmname',
                        'updateuserid',
                        'otherreferralreason',
                        'insurerlawfirmname',
                        'lrassignedtouserid',
                        'workerdateengaged',
                        'insurerdateengaged'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}