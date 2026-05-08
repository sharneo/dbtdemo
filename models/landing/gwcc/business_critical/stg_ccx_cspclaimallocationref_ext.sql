{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_cspclaimallocationref_ext.
                                                cspclaimallocationref_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "ccx_cspclaimallocationref_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:GroupNumber::NUMBER AS groupnumber,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:EndDate::NUMBER/1000) AS enddate,
                CAST(data_payload:GroupName::TEXT AS VARCHAR(255)) AS groupname,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:StartDate::NUMBER/1000) AS startdate,
                data_payload:AllocationSegment::NUMBER AS allocationsegment,
                data_payload:AllocationStrategy::NUMBER AS allocationstrategy,
                data_payload:AssignedUserID::NUMBER AS assigneduserid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:IsIneffective::BOOLEAN AS isineffective,
                data_payload:AllocationType::NUMBER AS allocationtype,
                data_payload:GroupID::NUMBER AS groupid,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(30)) AS policynumber,
                data_payload:CSPID::NUMBER AS cspid,
                CAST(data_payload:PolicyName::TEXT AS VARCHAR(255)) AS policyname,
                CAST(data_payload:AgencyName::TEXT AS VARCHAR(50)) AS agencyname,
                CAST(data_payload:ReferenceCostCentreCode::TEXT AS VARCHAR(30)) AS referencecostcentrecode,
                CAST(data_payload:AgencyCode::TEXT AS VARCHAR(30)) AS agencycode,
                data_payload:ClaimType::NUMBER AS claimtype,
                CAST(data_payload:ReferenceCostCentreName::TEXT AS VARCHAR(50)) AS referencecostcentrename,
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
            FROM {{ source('gwcc', 'ccx_cspclaimallocationref_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:groupnumber::NUMBER AS groupnumber,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:enddate::TIMESTAMP_TZ AS enddate,
                CAST($1:groupname::TEXT AS VARCHAR(255)) AS groupname,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:startdate::TIMESTAMP_TZ AS startdate,
                $1:allocationsegment::NUMBER AS allocationsegment,
                $1:allocationstrategy::NUMBER AS allocationstrategy,
                $1:assigneduserid::NUMBER AS assigneduserid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:isineffective::BOOLEAN AS isineffective,
                $1:allocationtype::NUMBER AS allocationtype,
                $1:groupid::NUMBER AS groupid,
                $1:id::NUMBER AS id,
                CAST($1:policynumber::TEXT AS VARCHAR(30)) AS policynumber,
                $1:cspid::NUMBER AS cspid,
                CAST($1:policyname::TEXT AS VARCHAR(255)) AS policyname,
                CAST($1:agencyname::TEXT AS VARCHAR(50)) AS agencyname,
                CAST($1:referencecostcentrecode::TEXT AS VARCHAR(30)) AS referencecostcentrecode,
                CAST($1:agencycode::TEXT AS VARCHAR(30)) AS agencycode,
                $1:claimtype::NUMBER AS claimtype,
                CAST($1:referencecostcentrename::TEXT AS VARCHAR(50)) AS referencecostcentrename,
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
            FROM {{ source('gwcc', 'ccx_cspclaimallocationref_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS cspclaimallocationref_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'beanversion',
                        'groupnumber',
                        'createtime',
                        'enddate',
                        'groupname',
                        'updateuserid',
                        'startdate',
                        'allocationsegment',
                        'allocationstrategy',
                        'assigneduserid',
                        'updatetime',
                        'isineffective',
                        'allocationtype',
                        'groupid',
                        'policynumber',
                        'cspid',
                        'policyname',
                        'agencyname',
                        'referencecostcentrecode',
                        'agencycode',
                        'claimtype',
                        'referencecostcentrename'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}