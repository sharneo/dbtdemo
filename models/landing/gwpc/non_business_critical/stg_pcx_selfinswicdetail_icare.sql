{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_selfinswicdetail_icare.
                                                selfinswicdetail_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "non_business_critical", "pcx_selfinswicdetail_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:InitialCoveragesCreated::BOOLEAN AS initialcoveragescreated,
                data_payload:FCTApprenticeWages::NUMBER AS fctapprenticewages,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Total::NUMBER AS total,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:ESTGrossWages::NUMBER AS estgrosswages,
                data_payload:FixedID::NUMBER AS fixedid,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:CostCenter_icare::NUMBER AS costcenter_icare,
                data_payload:FCTGrossWages::NUMBER AS fctgrosswages,
                data_payload:InitialExclusionsCreated::BOOLEAN AS initialexclusionscreated,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:SelfInsurerWIC::NUMBER AS selfinsurerwic,
                data_payload:ESTNoOfEmployees::NUMBER AS estnoofemployees,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ChangeType::NUMBER AS changetype,
                data_payload:InitialConditionsCreated::BOOLEAN AS initialconditionscreated,
                data_payload:BasedOnID::NUMBER AS basedonid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:ESTApprenticeWages::NUMBER AS estapprenticewages,
                data_payload:location::NUMBER AS location,
                data_payload:FCTNoOfEmployees::NUMBER AS fctnoofemployees,
                TO_TIMESTAMP_TZ(data_payload:ReferenceDateInternal::NUMBER/1000) AS referencedateinternal,
                CAST(data_payload:OccupationCode::TEXT AS VARCHAR(60)) AS occupationcode,
                data_payload:PreferredCoverageCurrency::NUMBER AS preferredcoveragecurrency,
                data_payload:WCLine_icare::NUMBER AS wcline_icare,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
                data_payload:BranchID::NUMBER AS branchid,
                CAST(data_payload:Category::TEXT AS VARCHAR(255)) AS category,
                data_payload:TMFWICCodeID::NUMBER AS tmfwiccodeid,
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
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_selfinswicdetail_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:initialcoveragescreated::BOOLEAN AS initialcoveragescreated,
                $1:fctapprenticewages::NUMBER AS fctapprenticewages,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:total::NUMBER AS total,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:estgrosswages::NUMBER AS estgrosswages,
                $1:fixedid::NUMBER AS fixedid,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:costcenter_icare::NUMBER AS costcenter_icare,
                $1:fctgrosswages::NUMBER AS fctgrosswages,
                $1:initialexclusionscreated::BOOLEAN AS initialexclusionscreated,
                $1:createuserid::NUMBER AS createuserid,
                $1:selfinsurerwic::NUMBER AS selfinsurerwic,
                $1:estnoofemployees::NUMBER AS estnoofemployees,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:changetype::NUMBER AS changetype,
                $1:initialconditionscreated::BOOLEAN AS initialconditionscreated,
                $1:basedonid::NUMBER AS basedonid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:estapprenticewages::NUMBER AS estapprenticewages,
                $1:location::NUMBER AS location,
                $1:fctnoofemployees::NUMBER AS fctnoofemployees,
                $1:referencedateinternal::TIMESTAMP_TZ AS referencedateinternal,
                CAST($1:occupationcode::TEXT AS VARCHAR(60)) AS occupationcode,
                $1:preferredcoveragecurrency::NUMBER AS preferredcoveragecurrency,
                $1:wcline_icare::NUMBER AS wcline_icare,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
                $1:branchid::NUMBER AS branchid,
                CAST($1:category::TEXT AS VARCHAR(255)) AS category,
                $1:tmfwiccodeid::NUMBER AS tmfwiccodeid,
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
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_selfinswicdetail_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS selfinswicdetail_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'initialcoveragescreated',
                        'fctapprenticewages',
                        'publicid',
                        'total',
                        'createtime',
                        'estgrosswages',
                        'fixedid',
                        'effectivedate',
                        'updatetime',
                        'expirationdate',
                        'costcenter_icare',
                        'fctgrosswages',
                        'initialexclusionscreated',
                        'createuserid',
                        'selfinsurerwic',
                        'estnoofemployees',
                        'archivepartition',
                        'beanversion',
                        'changetype',
                        'initialconditionscreated',
                        'basedonid',
                        'updateuserid',
                        'estapprenticewages',
                        'location',
                        'fctnoofemployees',
                        'referencedateinternal',
                        'occupationcode',
                        'preferredcoveragecurrency',
                        'wcline_icare',
                        'description',
                        'branchid',
                        'category',
                        'tmfwiccodeid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}