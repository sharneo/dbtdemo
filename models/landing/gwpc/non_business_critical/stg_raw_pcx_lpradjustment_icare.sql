{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_lpradjustment_icare.
                                                lpradjustment_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_policy_centre", "policy_centre", "non_business_critical", "pcx_lpradjustment_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:GrpCostOfClaims_cur::NUMBER AS grpcostofclaims_cur,
                data_payload:EffectiveDatedFieldsID::NUMBER AS effectivedatedfieldsid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:GrpCostOfClaims_amt AS NUMBER(18,2)) AS grpcostofclaims_amt,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:FixedID::NUMBER AS fixedid,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:LegacyGrpCostOfClaims_cur::NUMBER AS legacygrpcostofclaims_cur,
                data_payload:ID::NUMBER AS id,
                data_payload:CostOfClaims_cur::NUMBER AS costofclaims_cur,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                CAST(data_payload:LegacyGrpCostOfClaims_amt AS NUMBER(18,2)) AS legacygrpcostofclaims_amt,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:CostOfClaims_amt AS NUMBER(18,2)) AS costofclaims_amt,
                data_payload:AdjustmentType::NUMBER AS adjustmenttype,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:ChangeType::NUMBER AS changetype,
                data_payload:BasedOnID::NUMBER AS basedonid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:BranchID::NUMBER AS branchid,
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
            FROM {{ source('gwpc', 'pcx_lpradjustment_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:grpcostofclaims_cur::NUMBER AS grpcostofclaims_cur,
                $1:effectivedatedfieldsid::NUMBER AS effectivedatedfieldsid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:grpcostofclaims_amt AS NUMBER(18,2)) AS grpcostofclaims_amt,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:fixedid::NUMBER AS fixedid,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:legacygrpcostofclaims_cur::NUMBER AS legacygrpcostofclaims_cur,
                $1:id::NUMBER AS id,
                $1:costofclaims_cur::NUMBER AS costofclaims_cur,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                CAST($1:legacygrpcostofclaims_amt AS NUMBER(18,2)) AS legacygrpcostofclaims_amt,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:costofclaims_amt AS NUMBER(18,2)) AS costofclaims_amt,
                $1:adjustmenttype::NUMBER AS adjustmenttype,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:changetype::NUMBER AS changetype,
                $1:basedonid::NUMBER AS basedonid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:subtype::NUMBER AS subtype,
                $1:branchid::NUMBER AS branchid,
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
            FROM {{ source('gwpc', 'pcx_lpradjustment_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS lpradjustment_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'grpcostofclaims_cur',
                        'effectivedatedfieldsid',
                        'publicid',
                        'grpcostofclaims_amt',
                        'createtime',
                        'fixedid',
                        'effectivedate',
                        'updatetime',
                        'legacygrpcostofclaims_cur',
                        'costofclaims_cur',
                        'expirationdate',
                        'legacygrpcostofclaims_amt',
                        'createuserid',
                        'costofclaims_amt',
                        'adjustmenttype',
                        'beanversion',
                        'archivepartition',
                        'changetype',
                        'basedonid',
                        'updateuserid',
                        'subtype',
                        'branchid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}