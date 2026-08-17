{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_lprplusadjustment_ext.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwpc", "policy_centre", "non_business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:BranchID::NUMBER AS branchid,
                data_payload:BasedOnID::NUMBER AS basedonid,
                data_payload:FixedID::NUMBER AS fixedid,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ChangeType::NUMBER AS changetype,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:AdjustmentType::NUMBER AS adjustmenttype,
                data_payload:EffectiveDatedFieldsID::NUMBER AS effectivedatedfieldsid,
                data_payload:OpenCostOfClaims_cur::NUMBER AS opencostofclaims_cur,
                CAST(data_payload:OpenCostOfClaims_amt AS NUMBER(18,2)) AS opencostofclaims_amt,
                data_payload:ClosedCostOfClaims_cur::NUMBER AS closedcostofclaims_cur,
                CAST(data_payload:ClosedCostOfClaims_amt AS NUMBER(18,2)) AS closedcostofclaims_amt,
                data_payload:TotalCostOfClaims_cur::NUMBER AS totalcostofclaims_cur,
                CAST(data_payload:TotalCostOfClaims_amt AS NUMBER(18,2)) AS totalcostofclaims_amt,
                data_payload:GrpCostOfOpenClaims_cur::NUMBER AS grpcostofopenclaims_cur,
                CAST(data_payload:GrpCostOfOpenClaims_amt AS NUMBER(18,2)) AS grpcostofopenclaims_amt,
                data_payload:GrpCostOfClosedClaims_cur::NUMBER AS grpcostofclosedclaims_cur,
                CAST(data_payload:GrpCostOfClosedClaims_amt AS NUMBER(18,2)) AS grpcostofclosedclaims_amt,
                data_payload:TotalGroupCostClaims_cur::NUMBER AS totalgroupcostclaims_cur,
                CAST(data_payload:TotalGroupCostClaims_amt AS NUMBER(18,2)) AS totalgroupcostclaims_amt,
                data_payload:LegacyGrpCostOfClaims_cur::NUMBER AS legacygrpcostofclaims_cur,
                CAST(data_payload:LegacyGrpCostOfClaims_amt AS NUMBER(18,2)) AS legacygrpcostofclaims_amt,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS VARCHAR(300)) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'AVRO' as file_type,
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_lprplusadjustment_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:beanversion::NUMBER AS beanversion,
                $1:branchid::NUMBER AS branchid,
                $1:basedonid::NUMBER AS basedonid,
                $1:fixedid::NUMBER AS fixedid,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:subtype::NUMBER AS subtype,
                $1:changetype::NUMBER AS changetype,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:adjustmenttype::NUMBER AS adjustmenttype,
                $1:effectivedatedfieldsid::NUMBER AS effectivedatedfieldsid,
                $1:opencostofclaims_cur::NUMBER AS opencostofclaims_cur,
                CAST($1:opencostofclaims_amt AS NUMBER(18,2)) AS opencostofclaims_amt,
                $1:closedcostofclaims_cur::NUMBER AS closedcostofclaims_cur,
                CAST($1:closedcostofclaims_amt AS NUMBER(18,2)) AS closedcostofclaims_amt,
                $1:totalcostofclaims_cur::NUMBER AS totalcostofclaims_cur,
                CAST($1:totalcostofclaims_amt AS NUMBER(18,2)) AS totalcostofclaims_amt,
                $1:grpcostofopenclaims_cur::NUMBER AS grpcostofopenclaims_cur,
                CAST($1:grpcostofopenclaims_amt AS NUMBER(18,2)) AS grpcostofopenclaims_amt,
                $1:grpcostofclosedclaims_cur::NUMBER AS grpcostofclosedclaims_cur,
                CAST($1:grpcostofclosedclaims_amt AS NUMBER(18,2)) AS grpcostofclosedclaims_amt,
                $1:totalgroupcostclaims_cur::NUMBER AS totalgroupcostclaims_cur,
                CAST($1:totalgroupcostclaims_amt AS NUMBER(18,2)) AS totalgroupcostclaims_amt,
                $1:legacygrpcostofclaims_cur::NUMBER AS legacygrpcostofclaims_cur,
                CAST($1:legacygrpcostofclaims_amt AS NUMBER(18,2)) AS legacygrpcostofclaims_amt,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::VARCHAR(300) as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'PARQUET' as file_type,
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_lprplusadjustment_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS lprplusadjustment_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'createtime',
                        'updateuserid',
                        'updatetime',
                        'beanversion',
                        'branchid',
                        'basedonid',
                        'fixedid',
                        'effectivedate',
                        'expirationdate',
                        'subtype',
                        'changetype',
                        'archivepartition',
                        'adjustmenttype',
                        'effectivedatedfieldsid',
                        'opencostofclaims_cur',
                        'opencostofclaims_amt',
                        'closedcostofclaims_cur',
                        'closedcostofclaims_amt',
                        'totalcostofclaims_cur',
                        'totalcostofclaims_amt',
                        'grpcostofopenclaims_cur',
                        'grpcostofopenclaims_amt',
                        'grpcostofclosedclaims_cur',
                        'grpcostofclosedclaims_amt',
                        'totalgroupcostclaims_cur',
                        'totalgroupcostclaims_amt',
                        'legacygrpcostofclaims_cur',
                        'legacygrpcostofclaims_amt'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date
