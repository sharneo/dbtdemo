{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pc_accountcontactrole.
                                                accountcontactrole_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "business_critical", "pc_accountcontactrole"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:RelationshipTitle::NUMBER AS relationshiptitle,
                data_payload:YearLicensed::NUMBER AS yearlicensed,
                TO_TIMESTAMP_TZ(data_payload:DateCompletedTrainingClass::NUMBER/1000) AS datecompletedtrainingclass,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:NumberofAccidents::NUMBER AS numberofaccidents,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:IndustryCodeID::NUMBER AS industrycodeid,
                data_payload:GoodDriverDiscount::BOOLEAN AS gooddriverdiscount,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:BusOpsDescription::TEXT AS VARCHAR(60)) AS busopsdescription,
                data_payload:NumberofViolations::NUMBER AS numberofviolations,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:AccountContact::NUMBER AS accountcontact,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:TrainingClassType::NUMBER AS trainingclasstype,
                data_payload:Referenced::BOOLEAN AS referenced,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:Student::BOOLEAN AS student,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:OrgType::TEXT AS VARCHAR(60)) AS orgtype,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
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
            FROM {{ source('gwpc', 'pc_accountcontactrole') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:relationshiptitle::NUMBER AS relationshiptitle,
                $1:yearlicensed::NUMBER AS yearlicensed,
                $1:datecompletedtrainingclass::TIMESTAMP_TZ AS datecompletedtrainingclass,
                $1:createuserid::NUMBER AS createuserid,
                $1:numberofaccidents::NUMBER AS numberofaccidents,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:industrycodeid::NUMBER AS industrycodeid,
                $1:gooddriverdiscount::BOOLEAN AS gooddriverdiscount,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:busopsdescription::TEXT AS VARCHAR(60)) AS busopsdescription,
                $1:numberofviolations::NUMBER AS numberofviolations,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:accountcontact::NUMBER AS accountcontact,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:trainingclasstype::NUMBER AS trainingclasstype,
                $1:referenced::BOOLEAN AS referenced,
                $1:subtype::NUMBER AS subtype,
                $1:student::BOOLEAN AS student,
                $1:id::NUMBER AS id,
                CAST($1:orgtype::TEXT AS VARCHAR(60)) AS orgtype,
                $1:archivepartition::NUMBER AS archivepartition,
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
            FROM {{ source('gwpc', 'pc_accountcontactrole') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS accountcontactrole_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'relationshiptitle',
                        'yearlicensed',
                        'datecompletedtrainingclass',
                        'createuserid',
                        'numberofaccidents',
                        'publicid',
                        'industrycodeid',
                        'gooddriverdiscount',
                        'beanversion',
                        'retired',
                        'createtime',
                        'busopsdescription',
                        'numberofviolations',
                        'updateuserid',
                        'accountcontact',
                        'updatetime',
                        'trainingclasstype',
                        'referenced',
                        'subtype',
                        'student',
                        'orgtype',
                        'archivepartition'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}