{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_workstatus.
                                                workstatus_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "cc_workstatus"]
) }}


WITH cte_source_data AS 
(

            SELECT
                TO_TIMESTAMP_TZ(data_payload:StatusDate::NUMBER/1000) AS statusdate,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:WageLoss_icare::BOOLEAN AS wageloss_icare,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:NumDaysWorked AS NUMBER(2,1)) AS numdaysworked,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:EmploymentDataID::NUMBER AS employmentdataid,
                data_payload:Status::NUMBER AS status,
                CAST(data_payload:Comments::TEXT AS VARCHAR(2048)) AS comments,
                TO_TIMESTAMP_TZ(data_payload:LastWorkedDate::NUMBER/1000) AS lastworkeddate,
                data_payload:PaidFullForLastWorked::BOOLEAN AS paidfullforlastworked,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                TO_TIMESTAMP_TZ(data_payload:StatusEndDate::NUMBER/1000) AS statusenddate,
                CAST(data_payload:WageAmountPostInjury AS NUMBER(18,2)) AS wageamountpostinjury,
                CAST(data_payload:NumHoursWorked AS NUMBER(5,2)) AS numhoursworked,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:RefId::TEXT AS VARCHAR(255)) AS refid,
                TO_TIMESTAMP_TZ(data_payload:LegacyCreateTime_Ext::NUMBER/1000) AS legacycreatetime_ext,
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
            FROM {{ source('gwcc', 'cc_workstatus') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:statusdate::TIMESTAMP_TZ AS statusdate,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:wageloss_icare::BOOLEAN AS wageloss_icare,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:numdaysworked AS NUMBER(2,1)) AS numdaysworked,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:employmentdataid::NUMBER AS employmentdataid,
                $1:status::NUMBER AS status,
                CAST($1:comments::TEXT AS VARCHAR(2048)) AS comments,
                $1:lastworkeddate::TIMESTAMP_TZ AS lastworkeddate,
                $1:paidfullforlastworked::BOOLEAN AS paidfullforlastworked,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:statusenddate::TIMESTAMP_TZ AS statusenddate,
                CAST($1:wageamountpostinjury AS NUMBER(18,2)) AS wageamountpostinjury,
                CAST($1:numhoursworked AS NUMBER(5,2)) AS numhoursworked,
                $1:id::NUMBER AS id,
                CAST($1:refid::TEXT AS VARCHAR(255)) AS refid,
                $1:legacycreatetime_ext::TIMESTAMP_TZ AS legacycreatetime_ext,
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
            FROM {{ source('gwcc', 'cc_workstatus') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS workstatus_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'statusdate',
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'wageloss_icare',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'numdaysworked',
                        'updateuserid',
                        'employmentdataid',
                        'status',
                        'comments',
                        'lastworkeddate',
                        'paidfullforlastworked',
                        'updatetime',
                        'statusenddate',
                        'wageamountpostinjury',
                        'numhoursworked',
                        'refid',
                        'legacycreatetime_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}