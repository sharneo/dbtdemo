{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_claimtransferhistory_ext.
                                                claimtransferhistory_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "ccx_claimtransferhistory_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:ReceivingCSP::TEXT AS VARCHAR(40)) AS receivingcsp,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:ManagingClaimFrom::NUMBER/1000) AS managingclaimfrom,
                CAST(data_payload:TransferType::TEXT AS VARCHAR(40)) AS transfertype,
                CAST(data_payload:TransferringCSP::TEXT AS VARCHAR(40)) AS transferringcsp,
                data_payload:UserID::NUMBER AS userid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                TO_TIMESTAMP_TZ(data_payload:EventTimeStamp::NUMBER/1000) AS eventtimestamp,
                CAST(data_payload:RationaleForTransfer::TEXT AS VARCHAR(40)) AS rationalefortransfer,
                CAST(data_payload:TransferStatus::TEXT AS VARCHAR(40)) AS transferstatus,
                CAST(data_payload:DocumentStatus::TEXT AS VARCHAR(40)) AS documentstatus,
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
            FROM {{ source('gwcc', 'ccx_claimtransferhistory_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:id::NUMBER AS id,
                CAST($1:receivingcsp::TEXT AS VARCHAR(40)) AS receivingcsp,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:managingclaimfrom::TIMESTAMP_TZ AS managingclaimfrom,
                CAST($1:transfertype::TEXT AS VARCHAR(40)) AS transfertype,
                CAST($1:transferringcsp::TEXT AS VARCHAR(40)) AS transferringcsp,
                $1:userid::NUMBER AS userid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:eventtimestamp::TIMESTAMP_TZ AS eventtimestamp,
                CAST($1:rationalefortransfer::TEXT AS VARCHAR(40)) AS rationalefortransfer,
                CAST($1:transferstatus::TEXT AS VARCHAR(40)) AS transferstatus,
                CAST($1:documentstatus::TEXT AS VARCHAR(40)) AS documentstatus,
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
            FROM {{ source('gwcc', 'ccx_claimtransferhistory_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS claimtransferhistory_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'receivingcsp',
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'managingclaimfrom',
                        'transfertype',
                        'transferringcsp',
                        'userid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'updateuserid',
                        'updatetime',
                        'claimid',
                        'eventtimestamp',
                        'rationalefortransfer',
                        'transferstatus',
                        'documentstatus'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}