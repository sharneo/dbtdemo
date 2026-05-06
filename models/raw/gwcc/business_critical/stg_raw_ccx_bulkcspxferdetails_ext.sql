{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_bulkcspxferdetails_ext.
                                                bulkcspxferdetails_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "ccx_bulkcspxferdetails_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:ReceivingCSP::TEXT AS VARCHAR(40)) AS receivingcsp,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(40)) AS claimnumber,
                CAST(data_payload:CurrentCSP::TEXT AS VARCHAR(40)) AS currentcsp,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:isNullClaim::BOOLEAN AS isnullclaim,
                data_payload:isClaimRemoved::BOOLEAN AS isclaimremoved,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:TransferReason::TEXT AS VARCHAR(40)) AS transferreason,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                TO_TIMESTAMP_TZ(data_payload:TransferDate::NUMBER/1000) AS transferdate,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(40)) AS policynumber,
                data_payload:ReceivingCaseOwnerID::NUMBER AS receivingcaseownerid,
                data_payload:GroupID::NUMBER AS groupid,
                data_payload:GroupNumber::NUMBER AS groupnumber,
                data_payload:ClaimState::NUMBER AS claimstate,
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
            FROM {{ source('gwcc', 'ccx_bulkcspxferdetails_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:id::NUMBER AS id,
                CAST($1:receivingcsp::TEXT AS VARCHAR(40)) AS receivingcsp,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:claimnumber::TEXT AS VARCHAR(40)) AS claimnumber,
                CAST($1:currentcsp::TEXT AS VARCHAR(40)) AS currentcsp,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:isnullclaim::BOOLEAN AS isnullclaim,
                $1:isclaimremoved::BOOLEAN AS isclaimremoved,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:transferreason::TEXT AS VARCHAR(40)) AS transferreason,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:transferdate::TIMESTAMP_TZ AS transferdate,
                $1:subtype::NUMBER AS subtype,
                CAST($1:policynumber::TEXT AS VARCHAR(40)) AS policynumber,
                $1:receivingcaseownerid::NUMBER AS receivingcaseownerid,
                $1:groupid::NUMBER AS groupid,
                $1:groupnumber::NUMBER AS groupnumber,
                $1:claimstate::NUMBER AS claimstate,
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
            FROM {{ source('gwcc', 'ccx_bulkcspxferdetails_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS bulkcspxferdetails_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'receivingcsp',
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'claimnumber',
                        'currentcsp',
                        'beanversion',
                        'createtime',
                        'retired',
                        'isnullclaim',
                        'isclaimremoved',
                        'updateuserid',
                        'transferreason',
                        'updatetime',
                        'transferdate',
                        'subtype',
                        'policynumber',
                        'receivingcaseownerid',
                        'groupid',
                        'groupnumber',
                        'claimstate'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}