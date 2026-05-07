{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_exposurerpt.
                                                exposurerpt_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "cc_exposurerpt"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:AvailableReserves AS NUMBER(18,2)) AS availablereserves,
                CAST(data_payload:OpenReserves AS NUMBER(18,2)) AS openreserves,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:OpenRecoveryReserves AS NUMBER(18,2)) AS openrecoveryreserves,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                CAST(data_payload:OpenRecoveryResRprtng AS NUMBER(18,2)) AS openrecoveryresrprtng,
                data_payload:ID::NUMBER AS id,
                data_payload:ExposureID::NUMBER AS exposureid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:RemainingResrvRprtng AS NUMBER(18,2)) AS remainingresrvrprtng,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:FuturePaymentsRprtng AS NUMBER(18,2)) AS futurepaymentsrprtng,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:TotalPaymentsRprtng AS NUMBER(18,2)) AS totalpaymentsrprtng,
                CAST(data_payload:RemainingReserves AS NUMBER(18,2)) AS remainingreserves,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:TotalRecoveriesRprtng AS NUMBER(18,2)) AS totalrecoveriesrprtng,
                CAST(data_payload:FuturePayments AS NUMBER(18,2)) AS futurepayments,
                CAST(data_payload:TotalPayments AS NUMBER(18,2)) AS totalpayments,
                CAST(data_payload:AvailableResrvRprtng AS NUMBER(18,2)) AS availableresrvrprtng,
                CAST(data_payload:TotalRecoveries AS NUMBER(18,2)) AS totalrecoveries,
                CAST(data_payload:OpenReservesRprtng AS NUMBER(18,2)) AS openreservesrprtng,
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
            FROM {{ source('gwcc', 'cc_exposurerpt') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:availablereserves AS NUMBER(18,2)) AS availablereserves,
                CAST($1:openreserves AS NUMBER(18,2)) AS openreserves,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:openrecoveryreserves AS NUMBER(18,2)) AS openrecoveryreserves,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                CAST($1:openrecoveryresrprtng AS NUMBER(18,2)) AS openrecoveryresrprtng,
                $1:id::NUMBER AS id,
                $1:exposureid::NUMBER AS exposureid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:remainingresrvrprtng AS NUMBER(18,2)) AS remainingresrvrprtng,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:futurepaymentsrprtng AS NUMBER(18,2)) AS futurepaymentsrprtng,
                $1:retired::NUMBER AS retired,
                CAST($1:totalpaymentsrprtng AS NUMBER(18,2)) AS totalpaymentsrprtng,
                CAST($1:remainingreserves AS NUMBER(18,2)) AS remainingreserves,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:totalrecoveriesrprtng AS NUMBER(18,2)) AS totalrecoveriesrprtng,
                CAST($1:futurepayments AS NUMBER(18,2)) AS futurepayments,
                CAST($1:totalpayments AS NUMBER(18,2)) AS totalpayments,
                CAST($1:availableresrvrprtng AS NUMBER(18,2)) AS availableresrvrprtng,
                CAST($1:totalrecoveries AS NUMBER(18,2)) AS totalrecoveries,
                CAST($1:openreservesrprtng AS NUMBER(18,2)) AS openreservesrprtng,
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
            FROM {{ source('gwcc', 'cc_exposurerpt') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS exposurerpt_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'availablereserves',
                        'openreserves',
                        'publicid',
                        'createtime',
                        'openrecoveryreserves',
                        'updatetime',
                        'claimid',
                        'openrecoveryresrprtng',
                        'exposureid',
                        'createuserid',
                        'remainingresrvrprtng',
                        'archivepartition',
                        'beanversion',
                        'futurepaymentsrprtng',
                        'retired',
                        'totalpaymentsrprtng',
                        'remainingreserves',
                        'updateuserid',
                        'totalrecoveriesrprtng',
                        'futurepayments',
                        'totalpayments',
                        'availableresrvrprtng',
                        'totalrecoveries',
                        'openreservesrprtng'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}