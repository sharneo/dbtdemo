{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental Curated model for cctl_liabilityreviewtype_icare.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["curated", "gwcc", "claim_centre", "non_business_critical"]
) }}

WITH cte_source_data AS 
(
    SELECT
        {{ dbt_utils.star(from=ref('stg_cctl_liabilityreviewtype_icare')) }}
    FROM {{ ref('stg_cctl_liabilityreviewtype_icare') }}
    {% if is_incremental() %}
        WHERE FILE_INGESTION_TIMESTAMP > (SELECT COALESCE(MAX(FILE_INGESTION_TIMESTAMP), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
    {% endif %}
)

SELECT * FROM cte_source_data
ORDER BY record_insertion_date