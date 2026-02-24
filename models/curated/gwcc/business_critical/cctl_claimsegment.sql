{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a DBT Incremental Model for the Curated Layer
                                                This Model is for the Curated table cctl_claimsegment in Schema gwcc in the Curated Layer 
-#}

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='HASH_KEY',
    tags=['daily', 'curated', 'hourly', 'curated_cc_incremental']
) }}

SELECT
    {{ dbt_utils.star(from=ref('vw_raw_cctl_claimsegment')) }}
FROM {{ ref('vw_raw_cctl_claimsegment') }} AS src

{% if is_incremental() %}
WHERE src.file_ingestion_timestamp >= (
    SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01')
    FROM {{ this }}
)
{% endif %}