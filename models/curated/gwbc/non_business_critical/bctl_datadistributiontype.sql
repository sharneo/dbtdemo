{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a DBT Incremental Model for the Curated Layer
                                                This Model is for the Curated table bctl_datadistributiontype in Schema gwbc in the Curated Layer 
-#}

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='HASH_KEY',
    tags=['daily', 'curated', 'hourly', 'curated_bc_incremental']
) }}

SELECT
    {{ dbt_utils.star(from=ref('vw_raw_bctl_datadistributiontype')) }}
FROM {{ ref('vw_raw_bctl_datadistributiontype') }} AS src

{% if is_incremental() %}
WHERE src.file_ingestion_timestamp >= (
    SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01')
    FROM {{ this }}
)
{% endif %}