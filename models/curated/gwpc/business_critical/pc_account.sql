{#-

Project: Data Uplift Progran 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2025-11-18      0.0                             This Build the pc_policy in the curated layer

-#}   

{{ config(
    materialized='incremental',
    unique_key='id',  
    incremental_strategy='merge'  
) }}

{# --- Select all columns except the snapshot fields and hash columns --- #}
SELECT
    {{ dbt_utils.star(
        from=ref('vw_raw_pc_account')
    ) }}
FROM {{ ref('vw_raw_pc_account') }} AS gwpc

{% if is_incremental() %}
  -- Only load new or updated rows in incremental runs
  WHERE gwpc.file_ingestion_timestamp > (SELECT MAX(file_ingestion_timestamp) FROM {{ this }})
{% endif %}
