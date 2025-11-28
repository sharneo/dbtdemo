{{ config(
    materialized='incremental',
    unique_key='id_hash',  
    incremental_strategy='merge'
) }}

{# --- Select all columns except the snapshot fields and hash columns --- #}
SELECT
    {{ dbt_utils.star(
        from=ref('vw_raw_cc_activity')
    ) }}
FROM {{ ref('vw_raw_cc_activity') }} AS gwpc

{% if is_incremental() %}
  -- Only load new or updated rows in incremental runs
  WHERE gwpc.file_ingestion_timestamp > (SELECT MAX(file_ingestion_timestamp) FROM {{ this }})
{% endif %}