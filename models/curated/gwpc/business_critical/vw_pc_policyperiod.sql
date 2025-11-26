{#-

Project: Data Uplift Progran 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2025-11-18      0.0                             This Build the pc_policy in the curated layer

-#}   

{{ config(
    materialized='view'
) }}

{# --- Select all columns except the snapshot fields and hash columns --- #}
SELECT
    {{ dbt_utils.star(
        from=ref('pc_policyperiod')
    ) }}
FROM {{ ref('pc_policyperiod') }} AS gwpc

