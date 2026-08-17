{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.5                             Curated current-state view for bctl_workitemsetstate.
                                                Based on Dynamic Table only show IS_CURRENT=TRUE
                                                Filters: Excludes Guidewire Cloud Program delete Flag
                                                i.e. CDA Deletion Flag for ROW is 1 Page 23 of the CDA Manual
-#}   

{{ config(
    materialized='view',
    tags=["current_view"]
) }}

WITH cte_source_data AS 
(
    SELECT
        {{ dbt_utils.star(from=ref('dt_bctl_workitemsetstate_current')) }}
    FROM {{ ref('dt_bctl_workitemsetstate_current') }}
    WHERE coalesce(gwcbi_operation, 5) <> 1
      AND IS_CURRENT = TRUE
)

SELECT * FROM cte_source_data