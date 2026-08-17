{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.5                             Dynamic Table for current-state of abtl_nameprefix.
                                                Materializes IS_CURRENT = TRUE rows from the SCD2 view.
                                                Refresh controlled by dbt_project.yml var dt_target_lag.
                                                Transient: No Fail-safe storage (derived, recomputable).
                                                Filters: Excludes Guidewire Cloud Program delete Flag
                                                i.e. CDA Deletion Flag for ROW is 1 Page 23 of the Manual
-#}   

{{ config(
    materialized='dynamic_table',
    target_lag=var('dt_target_lag', '720 minutes'),
    snowflake_warehouse=var('dt_warehouse', 'DEV_DBT_WH'),
    refresh_mode='ADAPTIVE',
    on_configuration_change='apply',
    tags=["curated", "gwab", "contact_manager", "non_business_critical", "dynamic_table"]
) }}

SELECT
    {{ dbt_utils.star(from=ref('v_abtl_nameprefix')) }}
FROM {{ ref('v_abtl_nameprefix') }}
WHERE coalesce(gwcbi_operation, 5) <> 1
AND IS_CURRENT = TRUE