{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Curated current-state view for pctl_effectivenessgrade.
                                                Source: {{ ref('pctl_effectivenessgrade_snapshot') }}
                                                Filters: dbt_valid_to IS NULL (current SCD2 record only)
                                                         COALESCE(gwcbi_operation, 0) <> 1 
                                                         This view removes deleted records coming from Guidewire CDA file.
                                                         gwcbi_operation = 1 indicates a delete in Guidewire CDC.
                                                         AVRO records have NULL gwcbi_operation, COALESCE to 0 retains them.
-#}

{{ config(
    materialized='view',
    tags=["curated", "curated_view", "curated_current", "policy_centre", "non_business_critical", "pctl_effectivenessgrade"]
) }}

WITH cte_source_data AS (
    SELECT
        {{ dbt_utils.star(from=ref('pctl_effectivenessgrade_snapshot')) }}
    FROM {{ ref('pctl_effectivenessgrade_snapshot') }}
    WHERE dbt_valid_to IS NULL
      AND COALESCE(gwcbi_operation, 0) <> 1
),

cte_transformed AS (
    SELECT
        *,
        CASE WHEN dbt_valid_to IS NULL THEN 'Y' ELSE 'N' END AS current_record,
        CASE WHEN dbt_is_deleted = TRUE THEN 'Y' ELSE 'N' END AS is_deleted_record
    FROM cte_source_data
)

SELECT * FROM cte_transformed