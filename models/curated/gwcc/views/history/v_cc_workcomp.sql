{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Curated SCD2 full history view for cc_workcomp.
                                                Source: {{ ref('cc_workcomp_snapshot') }}
                                                Contains full SCD2 history including dbt_valid_from, dbt_valid_to.
                                                For current-state-only view, use the current state view.
-#}

{{ config(
    materialized='view',
    tags=["curated", "curated_view", "curated_history", "claim_centre", "business_critical", "cc_workcomp"]
) }}

WITH cte_source_data AS (
    SELECT
        {{ dbt_utils.star(from=ref('cc_workcomp_snapshot')) }}
    FROM {{ ref('cc_workcomp_snapshot') }}
),

cte_transformed AS (
    SELECT
        *,
        CASE WHEN dbt_valid_to IS NULL THEN 'Y' ELSE 'N' END AS current_record,
        CASE WHEN dbt_is_deleted = TRUE THEN 'Y' ELSE 'N' END AS is_deleted_record
    FROM cte_source_data
)

SELECT * FROM cte_transformed