{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      1.7                             Curated SCD2 full history view for cctl_cts_benefittype.
                                                Source: {{ ref('cctl_cts_benefittype_snapshot') }}
                                                Contains full SCD2 history including valid_from, valid_to.
                                                Current record: valid_to open-ended .
                                                CDA Deletion Flag is 1 
                                                For current-state-only view, use the current state view.
-#}

{{ config(
    materialized='view',
    tags=["curated_history"]
) }}

WITH cte_source_data AS (
    SELECT
        {{ dbt_utils.star(from=ref('cctl_cts_benefittype_snapshot')) }}
    FROM {{ ref('cctl_cts_benefittype_snapshot') }}
),

cte_transformed AS (
    SELECT
        *,
        CASE 
            WHEN valid_to = {{ snapshot_valid_to_current() }}
            THEN 'Y'
            ELSE 'N' 
        END AS current_record,
        CASE 
            WHEN COALESCE(gwcbi_operation, 0) = 1 THEN 'Y' 
            WHEN LOWER(COALESCE(is_deleted, 'false')) = 'true' THEN 'Y'
            ELSE 'N' 
        END AS deletion_flag
    FROM cte_source_data
)

SELECT * FROM cte_transformed