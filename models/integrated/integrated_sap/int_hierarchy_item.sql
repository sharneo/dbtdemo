{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Incremental model for Hierarchy Text reference data.
-#}

{{
    config(
        materialized='incremental',
        unique_key='hierarchy_item_sk',
        on_schema_change='append_new_columns',
        incremental_strategy = 'merge',
        tags=['sap','business_critical']

    )
}}

WITH cte_source_data AS (
    SELECT
        {{ dbt_utils.star(from=ref('hierarchy_item_snapshot')) }}
    FROM {{ ref('hierarchy_item_snapshot') }}
    WHERE DBT_VALID_TO IS NULL 
    {% if is_incremental() %}
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
)

SELECT
    *
FROM 
    cte_source_data
