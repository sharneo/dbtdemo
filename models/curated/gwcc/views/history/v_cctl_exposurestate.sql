{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.5                             SCD Type II view over cctl_exposurestate.
                                                Deduplicates by ID + timestamp, computes VALID_FROM,
                                                VALID_TO, and IS_CURRENT via window functions.
                                                Filters: Excludes Guidewire Cloud Program delete Flag
                                                i.e. CDA Deletion is 1 Page 23 of the Manual
-#}   

{{ config(
    materialized='view',
    tags=["curated_history"]
) }}

WITH cte_source_data AS 
(
    SELECT
        {{ dbt_utils.star(from=ref('cctl_exposurestate')) }}
    FROM {{ ref('cctl_exposurestate') }}
    WHERE coalesce(gwcbi_operation, 5) <> 1
),

cte_deduplicated AS 
(
    SELECT
        *,
        coalesce(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS VALID_FROM
    FROM cte_source_data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ID, coalesce(gwcbi_payload_ts_ms, file_ingestion_timestamp)
        ORDER BY coalesce(gwcbi_seqval,0) DESC
    ) = 1
),

cte_scd2 AS 
(
    SELECT
        *,
        lead(valid_from) over (
            partition by id
            order by valid_from
        ) as _next_valid_from
    FROM cte_deduplicated
)

SELECT
    {{ dbt_utils.star(from=ref('cctl_exposurestate')) }},
    valid_from,
    coalesce(DATEADD('millisecond', -1, _next_valid_from), '9999-12-31'::TIMESTAMP_TZ) AS valid_to,
    CASE
         WHEN _next_valid_from IS NULL THEN TRUE
         ELSE FALSE
    END                           AS is_current
FROM cte_scd2