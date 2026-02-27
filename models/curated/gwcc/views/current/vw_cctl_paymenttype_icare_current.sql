
{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.02.26      2.0                             Creates Current View for Curated Layer  
                                            cctl_paymenttype_icare - Latest record per ID  
                                            Deterministic NULL handling
                                            Supports PARQUET + AVRO logic
-#}

{{ config(
    materialized='view',
    tags=["daily", "curated", "curated_view", "hourly", "claim"]
) }}

{% set relation = ref('cctl_paymenttype_icare') %}
{% set columns = adapter.get_columns_in_relation(relation) %}
{% set column_names = columns | map(attribute='name') | map('upper') | list %}
{% set has_updatetime = 'UPDATETIME' in column_names %}

WITH source_data AS (

    SELECT
        {{ dbt_utils.star(from=relation) }},
        {% if has_updatetime %}
            COALESCE(UPDATETIME, FILE_INGESTION_TIMESTAMP)
        {% else %}
            FILE_INGESTION_TIMESTAMP
        {% endif %} AS effective_ts
    FROM {{ relation }}
    WHERE FILE_TYPE IN ('PARQUET', 'AVRO')
),

-- Deduplicate PARQUET records using sequence value
-- GWCBI_OPERATION 1 is Do not Show Deleted Rows in GW
parquet_deduped AS (

    SELECT * FROM source_data
    WHERE FILE_TYPE = 'PARQUET'
    AND GWCBI_OPERATION NOT IN ('1')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ID
        ORDER BY GWCBI_SEQVAL DESC NULLS LAST
    ) = 1

),

-- Deduplicate AVRO records using effective timestamp
avro_deduped AS (

    SELECT * FROM source_data
    WHERE FILE_TYPE = 'AVRO'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ID
        ORDER BY effective_ts DESC NULLS LAST
    ) = 1

),

-- Combine both file types
combined AS (

    SELECT * FROM parquet_deduped
    UNION ALL
    SELECT * FROM avro_deduped

),

-- Final dedup across file types
latest_records AS (

    SELECT * FROM combined
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ID
        ORDER BY effective_ts DESC NULLS LAST
    ) = 1

)

SELECT * FROM latest_records
