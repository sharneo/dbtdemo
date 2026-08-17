{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Incremental model for Profit Centre reference data.
-#}

{{
  config(
    materialized='incremental',
    unique_key='doc_types_sk',
    on_schema_change='append_new_columns',
    incremental_strategy='merge',
    tags=['sap','business_critical']

  )
}}

WITH source_cte as 
(
  SELECT 
      CAST(doc_types_sk as varchar(64)) as doc_types_sk,
      CAST(spras as varchar(40)) as language_key,
      CAST(blart as varchar(40)) as document_type,
      CAST(ltext as varchar(40)) as description,
      dbt_valid_from as valid_from,
      coalesce(dbt_valid_to,to_date('9999-12-31')) as valid_to,
      metadata_file_name,
      file_ingestion_timestamp
      FROM {{ ref('doc_types_snapshot') }}
      WHERE DBT_VALID_TO IS NULL
      {% if is_incremental() %}
          AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
      {% endif %}
)
SELECT 
    doc_types_sk,
    language_key,
    document_type,
    description,
    valid_from,
    valid_to,
    metadata_file_name,
    file_ingestion_timestamp
FROM source_cte 