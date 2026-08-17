
{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Incremental model for HIERARCHY HEADER reference data.
-#}

{{
  config(
    materialized='incremental',
    unique_key='hierarchy_header_sk',
    on_schema_change='append_new_columns',
    incremental_strategy='merge',
    tags=['sap','business_critical']
  )
}}

with cte_source as (
SELECT
    cast(hierarchy_header_sk as varchar(64)) as hierarchy_header_sk,
    cast(setclass as varchar(40)) as setclass,
    cast(subclass as varchar(40)) as subclass,
    cast(setname as varchar(100)) as setname,
    cast(lineid as varchar(10)) as lineid,
    cast(subsetcls as varchar(40)) as subsetcls,
    cast(subsetscls as varchar(10)) as subsetscls,
    cast(subsetname as varchar(100)) as subsetname,
    cast(seqnr as varchar(10)) as sequence_number,
    metadata_file_name,
    file_ingestion_timestamp
    FROM {{ ref('hierarchy_header_snapshot') }}
    WHERE DBT_VALID_TO IS NULL 
    {% if is_incremental() %}
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
)

SELECT 
    hierarchy_header_sk,
    setclass,
    subclass,
    setname,
    lineid,
    subsetcls,
    subsetscls,
    subsetname,
    sequence_number,
    metadata_file_name,
    file_ingestion_timestamp
FROM    
    cte_source 