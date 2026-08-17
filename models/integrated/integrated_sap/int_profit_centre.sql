{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Incremental model for Profit Centre reference data.
-#}

{{
  config(
    materialized='incremental',
    unique_key='profit_center_sk',
    on_schema_change='append_new_columns',
    incremental_strategy='merge',
    tags=['sap','business_critical']

  )
}}

with cte_source as (
    select
        cast(profit_center_sk as varchar(64)) as profit_center_sk,
        cast(prctr as varchar(10)) as profit_center,
        cast(datbi as varchar(10)) as valid_to,
        cast(kokrs as varchar(4)) as controlling_area,
        cast(datab as date) as valid_from,
        cast(ersda as date) as created_on,
        cast(usnam as varchar(12)) as changed_by_the_user,
        cast(verak as varchar(40)) as person_resp_for_pc,
        cast(khinr as varchar(12)) as hierarchy_area,
        cast(ktext as varchar(20)) as name,
        cast(ltext as varchar(40)) as long_text,
        metadata_file_name,
        file_ingestion_timestamp
    from {{ ref('profit_center_snapshot') }}
    {% if is_incremental() %}
    where file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
)

select
    profit_center_sk,
    profit_center,
    valid_to,
    controlling_area,
    valid_from,
    created_on,
    changed_by_the_user,
    person_resp_for_pc,
    hierarchy_area,
    name,
    long_text,
    metadata_file_name,
    file_ingestion_timestamp
from cte_source