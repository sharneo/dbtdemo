{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Incremental model for Profit Centre reference data.
-#}

{{
  config(
    materialized='incremental',
    unique_key='cost_center_sk',
    on_schema_change='append_new_columns',
    incremental_strategy='merge',
    tags=['sap','business_critical']

  )
}}

with cte_source as (
    SELECT
        CAST(cost_center_sk as varchar(64)) as cost_center_sk,
        CAST(kokrs as varchar(4)) as controlling_area,
        CAST(kostl as varchar(10)) as cost_center,
        CAST(datbi as DATE) as valid_to,
        CAST(datab as DATE) as valid_from,
        CAST(bkzkp as varchar(1)) as actual_primary_costs,
        CAST(pkzkp as varchar(1)) as plan_primary_costs,
        CAST(bukrs as varchar(4)) as company_code,
        CAST(gsber as varchar(4)) as business_area,
        CAST(kosar as varchar(1)) as cost_center_category,
        CAST(verak as varchar(20)) as person_responsible,
        CAST(verak_user as varchar(12)) as user_responsible,
        CAST(waers as varchar(5)) as currency,
        CAST(prctr as varchar(10)) as profit_center,
        CAST(ersda as DATE) as created_on,
        CAST(usnam as varchar(12)) as created_by,
        CAST(bkzks as varchar(1)) as actual_secondary_costs,
        CAST(bkzer as varchar(1)) as actual_revenues,
        CAST(bkzob as varchar(1)) as commitment_update,
        CAST(pkzks as varchar(1)) as plan_secondary_costs,
        CAST(pkzer as varchar(1)) as plan_revenues,
        CAST(vmeth as varchar(2)) as allocation_methods,
        CAST(abtei as varchar(12)) as department,
        CAST(khinr as varchar(12)) as hierarchy_area,
        CAST(kompl as varchar(1)) as complete,
        CAST(ktext as varchar(40)) as name,
        CAST(ltext as varchar(100)) as description,
        CAST(mctxt as varchar(40)) as cost_ctr_short_text,
        metadata_file_name,
        file_ingestion_timestamp
    FROM {{ ref('cost_center_snapshot') }}
    WHERE DBT_VALID_TO IS NULL 
    {% if is_incremental() %}
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
)

SELECT 
    cost_center_sk,
    controlling_area,
    cost_center,
    valid_to,
    valid_from,
    actual_primary_costs,
    plan_primary_costs,
    company_code,
    business_area,
    cost_center_category,
    person_responsible,
    user_responsible,
    currency,
    profit_center,
    created_on,
    created_by,
    actual_secondary_costs,
    actual_revenues,
    commitment_update,
    plan_secondary_costs,
    plan_revenues,
    allocation_methods,
    department,
    hierarchy_area,
    complete,
    name,
    description,
    cost_ctr_short_text,
    metadata_file_name,
    file_ingestion_timestamp
FROM cte_source 
