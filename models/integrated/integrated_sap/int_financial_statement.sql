{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Incremental model for Financial Statement reference data.
-#}

{{
  config(
    materialized='incremental',
    unique_key='fsv_sk',
    on_schema_change='append_new_columns',
    incremental_strategy='merge',
    tags=['sap','business_critical']

  )
}}

with cte_source as 
(
SELECT 
    CAST(fsv_sk as varchar(64)) as fsv_sk,
    CAST(versn as varchar(5)) as financial_statement_version,
    CAST(vstxt as varchar(50)) as financial_statement_version_name,
    CAST(txt45 as varchar(45)) as financial_statement_item_text,
    CAST(ktopl as varchar(4)) as chart_of_accounts,
    CAST(vonkt as varchar(10)) as account_interval_lower_limit,
    CAST(biskt as varchar(10)) as biskt,
    CAST(id as varchar(6)) as id,
    CAST(type as varchar(1)) as node_type,
    CAST(parent as varchar(6)) as parent,
    CAST(child as varchar(6)) as child,
    CAST(nextn as varchar(6)) as nextn,
    CAST(stufe as varchar(2)) as financial_statement_item_hierarchy_level,
    CAST(summe as varchar(1)) as summe,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to,to_date('9999-12-31')) as valid_to,
    metadata_file_name,
    file_ingestion_timestamp
FROM    
    {{ ref('fsv_snapshot') }}
WHERE DBT_VALID_TO IS NULL 
    {% if is_incremental() %}
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}

)

SELECT 
    fsv_sk,
    financial_statement_version,
    financial_statement_version_name,
    financial_statement_item_text,
    chart_of_accounts,
    account_interval_lower_limit,
    biskt,
    id,
    node_type,
    parent,
    child,
    nextn,
    financial_statement_item_hierarchy_level,
    summe
    valid_from,
    valid_to,
    metadata_file_name,
    file_ingestion_timestamp
FROM 
    cte_source