{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Incremental model for Vendor reference data.
-#}

{{
  config(
    materialized='incremental',
    unique_key='vendor_sk',
    on_schema_change='append_new_columns',
    incremental_strategy = 'merge',
    tags=['sap','business_critical']
  )
}}

with cte_source as 
(
SELECT 
    CAST(vendor_sk as varchar(64)) as vendor_sk,
    CAST(lifnr as varchar(10)) as vendor,
    CAST(bukrs as varchar(4)) as company_code,
    CAST(erdat as DATE) as created_on,
    CAST(zuawa as varchar(3)) as sort_key,
    CAST(akont as varchar(10)) as reconciliation_acct,
    CAST(zwels as varchar(10)) as payment_methods,
    CAST(zterm as varchar(4)) as terms_of_payment,
    CAST(zsabe as varchar(20)) as clerk_at_vendor,
    CAST(fdgrv as varchar(20)) as planning_group,
    CAST(intad as varchar(200)) as clrk_internet_add,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to,to_date('9999-12-31')) as valid_to,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('vendor_snapshot') }}
    WHERE DBT_VALID_TO IS NULL 
    {% if is_incremental() %}
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
)
SELECT 
    vendor_sk,
    vendor,
    company_code,
    created_on,
    sort_key,
    reconciliation_acct,
    payment_methods,
    terms_of_payment,
    clerk_at_vendor,
    planning_group,
    clrk_internet_add,
    valid_from,
    valid_to, 
    metadata_file_name,
    file_ingestion_timestamp
FROM cte_source