
{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Incremental model for HIERARCHY HEADER reference data.
-#}

{{
  config(
    materialized='incremental',
    unique_key='customer_sk',
    on_schema_change='append_new_columns',
    incremental_strategy='merge',
    tags=['sap','business_critical']

  )
}}

with cte_source as (
SELECT
    cast(customer_sk as varchar(64)) as customer_sk,
    cast(kunnr as varchar(10)) as customer,
    cast(land1 as varchar(3)) as country,
    cast(name1 as varchar(35)) as name,
    cast(name2 as varchar(35)) as name_2,
    cast(ort01 as varchar(35)) as city,
    cast(pstlz as varchar(10)) as postal_code,
    cast(regio as varchar(3)) as region,
    cast(sortl as varchar(10)) as search_term,
    cast(stras as varchar(35)) as street,
    cast(telf1 as varchar(20)) as telephone_1,
    cast(telfx as varchar(100)) as fax_number,
    cast(ktokd as varchar(4)) as account_group,
    cast(vbund as varchar(6)) as trading_partner,
    cast(stceg as varchar(20)) as vat_registration_no,
    cast(bukrs as varchar(4)) as company_code,
    cast(erdat as varchar(10)) as created_on,
    cast(zuawa as varchar(3)) as sort_key,
    cast(akont as varchar(10)) as reconciliation_acct,
    cast(zwels as varchar(10)) as payment_methods,
    cast(zterm as varchar(4)) as terms_of_payment,
    cast(zsabe as varchar(15)) as user_at_customer,
    cast(fdgrv as varchar(10)) as planning_group,
    cast(frgrp as varchar(4)) as release_group,
    cast(intad as varchar(200)) as clrk_internet_add,
    metadata_file_name,
    file_ingestion_timestamp
    FROM {{ ref('customer_snapshot') }}
    WHERE DBT_VALID_TO IS NULL 
    {% if is_incremental() %}
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
)

SELECT 
    customer_sk,
    customer,
    country,
    name,
    name_2,
    city,
    postal_code,
    region,
    search_term,
    street,
    telephone_1,
    fax_number,
    account_group,
    trading_partner,
    vat_registration_no,
    company_code,
    created_on,
    sort_key,
    reconciliation_acct,
    payment_methods,
    terms_of_payment,
    user_at_customer,
    planning_group,
    release_group,
    clrk_internet_add,
    metadata_file_name,
    file_ingestion_timestamp
from
    cte_source

