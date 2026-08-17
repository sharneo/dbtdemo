{#-
  Project: Data Uplift Program
  Project Description/Purpose: Data Uplift Program
  Date        Version  Author  Description of Change
  2026-01-01  0.0              Incremental model for Supplier reference data.
-#}

{{
  config(
    materialized='incremental',
    unique_key='supplier_sk',
    on_schema_change='append_new_columns',
    incremental_strategy='merge',
    tags=['sap','business_critical']

  )
}}

with cte_source as (

    select
        cast(supplier_sk as varchar(60)) as supplier_sk,
        cast(lifnr as varchar(10)) as vendor,
        cast(land1 as varchar(3)) as country_key,
        cast(name1 as varchar(35)) as name,
        cast(name2 as varchar(35)) as name_2,
        cast(name3 as varchar(35)) as name_3,
        cast(name4 as varchar(35)) as name_4,
        cast(ort01 as varchar(35)) as city,
        cast(pstlz as varchar(10)) as postal_code,
        cast(regio as varchar(3)) as region,
        cast(sortl as varchar(10)) as search_term,
        cast(stras as varchar(100)) as street,
        cast(ktokk as varchar(4)) as account_group,
        cast(telf1 as varchar(20)) as telephone_1,
        cast(telf2 as varchar(20)) as telephone_2,
        cast(telfx as varchar(100)) as fax_number,
        cast(vbund as varchar(6)) as trading_partner,
        cast(stceg as varchar(20)) as vat_registration_no,
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to, to_date('9999-12-31')) as valid_to,
        metadata_file_name,
        file_ingestion_timestamp
    from {{ ref('supplier_snapshot') }}
    where dbt_valid_to is null

    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}

)

select
    supplier_sk,
    vendor,
    country_key,
    name,
    name_2,
    name_3,
    name_4,
    city,
    postal_code,
    region,
    search_term,
    street,
    account_group,
    telephone_1,
    telephone_2,
    fax_number,
    trading_partner,
    vat_registration_no,
    valid_from,
    valid_to,
    metadata_file_name,
    file_ingestion_timestamp
from cte_source