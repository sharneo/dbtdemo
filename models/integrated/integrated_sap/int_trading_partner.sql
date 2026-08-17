{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Incremental model for Trading Partner reference data.
-#}

{{
  config(
    materialized='incremental',
    unique_key='trading_partner_sk',
    on_schema_change='append_new_columns',
    incremental_strategy='merge',
    tags=['sap','business_critical']

  )
}}

with cte_source as 
(
    SELECT 
        CAST(trading_partner_sk as varchar(64)) as trading_partner_sk,
        CAST(rcomp as varchar(6)) as company,
        CAST(name1 as varchar(30)) as name_of_the_company,
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to,to_date('9999-12-31')) as valid_to,
        metadata_file_name,
        file_ingestion_timestamp
    FROM {{ ref('trading_partner_snapshot') }}
    WHERE DBT_VALID_TO IS NULL 
    {% if is_incremental() %}
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
)
    SELECT
        trading_partner_sk,
        company,
        name_of_the_company,
        valid_from,
        valid_to,
        metadata_file_name,
        file_ingestion_timestamp
    FROM cte_source        
    
