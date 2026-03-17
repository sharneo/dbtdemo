{% snapshot snap_customer %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for CUSTOMER
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='customer_sk',
    alias ='customer',
    strategy='check',
    check_cols=['land1', 'name1', 'name2', 'ort01', 'pstlz', 'regio', 'sortl', 'stras', 'telf1', 'telfx', 'ktokd', 'vbund', 'stceg', 'erdat', 'zuawa', 'akont', 'zwels', 'zterm', 'zsabe', 'fdgrv', 'frgrp', 'intad'],
    tags=['sap','snapshot','snp_customer']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'kunnr',
            'bukrs'
    ]) }} AS customer_sk,
        *
    FROM {{ source('sap', 'customer') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}