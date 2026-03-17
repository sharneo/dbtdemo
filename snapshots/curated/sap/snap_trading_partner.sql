{% snapshot snap_trading_partner %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for TRADING_PARTNER
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='trading_partner_sk',
    alias ='trading_partner',
    strategy='check',
    check_cols=['name1'],
    tags=['sap','snapshot','snp_trading_partner']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'rcomp'
    ]) }} AS trading_partner_sk,
        *
    FROM {{ source('sap', 'trading_partner') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}