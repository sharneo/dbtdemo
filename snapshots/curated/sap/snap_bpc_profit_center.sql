{% snapshot snap_bpc_profit_center %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for BPC_PROFIT_CENTER
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='bpc_profit_center_sk',
    alias ='bpc_profit_center',
    strategy='check',
    check_cols=['b631_s_co_area', 'b631_s_proftctr', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','snp_bpc_profit_center']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_profit_center_sk,
        *
    FROM {{ source('sap', 'bpc_profit_center') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}