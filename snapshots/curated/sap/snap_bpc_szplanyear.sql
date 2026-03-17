{% snapshot snap_bpc_szplanyear %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for BPC_SZPLANYEAR
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='bpc_szplanyear_sk',
    alias ='bpc_szplanyear',
    strategy='check',
    check_cols=['bic_zplanyear', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','snp_bpc_szplanyear']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_szplanyear_sk,
        *
    FROM {{ source('sap', 'bpc_szplanyear') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}