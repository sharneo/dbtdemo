{% snapshot snap_vendor %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for VENDOR
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='vendor_sk',
    alias ='vendor',
    strategy='check',
    check_cols=['erdat', 'zuawa', 'akont', 'zwels', 'zterm', 'zsabe', 'fdgrv', 'intad'],
    tags=['sap','snapshot','snp_vendor']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'lifnr',
            'bukrs'
    ]) }} AS vendor_sk,
        *
    FROM {{ source('sap', 'vendor') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}