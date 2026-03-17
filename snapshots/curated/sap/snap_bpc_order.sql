{% snapshot snap_bpc_order %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for BPC_ORDER
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='bpc_order_sk',
    alias ='bpc_order',
    strategy='check',
    check_cols=['b631_s_coorder', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','snp_bpc_order']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_order_sk,
        *
    FROM {{ source('sap', 'bpc_order') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}