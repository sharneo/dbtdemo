{% snapshot snap_bpc_compcode %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for BPC_COMPCODE
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='bpc_compcode_sk',
    alias ='bpc_compcode',
    strategy='check',
    check_cols=['b631_s_compcode', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','snp_bpc_compcode']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_compcode_sk,
        *
    FROM {{ source('sap', 'bpc_compcode') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}