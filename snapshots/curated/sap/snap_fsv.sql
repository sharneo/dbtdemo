{% snapshot snap_fsv %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for FSV
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='fsv_sk',
    alias ='fsv',
    strategy='check',
    check_cols=['vstxt', 'txt45', 'ktopl', 'type', 'parent', 'child', 'nextn', 'stufe', 'summe'],
    tags=['sap','snapshot','snp_fsv']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'versn',
            'vonkt',
            'biskt',
            'id'
    ]) }} AS fsv_sk,
        *
    FROM {{ source('sap', 'fsv') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}