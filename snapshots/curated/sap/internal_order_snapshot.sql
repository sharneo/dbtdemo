{% snapshot internal_order_snapshot%}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for INTERNAL_ORDER
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='internal_order_sk',
    alias ='internal_order',
    strategy='check',
    check_cols=['auart', 'autyp', 'refnr', 'ernam', 'erdat', 'aenam', 'aedat', 'ktext', 'ltext', 'bukrs', 'werks', 'gsber', 'kokrs', 'cckey', 'kostv', 'waers', 'astnr', 'estnr', 'phas1', 'idat1', 'objid', 'kvewe', 'kappl', 'abkrs', 'seqnr', 'user0', 'user4', 'objnr', 'prctr', 'pspel', 'scope', 'plint', 'kdpos', 'aufex', 'erfzeit', 'aezeit'],
    tags=['sap','snapshot','internal_order_snapshot']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'aufnr'
    ]) }} AS internal_order_sk,
        *
    FROM {{ source('sap', 'internal_order') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}