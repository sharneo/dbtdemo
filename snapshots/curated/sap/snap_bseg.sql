{% snapshot snap_bseg %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for BSEG
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='bseg_sk',
    alias ='bseg',
    strategy='check',
    check_cols=['buzid', 'augdt', 'augcp', 'augbl', 'bschl', 'koart', 'umskz', 'umsks', 'shkzg', 'gsber', 'pargb', 'mwskz', 'qsskz', 'dmbtr', 'wrbtr', 'kzbtr', 'pswbt', 'pswsl', 'txbhw', 'txbfw', 'mwsts', 'wmwst', 'hwbas', 'fwbas', 'hwzuz', 'fwzuz', 'shzuz', 'stekz', 'mwart', 'txgrp', 'ktosl', 'qsshb', 'kursr', 'gbetr', 'bdiff', 'bdif2', 'valut', 'zuonr', 'sgtxt', 'zinkz', 'vbund', 'bewar', 'altkt', 'vorgn', 'fdlev', 'fdgrp', 'fdwbt', 'fdtag', 'fkont', 'kokrs', 'kostl', 'projn', 'aufnr', 'vbeln', 'xsauf', 'agzei', 'esrre', 'h_monat', 'h_budat'],
    tags=['sap','snapshot','snp_bseg']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'bukrs',
            'belnr',
            'gjahr',
            'buzei'
    ]) }} AS bseg_sk,
        *
    FROM {{ source('sap', 'bseg') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}