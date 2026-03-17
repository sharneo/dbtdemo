{% snapshot snap_comp_code %}

{{ config(
    target_schema='sap',
    unique_key='comp_code_sk',
    alias ='comp_code',
    strategy='check',
    check_cols=['butxt', 'ort01', 'land1', 'waers', 'ktopl', 'waabw', 'periv', 'kokfi', 'rcomp', 'stceg', 'xfdis', 'kkber', 'mwskv', 'mwska'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'bukrs'
    ]) }} AS comp_code_sk,
        *
    FROM {{ source('sap', 'comp_code') }}
    WHERE RECORD_TYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}