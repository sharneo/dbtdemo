{% snapshot snap_bpc_tradpartner %}

{{ config(
    target_schema='sap',
    unique_key='bpc_tradpartner_sk',
    alias ='bpc_tradpartner',
    strategy='check',
    check_cols=['b631_s_tdp', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_tradpartner_sk,
        *
    FROM {{ source('sap', 'bpc_tradpartner') }}
    WHERE RECORD_TYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}