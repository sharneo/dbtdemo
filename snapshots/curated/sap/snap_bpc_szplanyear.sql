{% snapshot snap_bpc_szplanyear %}

{{ config(
    target_schema='sap',
    unique_key='bpc_szplanyear_sk',
    alias ='bpc_szplanyear',
    strategy='check',
    check_cols=['bic_zplanyear', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_szplanyear_sk,
        *
    FROM {{ source('sap', 'bpc_szplanyear') }}
    WHERE RECORD_TYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}