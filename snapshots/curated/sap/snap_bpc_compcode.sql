{% snapshot snap_bpc_compcode %}

{{ config(
    target_schema='sap',
    unique_key='bpc_compcode_sk',
    alias ='bpc_compcode',
    strategy='check',
    check_cols=['b631_s_compcode', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','curated']
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