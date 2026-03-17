{% snapshot snap_trading_partner %}

{{ config(
    target_schema='sap',
    unique_key='trading_partner_sk',
    alias ='trading_partner',
    strategy='check',
    check_cols=['name1'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'rcomp'
    ]) }} AS trading_partner_sk,
        *
    FROM {{ source('sap', 'trading_partner') }}
    WHERE RECORD_TYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}