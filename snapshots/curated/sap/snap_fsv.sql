{% snapshot snap_fsv %}

{{ config(
    target_schema='sap',
    unique_key='fsv_sk',
    alias ='fsv',
    strategy='check',
    check_cols=['vstxt', 'txt45', 'ktopl', 'type', 'parent', 'child', 'nextn', 'stufe', 'summe'],
    tags=['sap','snapshot','curated']
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
    WHERE RECORD_TYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}