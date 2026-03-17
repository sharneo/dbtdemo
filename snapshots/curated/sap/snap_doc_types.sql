{% snapshot snap_doc_types %}

{{ config(
    target_schema='sap',
    unique_key='doc_types_sk',
    alias ='doc_types',
    strategy='check',
    check_cols=['spras', 'ltext'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'blart'
    ]) }} AS doc_types_sk,
        *
    FROM {{ source('sap', 'doc_types') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}