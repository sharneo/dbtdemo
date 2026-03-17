{% snapshot snap_supplier %}

{{ config(
    target_schema='sap',
    unique_key='supplier_sk',
    alias ='supplier',
    strategy='check',
    check_cols=['land1', 'name1', 'name2', 'name3', 'name4', 'ort01', 'pstlz', 'regio', 'sortl', 'stras', 'ktokk', 'telf1', 'telf2', 'telfx', 'vbund', 'stceg'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'lifnr'
    ]) }} AS supplier_sk,
        *
    FROM {{ source('sap', 'supplier') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}