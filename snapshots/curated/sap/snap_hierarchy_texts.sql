{% snapshot snap_hierarchy_texts %}

{{ config(
    target_schema='sap',
    unique_key='hierarchy_texts_sk',
    alias ='hierarchy_texts',
    strategy='check',
    check_cols=['langu', 'descript'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'setclass',
            'subclass',
            'setname'
    ]) }} AS hierarchy_texts_sk,
        *
    FROM {{ source('sap', 'hierarchy_texts') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}