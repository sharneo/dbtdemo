{% snapshot snap_hierarchy_header %}

{{ config(
    target_schema='sap',
    unique_key='hierarchy_header_sk',
    alias ='hierarchy_header',
    strategy='check',
    check_cols=['lineid', 'subsetcls', 'subsetscls', 'seqnr'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'setclass',
            'subclass',
            'setname',
            'subsetname'
    ]) }} AS hierarchy_header_sk,
        *
    FROM {{ source('sap', 'hierarchy_header') }}
    WHERE RECORD_TYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}