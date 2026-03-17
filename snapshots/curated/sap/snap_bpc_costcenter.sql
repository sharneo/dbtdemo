{% snapshot snap_bpc_costcenter %}

{{ config(
    target_schema='sap',
    unique_key='bpc_costcenter_sk',
    alias ='bpc_costcenter',
    strategy='check',
    check_cols=['b631_s_co_area', 'b631_s_costcntr', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_costcenter_sk,
        *
    FROM {{ source('sap', 'bpc_costcenter') }}
    WHERE RECORD_TYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}