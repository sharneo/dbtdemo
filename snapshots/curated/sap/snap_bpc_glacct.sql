{% snapshot snap_bpc_glacct %}

{{ config(
    target_schema='sap',
    unique_key='bpc_glacct_sk',
    alias ='bpc_glacct',
    strategy='check',
    check_cols=['b631_s_chrtacct', 'b631_s_gl_acct', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_glacct_sk,
        *
    FROM {{ source('sap', 'bpc_glacct') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}