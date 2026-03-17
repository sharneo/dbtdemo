{% snapshot snap_internal_order %}

{{ config(
    target_schema='sap',
    unique_key='internal_order_sk',
    alias ='internal_order',
    strategy='check',
    check_cols=['auart', 'autyp', 'refnr', 'ernam', 'erdat', 'aenam', 'aedat', 'ktext', 'ltext', 'bukrs', 'werks', 'gsber', 'kokrs', 'cckey', 'kostv', 'waers', 'astnr', 'estnr', 'phas1', 'idat1', 'objid', 'kvewe', 'kappl', 'abkrs', 'seqnr', 'user0', 'user4', 'objnr', 'prctr', 'pspel', 'scope', 'plint', 'kdpos', 'aufex', 'erfzeit', 'aezeit'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'aufnr'
    ]) }} AS internal_order_sk,
        *
    FROM {{ source('sap', 'internal_order') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}