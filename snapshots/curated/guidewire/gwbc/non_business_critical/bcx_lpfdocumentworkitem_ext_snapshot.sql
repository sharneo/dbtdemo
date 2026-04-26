{% snapshot bcx_lpfdocumentworkitem_ext_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bcx_lpfdocumentworkitem_ext.
                                                Source: ref('stg_raw_bcx_lpfdocumentworkitem_ext')
                                                unique_key: lpfdocumentworkitem_ext_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='lpfdocumentworkitem_ext_sk',
    strategy='check',
    alias='bcx_lpfdocumentworkitem_ext',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bcx_lpfdocumentworkitem_ext']
) }}

SELECT
    lpfdocumentworkitem_ext_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    processhistoryid,
    publicid,
    priority,
    attempts,
    lastupdatetime,
    creationtime,
    account,
    exception,
    availablesince,
    status,
    rundate,
    id,
    checkedoutby,
    'GWBC' AS source_system,
    gwcbi_connector_ts_ms,
    gwcbi_lsn,
    gwcbi_operation,
    gwcbi_payload_ts_ms,
    gwcbi_seqval,
    gwcbi_seqval_hex,
    gwcbi_tx_id,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('stg_raw_bcx_lpfdocumentworkitem_ext') }}

{% endsnapshot %}