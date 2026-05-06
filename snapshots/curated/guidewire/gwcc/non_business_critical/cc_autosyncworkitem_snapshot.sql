{% snapshot cc_autosyncworkitem_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_autosyncworkitem.
                                                Source: ref('stg_raw_cc_autosyncworkitem')
                                                unique_key: autosyncworkitem_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='autosyncworkitem_sk',
    strategy='check',
    alias='cc_autosyncworkitem',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'cc_autosyncworkitem']
) }}

SELECT
    autosyncworkitem_sk,
    hash_key,
    processhistoryid,
    publicid,
    priority,
    mincontactref,
    attempts,
    lastupdatetime,
    creationtime,
    exception,
    availablesince,
    status,
    addressbookuid,
    newaddressbookuid,
    maxcontactref,
    id,
    checkedoutby,
    skip,
    'GWCC' AS source_system,
    gwcbi_connector_ts_ms,
    gwcbi_lsn,
    gwcbi_operation,
    gwcbi_payload_ts_ms,
    gwcbi_seqval,
    gwcbi_seqval_hex,
    gwcbi_tx_id,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('stg_raw_cc_autosyncworkitem') }}

{% endsnapshot %}