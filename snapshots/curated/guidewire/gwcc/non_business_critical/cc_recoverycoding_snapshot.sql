{% snapshot cc_recoverycoding_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_recoverycoding.
                                                Source: ref('stg_raw_cc_recoverycoding')
                                                unique_key: recoverycoding_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='recoverycoding_sk',
    strategy='check',
    alias='cc_recoverycoding',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'cc_recoverycoding']
) }}

SELECT
    recoverycoding_sk,
    hash_key,
    loadcommandid,
    recoverycategory,
    publicid,
    beanversion,
    archivepartition,
    id,
    reservelineid,
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
FROM {{ ref('stg_raw_cc_recoverycoding') }}

{% endsnapshot %}