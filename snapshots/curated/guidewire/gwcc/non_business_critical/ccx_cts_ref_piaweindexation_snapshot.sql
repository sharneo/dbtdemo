{% snapshot ccx_cts_ref_piaweindexation_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_cts_ref_piaweindexation.
                                                Source: ref('stg_raw_ccx_cts_ref_piaweindexation')
                                                unique_key: cts_ref_piaweindexation_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='cts_ref_piaweindexation_sk',
    strategy='check',
    alias='ccx_cts_ref_piaweindexation',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'ccx_cts_ref_piaweindexation']
) }}

SELECT
    cts_ref_piaweindexation_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    value,
    publicid,
    fromdate,
    todate,
    percentagechange,
    subtype,
    name,
    id,
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
FROM {{ ref('stg_raw_ccx_cts_ref_piaweindexation') }}

{% endsnapshot %}