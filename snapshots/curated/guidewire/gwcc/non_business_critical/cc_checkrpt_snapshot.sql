{% snapshot cc_checkrpt_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_checkrpt.
                                                Source: ref('stg_raw_cc_checkrpt')
                                                unique_key: checkrpt_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='checkrpt_sk',
    strategy='check',
    alias='cc_checkrpt',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'cc_checkrpt']
) }}

SELECT
    checkrpt_sk,
    hash_key,
    createuserid,
    checkid,
    publicid,
    beanversion,
    archivepartition,
    retired,
    createtime,
    reservingcurrency,
    grossreservingamount,
    updateuserid,
    grossclaimamount,
    currency,
    updatetime,
    id,
    grossamount,
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
FROM {{ ref('stg_raw_cc_checkrpt') }}

{% endsnapshot %}