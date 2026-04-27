{% snapshot bc_taccount_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_taccount.
                                                Source: ref('stg_raw_bc_taccount')
                                                unique_key: taccount_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='taccount_sk',
    strategy='check',
    alias='bc_taccount',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_taccount']
) }}

SELECT
    taccount_sk,
    hash_key,
    loadcommandid,
    createuserid,
    taccountcontainerid,
    publicid,
    beanversion,
    archivepartition,
    createtime,
    creationorder,
    updateuserid,
    currency,
    taccountpatternid,
    updatetime,
    balancedenorm,
    subtype,
    balancedenorm_cur,
    id,
    description,
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
FROM {{ ref('stg_raw_bc_taccount') }}

{% endsnapshot %}