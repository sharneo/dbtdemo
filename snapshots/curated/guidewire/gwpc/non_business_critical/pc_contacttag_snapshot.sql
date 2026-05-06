{% snapshot pc_contacttag_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_contacttag.
                                                Source: ref('stg_raw_pc_contacttag')
                                                unique_key: contacttag_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='contacttag_sk',
    strategy='check',
    alias='pc_contacttag',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pc_contacttag']
) }}

SELECT
    contacttag_sk,
    hash_key,
    loadcommandid,
    createuserid,
    publicid,
    beanversion,
    retired,
    createtime,
    updateuserid,
    addressbookuid,
    updatetime,
    type,
    contactid,
    id,
    obfuscatedinternal,
    archivepartition,
    'GWPC' AS source_system,
    gwcbi_connector_ts_ms,
    gwcbi_lsn,
    gwcbi_operation,
    gwcbi_payload_ts_ms,
    gwcbi_seqval,
    gwcbi_seqval_hex,
    gwcbi_tx_id,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('stg_raw_pc_contacttag') }}

{% endsnapshot %}