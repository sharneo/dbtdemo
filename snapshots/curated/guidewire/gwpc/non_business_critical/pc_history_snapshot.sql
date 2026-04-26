{% snapshot pc_history_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_history.
                                                Source: ref('stg_raw_pc_history')
                                                unique_key: history_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='history_sk',
    strategy='check',
    alias='pc_history',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pc_history']
) }}

SELECT
    history_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    originalvalue,
    publicid,
    userid,
    archivepartition,
    beanversion,
    accountid,
    customtype,
    policyid,
    newvalue,
    policyperiod,
    ruleuid,
    policytermid,
    type,
    subtype,
    id,
    description,
    eventtimestamp,
    job,
    contact,
    managingentity,
    bulkpolicyxfer,
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
FROM {{ ref('stg_raw_pc_history') }}

{% endsnapshot %}