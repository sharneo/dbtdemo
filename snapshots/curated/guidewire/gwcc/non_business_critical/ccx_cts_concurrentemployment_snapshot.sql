{% snapshot ccx_cts_concurrentemployment_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_cts_concurrentemployment.
                                                Source: ref('stg_raw_ccx_cts_concurrentemployment')
                                                unique_key: cts_concurrentemployment_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='cts_concurrentemployment_sk',
    strategy='check',
    alias='ccx_cts_concurrentemployment',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'ccx_cts_concurrentemployment']
) }}

SELECT
    cts_concurrentemployment_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    createuserid,
    publicid,
    weeklywage_icare,
    beanversion,
    companyname,
    archivepartition,
    enddate,
    retired,
    createtime,
    updateuserid,
    startdate,
    piaweid,
    updatetime,
    fulltime_icare,
    numhoursworked,
    id,
    description,
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
FROM {{ ref('stg_raw_ccx_cts_concurrentemployment') }}

{% endsnapshot %}