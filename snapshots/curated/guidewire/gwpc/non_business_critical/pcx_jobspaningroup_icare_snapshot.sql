{% snapshot pcx_jobspaningroup_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pcx_jobspaningroup_icare.
                                                Source: ref('stg_raw_pcx_jobspaningroup_icare')
                                                unique_key: jobspaningroup_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='jobspaningroup_icare_sk',
    strategy='check',
    alias='pcx_jobspaningroup_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pcx_jobspaningroup_icare']
) }}

SELECT
    jobspaningroup_icare_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    createuserid,
    publicid,
    beanversion,
    accountid,
    archivepartition,
    retired,
    createtime,
    exitdate,
    updateuserid,
    comments,
    updatetime,
    joindate,
    jobid,
    id,
    exitreasontype,
    exempted,
    regletterversiontriggered,
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
FROM {{ ref('stg_raw_pcx_jobspaningroup_icare') }}

{% endsnapshot %}