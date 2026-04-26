{% snapshot ccx_s39transition_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_s39transition_icare.
                                                Source: ref('stg_raw_ccx_s39transition_icare')
                                                unique_key: s39transition_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='s39transition_icare_sk',
    strategy='check',
    alias='ccx_s39transition_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_s39transition_icare']
) }}

SELECT
    s39transition_icare_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    exposureid,
    createuserid,
    publicid,
    beanversion,
    archivepartition,
    retired,
    createtime,
    wpipercentage,
    interventionlevel,
    updateuserid,
    outcome,
    status,
    updatetime,
    imedocumentid,
    id,
    dateofmac,
    effectivedate,
    outcomereason,
    noticeperiod,
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
FROM {{ ref('stg_raw_ccx_s39transition_icare') }}

{% endsnapshot %}