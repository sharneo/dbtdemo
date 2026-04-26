{% snapshot cc_injurydiagnosis_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_injurydiagnosis.
                                                Source: ref('stg_raw_cc_injurydiagnosis')
                                                unique_key: injurydiagnosis_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='injurydiagnosis_sk',
    strategy='check',
    alias='cc_injurydiagnosis',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_injurydiagnosis']
) }}

SELECT
    injurydiagnosis_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    createuserid,
    publicid,
    injuryincidentid,
    beanversion,
    archivepartition,
    createtime,
    retired,
    dateended,
    updateuserid,
    injuryseverity_icare,
    datestarted,
    comments,
    compensable,
    updatetime,
    icdcode,
    isprimary,
    id,
    contactid,
    payable_icare,
    legacycreatetime_ext,
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
FROM {{ ref('stg_raw_cc_injurydiagnosis') }}

{% endsnapshot %}