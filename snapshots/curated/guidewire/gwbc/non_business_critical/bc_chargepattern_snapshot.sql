{% snapshot bc_chargepattern_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_chargepattern.
                                                Source: ref('stg_raw_bc_chargepattern')
                                                unique_key: chargepattern_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='chargepattern_sk',
    strategy='check',
    alias='bc_chargepattern',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_chargepattern']
) }}

SELECT
    chargepattern_sk,
    hash_key,
    chargecode,
    createuserid,
    inuse,
    publicid,
    priority,
    beanversion,
    createtime,
    retired,
    invoicetreatment,
    updateuserid,
    internalcharge,
    includedinequitydating,
    taccountownerpatternid,
    periodicity,
    taccountslazyloaded,
    reversible,
    updatetime,
    subtype,
    id,
    category,
    chargename,
    includedindelinquencyplan_ext,
    refertornsw_ext,
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
FROM {{ ref('stg_raw_bc_chargepattern') }}

{% endsnapshot %}