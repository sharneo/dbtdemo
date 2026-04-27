{% snapshot bc_basedistitem_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_basedistitem.
                                                Source: ref('stg_raw_bc_basedistitem')
                                                unique_key: basedistitem_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='basedistitem_sk',
    strategy='check',
    alias='bc_basedistitem',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_basedistitem']
) }}

SELECT
    basedistitem_sk,
    hash_key,
    loadcommandid,
    reverseddistid,
    publicid,
    activedistid,
    createtime,
    policyperiodid,
    currency,
    updatetime,
    producercodeid,
    invoiceitemid,
    id,
    createuserid,
    archivepartition,
    beanversion,
    retired,
    applieddate,
    updateuserid,
    reverseddate,
    commissionamounttoapply,
    commissionamounttoapply_cur,
    grossamounttoapply,
    disposition,
    grossamounttoapply_cur,
    subtype,
    paymentcomments,
    executeddate,
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
FROM {{ ref('stg_raw_bc_basedistitem') }}

{% endsnapshot %}