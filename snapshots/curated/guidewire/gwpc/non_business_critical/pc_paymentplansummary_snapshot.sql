{% snapshot pc_paymentplansummary_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_paymentplansummary.
                                                Source: ref('stg_raw_pc_paymentplansummary')
                                                unique_key: paymentplansummary_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='paymentplansummary_sk',
    strategy='check',
    alias='pc_paymentplansummary',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pc_paymentplansummary']
) }}

SELECT
    paymentplansummary_sk,
    hash_key,
    notes,
    publicid,
    total,
    total_cur,
    fee,
    fee_cur,
    createtime,
    name,
    reportingpatterncode,
    invoicefrequency,
    billingid,
    updatetime,
    id,
    createuserid,
    downpayment,
    downpayment_cur,
    archivepartition,
    beanversion,
    paymentplantype,
    retired,
    updateuserid,
    totalfees,
    totalfees_cur,
    policyperiod,
    installment,
    installment_cur,
    tax,
    tax_cur,
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
FROM {{ ref('stg_raw_pc_paymentplansummary') }}

{% endsnapshot %}