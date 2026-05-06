{% snapshot bcx_bizruleoverrides_sp_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bcx_bizruleoverrides_sp.
                                                Source: ref('stg_raw_bcx_bizruleoverrides_sp')
                                                unique_key: bizruleoverrides_sp_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='bizruleoverrides_sp_sk',
    strategy='check',
    alias='bcx_bizruleoverrides_sp',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bcx_bizruleoverrides_sp']
) }}

SELECT
    bizruleoverrides_sp_sk,
    hash_key,
    feetype,
    loadcommandid,
    overridebit,
    jurisdiction,
    inuse,
    publicid,
    minappamount_ext,
    createtime,
    minappamount_ext_cur,
    maximumpremiumamount,
    maximumpremiumamount_cur,
    effectivedate,
    updatetime,
    dayunittype,
    uwcompany,
    id,
    billingmethod,
    expirationdate,
    paymentrevreason,
    minimumpremiumamount,
    offeringtype_ext,
    minimumpremiumamount_cur,
    createuserid,
    leadtimetype,
    overrideamount,
    disbursementreason,
    thresholdtype,
    overrideamount_cur,
    isbackdated_ext,
    paymentmethod,
    beanversion,
    lowbalancemethod,
    retired,
    overrideintvalue,
    paymentplan,
    product,
    updateuserid,
    writeoffreason,
    delinquencyreason,
    subtype,
    shouldcreateactivity,
    producercode,
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
FROM {{ ref('stg_raw_bcx_bizruleoverrides_sp') }}

{% endsnapshot %}