{% snapshot cc_address_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_address.
                                                Source: ref('stg_raw_cc_address')
                                                unique_key: address_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='address_sk',
    strategy='check',
    alias='cc_address',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_address']
) }}

SELECT
    address_sk,
    hash_key,
    loadcommandid,
    obfuscatedinternal,
    publicid,
    batchgeocode,
    createtime,
    addressline1,
    addressline2,
    county,
    addressline3,
    citykanji,
    spatialpoint,
    addressline2kanji,
    state,
    addressbookuid,
    updatetime,
    country,
    id,
    standardizedaddressid_icare,
    isvalidated_icare,
    externallinkid,
    createuserid,
    isaddressfrompc_icare,
    validuntil,
    dpid_icare,
    archivepartition,
    beanversion,
    citydenorm,
    retired,
    city,
    addresstype,
    addressline1kanji,
    updateuserid,
    cedexbureau,
    geocodestatus,
    cedex,
    postalcodedenorm,
    postalcode,
    subtype,
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
FROM {{ ref('stg_raw_cc_address') }}

{% endsnapshot %}