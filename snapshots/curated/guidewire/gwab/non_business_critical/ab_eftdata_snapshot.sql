{% snapshot ab_eftdata_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ab_eftdata.
                                                Source: ref('stg_raw_ab_eftdata')
                                                unique_key: eftdata_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwab',
    unique_key='eftdata_sk',
    strategy='check',
    alias='ab_eftdata',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'contact_manager', 'non_business_critical', 'ab_eftdata']
) }}

SELECT
    eftdata_sk,
    hash_key,
    loadcommandid,
    createuserid,
    publicid,
    bankbranchname_icare,
    bankname,
    beanversion,
    archivepartition,
    createtime,
    retired,
    bankaccounttype,
    updateuserid,
    banktype_icare,
    approved_icare,
    bankroutingnumber,
    linkid,
    updatetime,
    accountname,
    isprimary,
    bankaccountnumber,
    id,
    contactid,
    bankswiftcode_icare,
    eftsource_ext,
    isedited_ext,
    timestamp_ext,
    isonboarded_ext,
    providerrefid_ext,
    'GWAB' AS source_system,
    gwcbi_connector_ts_ms,
    gwcbi_lsn,
    gwcbi_operation,
    gwcbi_payload_ts_ms,
    gwcbi_seqval,
    gwcbi_seqval_hex,
    gwcbi_tx_id,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('stg_raw_ab_eftdata') }}

{% endsnapshot %}