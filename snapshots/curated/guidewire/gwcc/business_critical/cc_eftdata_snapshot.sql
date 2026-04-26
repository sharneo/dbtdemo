{% snapshot cc_eftdata_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_eftdata.
                                                Source: ref('stg_raw_cc_eftdata')
                                                unique_key: eftdata_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='eftdata_sk',
    strategy='check',
    alias='cc_eftdata',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_eftdata']
) }}

SELECT
    eftdata_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    publicid,
    bankname,
    createtime,
    bankroutingnumber,
    addressbookuid,
    updatetime,
    isprimary,
    id,
    westpacfail_icare,
    externallinkid,
    createuserid,
    bankbranchname_icare,
    archivepartition,
    beanversion,
    retired,
    bankaccounttype,
    updateuserid,
    banktype_icare,
    approved_icare,
    accountname,
    bankaccountnumber,
    contactid,
    bankswiftcode_icare,
    eftsource_ext,
    isedited_ext,
    timestamp_ext,
    providerrefid_ext,
    document_extid,
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
FROM {{ ref('stg_raw_cc_eftdata') }}

{% endsnapshot %}