{% snapshot ccx_industrycode_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_industrycode_icare.
                                                Source: ref('stg_raw_ccx_industrycode_icare')
                                                unique_key: industrycode_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='industrycode_icare_sk',
    strategy='check',
    alias='ccx_industrycode_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_industrycode_icare']
) }}

SELECT
    industrycode_icare_sk,
    hash_key,
    loadcommandid,
    createuserid,
    publicid,
    beanversion,
    createtime,
    retired,
    industrycode,
    groupcode,
    updateuserid,
    industrydesc,
    groupdesc,
    effectivedate,
    updatetime,
    version,
    divisioncode,
    subdivisioncode,
    id,
    expirationdate,
    divisiondesc,
    subdivisiondesc,
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
FROM {{ ref('stg_raw_ccx_industrycode_icare') }}

{% endsnapshot %}