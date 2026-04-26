{% snapshot cc_authoritylimit_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_authoritylimit.
                                                Source: ref('stg_raw_cc_authoritylimit')
                                                unique_key: authoritylimit_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='authoritylimit_sk',
    strategy='check',
    alias='cc_authoritylimit',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'cc_authoritylimit']
) }}

SELECT
    authoritylimit_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    coveragetype,
    createuserid,
    publicid,
    profileid,
    costtype,
    beanversion,
    retired,
    createtime,
    policytype,
    updateuserid,
    limitamount,
    limittype,
    updatetime,
    id,
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
FROM {{ ref('stg_raw_cc_authoritylimit') }}

{% endsnapshot %}