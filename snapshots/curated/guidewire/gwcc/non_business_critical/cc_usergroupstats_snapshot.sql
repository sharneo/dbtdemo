{% snapshot cc_usergroupstats_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_usergroupstats.
                                                Source: ref('stg_raw_cc_usergroupstats')
                                                unique_key: usergroupstats_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='usergroupstats_sk',
    strategy='check',
    alias='cc_usergroupstats',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'cc_usergroupstats']
) }}

SELECT
    usergroupstats_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    sublitagingone,
    litagingone,
    publicid,
    expagingone,
    allsubopen,
    subroactiveexposure,
    claimagingtwo,
    duetoday,
    subagingtwo,
    calculatedate,
    completedtoday,
    subroactiveclaim,
    flagged,
    groupid,
    id,
    allactopen,
    claimagingthree,
    overdue,
    subagingfour,
    subagingthree,
    expagingfour,
    userid,
    claimagingone,
    claimagingfour,
    sublitagingtwo,
    litagingtwo,
    subroclosed,
    subagingone,
    sublitagingfour,
    allopen,
    litagingfour,
    allmatteropen,
    expagingtwo,
    newthisweek,
    claimworkload,
    sublitagingthree,
    litagingthree,
    totalworkload,
    subroactiveall,
    exposureworkload,
    subclosedthisweek,
    closedthisweek,
    expagingthree,
    matterclosedthisweek,
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
FROM {{ ref('stg_raw_cc_usergroupstats') }}

{% endsnapshot %}