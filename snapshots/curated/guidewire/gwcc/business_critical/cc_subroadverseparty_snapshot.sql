{% snapshot cc_subroadverseparty_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_subroadverseparty.
                                                Source: ref('stg_raw_cc_subroadverseparty')
                                                unique_key: subroadverseparty_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='subroadverseparty_sk',
    strategy='check',
    alias='cc_subroadverseparty',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_subroadverseparty']
) }}

SELECT
    subroadverseparty_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    maincontacttype,
    maincontact_icare,
    activityworflowdate_icare,
    publicid,
    claimnumber,
    closingcomment_icare,
    createtime,
    documentlinkableid,
    notesent,
    recoverytype_icare,
    expectedrecovery,
    outcome_icare,
    adversepartyid,
    maincontactsnumber_icare,
    notereceived,
    updatetime,
    subrogationstatus_icare,
    id,
    duedate_icare,
    expectedrecoveryamount_icare,
    recoverycommenceddate_icare,
    closedate_icare,
    createuserid,
    fault,
    waiver,
    schedulerecoverytype,
    classification,
    beanversion,
    archivepartition,
    payeeinstructions_icare,
    retired,
    schedulerecovery,
    updateuserid,
    strategy,
    subrogovernmentinvolved,
    subrogationsummaryid,
    courtawardedinterest_icare,
    nextcollectiondate_icare,
    policynumber,
    servicedate_ext,
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
FROM {{ ref('stg_raw_cc_subroadverseparty') }}

{% endsnapshot %}