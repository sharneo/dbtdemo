{% snapshot cc_note_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_note.
                                                Source: ref('stg_raw_cc_note')
                                                unique_key: note_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='note_sk',
    strategy='check',
    alias='cc_note',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_note']
) }}

SELECT
    note_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    publicid,
    confidential,
    createtime,
    casemanagementservicesubject,
    rehabplanservices_icareid,
    servicerequestid,
    updatetime,
    claimid,
    language,
    portalauthorrealm_ext,
    dispute_icareid,
    id,
    matterid,
    workcapacity_icareid,
    createuserid,
    exposureid,
    body,
    authoringdate,
    authorid,
    archivepartition,
    beanversion,
    retired,
    activityid,
    subject,
    updateuserid,
    topic,
    portalauthorusername_ext,
    securitytype,
    claimcontactid,
    mspreferral_extid,
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
FROM {{ ref('stg_raw_cc_note') }}

{% endsnapshot %}