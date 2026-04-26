{% snapshot pc_job_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_job.
                                                Source: ref('stg_raw_pc_job')
                                                unique_key: job_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='job_sk',
    strategy='check',
    alias='pc_job',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'business_critical', 'pc_job']
) }}

SELECT
    job_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    nottakennotifdate,
    archivestate,
    archiveschemainfo,
    quoteddate_icare,
    updatetime,
    notificationdate,
    id,
    source,
    excludereason,
    nextpurgecheckdate,
    createuserid,
    archivefailureid,
    rejectreason,
    closedate,
    beanversion,
    retired,
    cancelreasoncode,
    changepolicynumber,
    updateuserid,
    primaryinsurednamedenorm,
    nonrenewalnotifdate,
    primaryinsuredname,
    quotetype,
    datequoteneeded,
    publicid,
    sidebyside,
    jobnumber,
    rewritetype,
    createtime,
    auditinformationid,
    policyid,
    excludedfromarchive,
    rejectreasontext,
    archivefailuredetailsid,
    rescindnotificationdate,
    purgestatus,
    initialnotificationdate,
    lastnotifiedcancellationdate,
    jobgroup,
    cancelprocessdate,
    renewalcode,
    escalateafterholdreleased,
    renewalnotifdate,
    reinstatecode,
    paymentreceived,
    archivepartition,
    paymentreceived_cur,
    notificationackdate,
    archivedate,
    bindoption,
    nonrenewalcode,
    subtype,
    submissiondate,
    description,
    contributedtonigroup_icare,
    isabnchanged_icare,
    prioranonymous_icare,
    policychangereason_icare,
    schemeagentchange_icare,
    forecast_icare,
    producerchangesource_ext,
    changetype_icare,
    forecastcalculation_icare,
    reasoncodeforcancel_icare,
    requestedby_icare,
    cancelonexpiryreason_icare,
    suppressdocuments_icare,
    isgroupstructurechanged_icare,
    gotbussinlast12mons_icare,
    isloadingapplied_icare,
    policyterm,
    jobnumberdenorm,
    iscssportal_ext,
    iscrifreminderemailsent_ext,
    wageaudittype_ext,
    isbulkpolicycancel_ext,
    isselectedfromgroup_ext,
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
FROM {{ ref('stg_raw_pc_job') }}

{% endsnapshot %}