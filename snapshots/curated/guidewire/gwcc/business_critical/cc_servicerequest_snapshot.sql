{% snapshot cc_servicerequest_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_servicerequest.
                                                Source: ref('stg_raw_cc_servicerequest')
                                                unique_key: servicerequest_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='servicerequest_sk',
    strategy='check',
    alias='cc_servicerequest',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_servicerequest']
) }}

SELECT
    servicerequest_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    hostabn_icare,
    requestedby_icare,
    previousgroupid,
    servicerequestnumber,
    publicid,
    expectedservicecompletiondate,
    createtime,
    documentlinkableid,
    assignedbyuserid,
    assignedgroupid,
    specialistid,
    siraaccredrehabprovider_icare,
    expectedquotecompletiondate,
    currency,
    requestedservicecompletiondate,
    kind,
    previousqueueid,
    updatetime,
    claimid,
    requestedquotecompletiondate,
    servicerequestreferencenumber,
    id,
    previoususerid,
    canceledreason,
    assignedqueueid,
    specialistcommmethod,
    createuserid,
    tier,
    exposureid,
    closedate,
    beanversion,
    archivepartition,
    incidentid,
    progress,
    quotestatus,
    linkedtocommonlawcase_icare,
    updateuserid,
    nulldate_icare,
    linktoimp_icare,
    assigneduserid,
    description_icare,
    assignmentdate,
    daterequested_icare,
    latestchangetimestampdenorm,
    assignmentstatus,
    otheroutcome_icare,
    requestoutcome_ext,
    totalapprovedamount_ext,
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
FROM {{ ref('stg_raw_cc_servicerequest') }}

{% endsnapshot %}