{% snapshot pc_document_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_document.
                                                Source: ref('stg_raw_pc_document')
                                                unique_key: document_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='document_sk',
    strategy='check',
    alias='pc_document',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pc_document']
) }}

SELECT
    document_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    documentidentifierdenorm,
    publicid,
    documentidentifier,
    portalsecurityrealm_ext,
    authordenorm,
    namedenorm,
    accountid,
    createtime,
    dms,
    author,
    name,
    policyid,
    packtype_icare,
    policyperiodid,
    contingency,
    ecmdocstatusid_icare,
    updatetime,
    onbasedoctypeid_icare,
    docuid,
    language,
    jobid,
    obsolete,
    recipient,
    id,
    createuserid,
    section,
    archivepartition,
    beanversion,
    retired,
    mimetype,
    updateuserid,
    pendingdocuid,
    status,
    policyterm_icareid,
    transactionid_icare,
    resend_icare,
    datemodified,
    inbound,
    datecreated,
    securitytype,
    type,
    description,
    addresseetype_icare,
    sourcesystem_icare,
    s3url_icare,
    s3uploaddate_icare,
    onbasestoredate_icare,
    externaldoctypeid_icare,
    templateid_icare,
    onbaseurl_icare,
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
FROM {{ ref('stg_raw_pc_document') }}

{% endsnapshot %}