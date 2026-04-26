{% snapshot cc_document_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_document.
                                                Source: ref('stg_raw_cc_document')
                                                unique_key: document_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='document_sk',
    strategy='check',
    alias='cc_document',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_document']
) }}

SELECT
    document_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    approver_icare,
    peerreviewnotes_icare,
    documentchannel_icare,
    documentidentifier,
    physicallocation_icare,
    authordenorm,
    namedenorm,
    documentpackid_icare,
    dms,
    name,
    author,
    deliverymethod_icare,
    updatetime,
    templatexml_icare,
    docuid,
    recipient,
    obsolete,
    id,
    matterid,
    workcapacity_icareid,
    createuserid,
    sharedwithworker_icare,
    beanversion,
    retired,
    resendpackid_icare,
    portalusertype_icare,
    updateuserid,
    direction_icare,
    type,
    redacted_icare,
    uploadfilekey_icare,
    documentidentifierdenorm,
    filename_icare,
    publicid,
    processid_icare,
    portalsecurityrealm_ext,
    lineofbusiness_icare,
    createtime,
    interactivestatus_icare,
    dpsxml_psc,
    portalclaimupdate_icare,
    creationtype_icare,
    claimid,
    language,
    approvalrequired_icare,
    sharedwithemployer_icare,
    authorid_icare,
    exposureid,
    datesentreceived_icare,
    section,
    archivepartition,
    inboundpackid_icare,
    mimetype,
    pendingdocuid,
    status,
    primaryrecipient_pscid,
    datemodified,
    inbound,
    securitytype,
    datecreated,
    schemeagent_icare,
    documentsubsection_icare,
    datelastsaved_icare,
    claimcontactid,
    newclaimnumber_icare,
    batchid_ext,
    prioritylevel_ext,
    docgenerationtype_ext,
    description,
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
FROM {{ ref('stg_raw_cc_document') }}

{% endsnapshot %}