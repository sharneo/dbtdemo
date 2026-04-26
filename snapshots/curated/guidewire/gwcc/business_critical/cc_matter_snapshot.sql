{% snapshot cc_matter_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_matter.
                                                Source: ref('stg_raw_cc_matter')
                                                unique_key: matter_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='matter_sk',
    strategy='check',
    alias='cc_matter',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_matter']
) }}

SELECT
    matter_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    punitivedamages,
    loadcommandid,
    previousgroupid,
    firstnotice,
    risktype,
    assignedbyuserid,
    name,
    declaratoryjgmt,
    finalsettlecost,
    subrorelated,
    hearingroom,
    responsedue,
    keyissues_icare,
    previousqueueid,
    updatetime,
    courtdistrict,
    id,
    reopenedreason,
    raiseddate_icare,
    createuserid,
    closedate,
    mediationdate,
    primarycause,
    beanversion,
    retired,
    filedate,
    wpi_icare,
    validationlevel,
    resolution,
    arbitrationdate,
    updateuserid,
    othermattersubtypetext_icare,
    commonlawid,
    punitiveamount,
    publicid,
    trialdate,
    courttype,
    suittype,
    responsefiled,
    createtime,
    documentlinkableid,
    assignedgroupid,
    venuerating,
    significantlitigation_icare,
    litigationstrategy_icare,
    eventtype_icare,
    finallegalcost,
    mediationroom,
    motionsummaryjgmt,
    methodserved,
    claimid,
    arbitrationroom,
    room,
    structuredsettle,
    previoususerid,
    assignedqueueid,
    defenseapptdate,
    hearingdate,
    servicedate,
    legalspecialty,
    filingdate,
    archivepartition,
    docketnumber,
    finalsettledate,
    teleconferencedate_icare,
    addamnumamount,
    assigneduserid,
    newlitstranote_icare,
    statemantofclaim_icare,
    subrogationsummaryid,
    addamnumspecified,
    casenumber,
    mattercasenumber,
    assignmentdate,
    senttodefensedate,
    mattertype,
    assignmentstatus,
    arbitration,
    workcapcitydecision_icareid,
    legacyupdatetime_ext,
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
FROM {{ ref('stg_raw_cc_matter') }}

{% endsnapshot %}