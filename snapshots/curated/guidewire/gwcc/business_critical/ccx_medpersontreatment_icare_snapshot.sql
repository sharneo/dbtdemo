{% snapshot ccx_medpersontreatment_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_medpersontreatment_icare.
                                                Source: ref('stg_raw_ccx_medpersontreatment_icare')
                                                unique_key: medpersontreatment_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='medpersontreatment_icare_sk',
    strategy='check',
    alias='ccx_medpersontreatment_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_medpersontreatment_icare']
) }}

SELECT
    medpersontreatment_icare_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    autocreatedahrr,
    loadcommandid,
    icd1id,
    publicid,
    createtime,
    enddate,
    documentlinkableid,
    treatmentquantityrequested,
    cost,
    hours,
    startdate,
    totalcost,
    updatetime,
    claimid,
    id,
    frequency,
    impinclude,
    originalrequest,
    odgflag,
    createuserid,
    dateapproved,
    beanversion,
    archivepartition,
    retired,
    units,
    updateuserid,
    failedahrrdocid,
    paycodeid,
    treatmenttype,
    odgmax,
    frequencycount,
    approvalstatus,
    treatmentquantityapproved,
    subtype,
    contactid,
    reviewrequiredreason,
    description,
    requestdate,
    hourlycost,
    category,
    surgerycost,
    surgerygroupid,
    hicsirahospitalnumber,
    dateofsurgery,
    noofnightsinhospital,
    minutesofoperatingtime,
    autocalcsurgerycost,
    isselectedformsp,
    lasteditedby,
    reasonfordecision,
    medproviderid,
    fractureordislocation,
    reasonforcostvariancesurgery,
    reasonforcostvariancehospital,
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
FROM {{ ref('stg_raw_ccx_medpersontreatment_icare') }}

{% endsnapshot %}