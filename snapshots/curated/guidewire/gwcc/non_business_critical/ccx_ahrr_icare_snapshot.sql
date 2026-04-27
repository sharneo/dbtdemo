{% snapshot ccx_ahrr_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_ahrr_icare.
                                                Source: ref('stg_raw_ccx_ahrr_icare')
                                                unique_key: ahrr_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='ahrr_icare_sk',
    strategy='check',
    alias='ccx_ahrr_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'ccx_ahrr_icare']
) }}

SELECT
    ahrr_icare_sk,
    hash_key,
    loadcommandid,
    ahrrenddate,
    publicid,
    clmpracticesuburb,
    claimnumber,
    createtime,
    ahrrstartdate,
    clmreferredby,
    clmserviceprovidername,
    clmpracticestate,
    clmreferrerphonenumber,
    clmworkerphonenumber,
    updatetime,
    clmchiropractor,
    claimid,
    clmpracticepostalcode,
    clmtreatmentcommenced,
    id,
    clmpracticeemail,
    clmnosessionprovidedtodate,
    clmcounsellor,
    clmphysiotherapist,
    clmdateofinjury,
    createuserid,
    beanversion,
    archivepartition,
    retired,
    dateofrequest,
    clmpracticename,
    clmother,
    clmworkername,
    updateuserid,
    ahrrnumber,
    documentindentifier,
    clmsiranumber,
    clmaccexercisephysiologist,
    clmdateofbirth,
    clmpracticephonenumber,
    clmosteopath,
    clmpsychologist,
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
FROM {{ ref('stg_raw_ccx_ahrr_icare') }}

{% endsnapshot %}