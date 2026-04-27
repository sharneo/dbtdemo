{% snapshot ccx_wcdmeritreview_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_wcdmeritreview_icare.
                                                Source: ref('stg_raw_ccx_wcdmeritreview_icare')
                                                unique_key: wcdmeritreview_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='wcdmeritreview_icare_sk',
    strategy='check',
    alias='ccx_wcdmeritreview_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'ccx_wcdmeritreview_icare']
) }}

SELECT
    wcdmeritreview_icare_sk,
    hash_key,
    loadcommandid,
    dateapplicationreceived,
    publicid,
    reviewdetailsid,
    createtime,
    meritreviewer,
    meritreviewissuedate,
    applicationlodgedby,
    solicitorid,
    updatetime,
    newwcdrequired,
    id,
    decision43_1a_yesno,
    decision43_1_other,
    decision43_1b_yesno,
    decision43_1d_i,
    createuserid,
    beanversion,
    decision43_1d_ii,
    archivepartition,
    retired,
    furtherinfosubmitted,
    section54stillactive,
    updateuserid,
    responsedate,
    decision43_1a,
    decision43_1b,
    reviewrequestedwithin30days,
    decision43_1c,
    stayapplicable,
    decision43_1e,
    followupflag,
    decision43_1f,
    casenumber,
    stayexpirydate,
    reviewoutcome,
    sections80stillactive,
    piawe,
    withins80noticeperiod,
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
FROM {{ ref('stg_raw_ccx_wcdmeritreview_icare') }}

{% endsnapshot %}