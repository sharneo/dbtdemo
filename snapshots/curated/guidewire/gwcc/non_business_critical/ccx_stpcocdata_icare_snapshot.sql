{% snapshot ccx_stpcocdata_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_stpcocdata_icare.
                                                Source: ref('stg_raw_ccx_stpcocdata_icare')
                                                unique_key: stpcocdata_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='stpcocdata_icare_sk',
    strategy='check',
    alias='ccx_stpcocdata_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'ccx_stpcocdata_icare']
) }}

SELECT
    stpcocdata_icare_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    drivingability,
    claimnumber,
    postcode,
    employmentdeclaration,
    referraltorehabprovider,
    requestforpositiondesc,
    cwenddate,
    pushingpullingability,
    signatureofpatient,
    updatetime,
    liftingcarryingcapacity,
    factorsdelayingrecovery,
    docuid,
    id,
    cwstartdate,
    addressstate,
    lastname,
    treatmentplan,
    createuserid,
    otherconsiderations,
    beanversion,
    retired,
    addressstreetlocation,
    hoursperday,
    date,
    updateuserid,
    fitforpreinjuryduties,
    nextreviewdate,
    firstname,
    dateofinjury,
    standingtolerance,
    sittingtolerance,
    publicid,
    createtime,
    telephonenumber,
    initialcertificate,
    bendingtwistingsquatting,
    estimateddatetoreturn,
    employmentdetails,
    addresslocality,
    daysperweek,
    dateofbirth,
    capacityforwork,
    preexistingconditions,
    otherreferrals,
    ncwenddate,
    ncwstartdate,
    diagnosis,
    medicarenumber,
    nocapacityforwork,
    preinjfitnessstartdate,
    treatingmedpractsign,
    comment,
    injuryconsistentwithcause,
    additionalinjury,
    treatmentcomments1,
    treatmentcomments2,
    treatmentcomments3,
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
FROM {{ ref('stg_raw_ccx_stpcocdata_icare') }}

{% endsnapshot %}