{% snapshot cc_transactionlineitem_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_transactionlineitem.
                                                Source: ref('stg_raw_cc_transactionlineitem')
                                                unique_key: transactionlineitem_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='transactionlineitem_sk',
    strategy='check',
    alias='cc_transactionlineitem',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_transactionlineitem']
) }}

SELECT
    transactionlineitem_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    nonpecuniarybenefitd_icare,
    reportingamount,
    itcaaamountpel,
    paygtax_icare,
    grossweeklywagerate_icare,
    dateofservice_icare,
    paymentvariationreason_icare,
    reservingamount,
    claimamount,
    updatetime,
    hourspaid_icare,
    amountwithoutgst,
    id,
    paycodename_icare,
    defaultdecision,
    benefitsuspended_icare,
    createuserid,
    reportingforexamount,
    beanversion,
    retired,
    updateuserid,
    transactionamount,
    gstamountpel,
    gstoverchargedflag,
    financialyear_icare,
    garnishee_icare,
    publicid,
    createtime,
    weeklybenefitrate_icare,
    datefrom_icare,
    gstexemptflag,
    countofweeks_icare,
    earningse_icare,
    claimforexamount,
    lumpsume_icare,
    gstapplicable_icare,
    dateto_icare,
    gstdecision,
    archivepartition,
    paycodeglaggregation,
    payeeregisteredforgst,
    paycode_icareid,
    hourslost_icare,
    paycodereportattribute_icare,
    gstmethodpel_ext,
    deductibleid,
    paymentcategorycode,
    linecategory,
    itcapplicable_icare,
    comments,
    itcdecision,
    gazettedamount_icare,
    gstcalcrate_icare,
    transactionid,
    reservingforexamount,
    deemedearningsperweek_icare,
    serviceproviderid_icare,
    postgarnishee_icare,
    nocappaygtax_icare,
    medprovidertreatment_extid,
    provider_extid,
    dateofsurgery_ext,
    recalculatepaygforreissue_ext,
    rehabservice_extid,
    maxodgcaa_ext,
    draftamount_ext,
    meddomassist_extid,
    medtreatment_extid,
    medicationname_ext,
    drugprescribed_extid,
    weeklyactualrate_ext,
    previouswithheldpayg_ext,
    previouspaidpregarnishee_ext,
    previouslyprocessedpaycode_ext,
    cwwr_ext,
    previouspaidamount_ext,
    percentageofweek_ext,
    deductibledm_ext,
    adjustmentflag_ext,
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
FROM {{ ref('stg_raw_cc_transactionlineitem') }}

{% endsnapshot %}