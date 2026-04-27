{% snapshot ccx_paygsummaryhistory_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_paygsummaryhistory_icare.
                                                Source: ref('stg_raw_ccx_paygsummaryhistory_icare')
                                                unique_key: paygsummaryhistory_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='paygsummaryhistory_icare_sk',
    strategy='check',
    alias='ccx_paygsummaryhistory_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'ccx_paygsummaryhistory_icare']
) }}

SELECT
    paygsummaryhistory_icare_sk,
    hash_key,
    loadcommandid,
    fbtamount,
    payeesurname,
    updatetime,
    payeeaddr1,
    payeeaddr2,
    payeeaddr3,
    id,
    secondlatestyearlumpsume,
    secondlatestlumpsumeyear,
    totalallowances,
    createuserid,
    amendmentindicator,
    workplacegivingamt,
    beanversion,
    retired,
    latestyearlumpsume,
    latestlumpsumeyear,
    tottaxwithheld,
    updateuserid,
    payerid,
    dateproduced,
    paygsummarystatus,
    tfn,
    paygsnapshot_icare,
    paygsummary_icare,
    pmtperiodend,
    lumpsumpmtatype,
    publicid,
    financialyear,
    payee,
    createtime,
    payeestate,
    pmtperiodstart,
    cdep,
    paygsummarychanged,
    payeecountry,
    payeepostcode,
    claimid,
    outboundpack_icareid,
    lumpsuma,
    lumpsumb,
    lumpsumd,
    lumpsume,
    payeemiddlename,
    priorsecondlatestyearlumpsume,
    priorsecondlatestlumpsumeyear,
    archivepartition,
    payeesuburb,
    payeedob,
    grosspayments,
    payeefirstname,
    errordescription,
    subtype,
    reportablesuperamt,
    holdreason,
    holddate,
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
FROM {{ ref('stg_raw_ccx_paygsummaryhistory_icare') }}

{% endsnapshot %}