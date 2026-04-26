{% snapshot ccx_paygsummary_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_paygsummary_icare.
                                                Source: ref('stg_raw_ccx_paygsummary_icare')
                                                unique_key: paygsummary_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='paygsummary_icare_sk',
    strategy='check',
    alias='ccx_paygsummary_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'ccx_paygsummary_icare']
) }}

SELECT
    paygsummary_icare_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
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
    pmtperiodend,
    lumpsumpmtatype,
    publicid,
    financialyear,
    payeestate,
    createtime,
    pmtperiodstart,
    cdep,
    paygsummarychanged,
    payeecountry,
    payeepostcode,
    claimid,
    paygsnapshot_icareid,
    outboundpack_icareid,
    lumpsuma,
    lumpsumb,
    payeeid,
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
FROM {{ ref('stg_raw_ccx_paygsummary_icare') }}

{% endsnapshot %}