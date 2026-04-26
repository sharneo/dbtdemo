{% snapshot pc_losshistentry_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_losshistentry.
                                                Source: ref('stg_raw_pc_losshistentry')
                                                unique_key: losshistentry_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='losshistentry_sk',
    strategy='check',
    alias='pc_losshistentry',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pc_losshistentry']
) }}

SELECT
    losshistentry_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    wages1_icare_cur,
    wages2_icare_cur,
    publicid,
    wages3_icare_cur,
    wages1,
    wages2,
    wages3,
    createtime,
    policyid,
    islossadded_icare,
    policyperiodid,
    claimyears1_icare_cur,
    claimyears2_icare_cur,
    claimyears3_icare_cur,
    lossstatus,
    updatetime,
    policylinepatterncode,
    claimyears1,
    claimyears2,
    claimyears3,
    id,
    costcenterno_icare,
    createuserid,
    amountpaid,
    amountpaid_cur,
    losscause,
    beanversion,
    archivepartition,
    directwageid,
    amountresv,
    updateuserid,
    amountresv_cur,
    btpyear1_icare_cur,
    btpyear2_icare_cur,
    btpyear3_icare_cur,
    btpyear1,
    numofpercapunit1_icare,
    btpyear2,
    numofpercapunit2_icare,
    btpyear3,
    numofpercapunit3_icare,
    totalincurred,
    totalincurred_cur,
    locationname_icare,
    description,
    occurrencedate,
    addedtopolicyperiodterm_icare,
    losshistorysource_icare,
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
FROM {{ ref('stg_raw_pc_losshistentry') }}

{% endsnapshot %}