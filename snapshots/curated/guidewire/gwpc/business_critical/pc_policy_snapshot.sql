{% snapshot pc_policy_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_policy.
                                                Source: ref('stg_raw_pc_policy')
                                                unique_key: policy_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='policy_sk',
    strategy='check',
    alias='pc_policy',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'business_critical', 'pc_policy']
) }}

SELECT
    policy_sk,
    hash_key,
    donotdestroy,
    isportalpolicy_icare,
    publicid,
    priorpremiums,
    issuedate,
    priorpremiums_cur,
    movedpolicysourceaccountid,
    accountid,
    createtime,
    losshistorytype,
    excludedfromarchive,
    archivestate,
    archiveschemainfo,
    archivefailuredetailsid,
    packagerisk,
    numpriorlosses,
    updatetime,
    primarylanguage,
    donotarchive,
    id,
    primarylocale,
    productcode,
    excludereason,
    groupnumberfromportal,
    createuserid,
    archivefailureid,
    crnnumber_icare,
    originaleffectivedate,
    beanversion,
    archivepartition,
    retired,
    updateuserid,
    priortotalincurred,
    archivedate,
    priortotalincurred_cur,
    producercodeofserviceid,
    newproducercode_ext,
    newclaimschemeagent_icare,
    movedpolsrcacctpubid,
    agencycontactdetails_ext,
    agencycontactemail_ext,
    agencycontactnumber_ext,
    insurancebook_extid,
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
FROM {{ ref('stg_raw_pc_policy') }}

{% endsnapshot %}