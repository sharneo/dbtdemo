{% snapshot pc_jobgroup_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_jobgroup.
                                                Source: ref('stg_raw_pc_jobgroup')
                                                unique_key: jobgroup_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='jobgroup_sk',
    strategy='check',
    alias='pc_jobgroup',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'business_critical', 'pc_jobgroup']
) }}

SELECT
    jobgroup_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    createuserid,
    groupnumber_icare,
    publicid,
    beanversion,
    groupannwages,
    retired,
    createtime,
    name,
    account,
    updateuserid,
    groupstatus,
    cpacategory,
    updatetime,
    grouptype,
    subtype,
    id,
    groupannbtp,
    groupsecurity_cur,
    lprproduct,
    groupdepositpremium_amt,
    sfactor,
    groupsecurity_amt,
    groupstage,
    groupsecurityperc,
    termnumber,
    lprclaimslimit,
    groupdepositpremium_cur,
    groupcostofclaims_cur,
    groupcostofclaims_amt,
    archivepartition,
    grpbasepremforlprplus_cur,
    grpbasepremforlprplus_amt,
    groupbtp_ext,
    grouptotalpremium,
    recalculateavailable,
    isissueclicked,
    policychangedescription,
    isrecalculategrp,
    policychangetype,
    policychangereason,
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
FROM {{ ref('stg_raw_pc_jobgroup') }}

{% endsnapshot %}