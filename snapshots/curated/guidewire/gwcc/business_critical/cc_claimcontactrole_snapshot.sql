{% snapshot cc_claimcontactrole_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_claimcontactrole.
                                                Source: ref('stg_raw_cc_claimcontactrole')
                                                unique_key: claimcontactrole_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='claimcontactrole_sk',
    strategy='check',
    alias='cc_claimcontactrole',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_claimcontactrole']
) }}

SELECT
    claimcontactrole_sk,
    hash_key,
    loadcommandid,
    publicid,
    active,
    witnessstatementind,
    createtime,
    coveredpartytype,
    policyid,
    updatetime,
    evaluationid,
    id,
    matterid,
    exposureid,
    witnessposition,
    createuserid,
    witnessperspective,
    archivepartition,
    beanversion,
    retired,
    incidentid,
    partynumber,
    updateuserid,
    comments,
    role,
    negotiationid,
    claimcontactid,
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
FROM {{ ref('stg_raw_cc_claimcontactrole') }}

{% endsnapshot %}