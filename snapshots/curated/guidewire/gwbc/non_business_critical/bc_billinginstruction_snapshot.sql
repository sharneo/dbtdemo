{% snapshot bc_billinginstruction_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_billinginstruction.
                                                Source: ref('stg_raw_bc_billinginstruction')
                                                unique_key: billinginstruction_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='billinginstruction_sk',
    strategy='check',
    alias='bc_billinginstruction',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_billinginstruction']
) }}

SELECT
    billinginstruction_sk,
    hash_key,
    loadcommandid,
    holdunbilledpremiumcharges,
    cancellationreason,
    accountid,
    paymentplanid,
    issuanceaccountid,
    executed,
    renewalaccountid,
    currency,
    totalpremium,
    updatetime,
    policypaymentplanid,
    priorpolicyperiodid,
    id,
    specialhandling,
    createuserid,
    offernumber,
    totalpremium_ha,
    finalaudit,
    beanversion,
    periodenddate,
    updateuserid,
    newrenewalaccountid,
    segregatedcollreqid,
    cancellationtype,
    totalpremium_awa,
    jobnumber_icare,
    publicid,
    policyboundorissue_icare,
    collateralrequirementid,
    createtime,
    modificationdate,
    policyid,
    finalaudit_awa,
    islegacytxn_icare,
    premiumreportduedateid,
    finalaudit_ha,
    associatedpolicyperiodid,
    paymentduedate,
    billinginstructiondate,
    depositrequirement,
    depositrequirement_cur,
    paymentreceived,
    archivepartition,
    newpolicyperiodid,
    periodstartdate,
    subtype,
    description,
    rewriteaccountid,
    policychangetype_icare,
    originaljobnumber_icare,
    issueddate_icare,
    rewritetype,
    fpdrevbipublicid_icare,
    confirmationdate_icare,
    renewalstate_icare,
    'GWBC' AS source_system,
    gwcbi_connector_ts_ms,
    gwcbi_lsn,
    gwcbi_operation,
    gwcbi_payload_ts_ms,
    gwcbi_seqval,
    gwcbi_seqval_hex,
    gwcbi_tx_id,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('stg_raw_bc_billinginstruction') }}

{% endsnapshot %}