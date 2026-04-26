{% snapshot bc_account_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_account.
                                                Source: ref('stg_raw_bc_account')
                                                unique_key: account_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='account_sk',
    strategy='check',
    alias='bc_account',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_account']
) }}

SELECT
    account_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    heldforinvoicesending,
    securityzoneid,
    acn_icare,
    secondtwicepermthinvoicedom,
    currency,
    updatetime,
    servicetier,
    hiddentaccountcontainerid,
    id,
    trustname_icare,
    createuserid,
    dba,
    fein,
    delinquencystatus,
    closedate,
    abn_icare,
    beanversion,
    retired,
    trusteetype_icare,
    updateuserid,
    segment,
    accountnumber,
    delinquencyplanid,
    accountname,
    billingplanid,
    publicid,
    createtime,
    trustabn_icare,
    firsttwicepermthinvoicedom,
    invoicedayofmonth,
    invoicedayofweek,
    invoicedeliverytype,
    accountnamekanji,
    feindenorm,
    allocationplanid,
    accounttype,
    chargeheld,
    billinglevel,
    newpolicypaymentdistributable,
    organizationtypedenorm,
    organizationtype,
    distributionlimittype,
    billdateorduedatebilling,
    accountnumberdenorm,
    accountnamedenorm,
    collecting,
    everyotherweekinvoiceanchor,
    collectionagencyid,
    crmuniqueid_icare,
    sourcesystem_icare,
    crn_icare,
    crmversion_icare,
    offeringtype_icare,
    lobaccounttype_icare,
    treasuryprimenumber_icare,
    excludeforreferral_ext,
    rnswclosurestatus_ext,
    insolvency_ext,
    disablebrokercopy_ext,
    creationsource_ext,
    isulaccount_ext,
    accountstatus_ext,
    abnstatus_ext,
    clientengagementmanager_ext,
    istmfaccount_ext,
    activefrom_ext,
    intermediarycode_ext,
    agencytype_ext,
    busopsdesc_ext,
    portfolio_ext,
    accountorgtype_ext,
    yearbusinessstarted_ext,
    agencycode_ext,
    activeto_ext,
    accountgroup_extid,
    icp_ext,
    pool_ext,
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
FROM {{ ref('stg_raw_bc_account') }}

{% endsnapshot %}