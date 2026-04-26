{% snapshot cc_policy_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_policy.
                                                Source: ref('stg_raw_cc_policy')
                                                unique_key: policy_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='policy_sk',
    strategy='check',
    alias='cc_policy',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_policy']
) }}

SELECT
    policy_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    groupnumber_icare,
    policysuffix,
    acn_icare,
    policyid_icare,
    totalvehicles,
    otherinsinfo,
    insuredsiccode,
    currency,
    totalbtp_icare,
    updatetime,
    id,
    assignedrisk,
    financialinterests,
    createuserid,
    returntoworkprgm,
    itcpercentagepel,
    beanversion,
    retired,
    validationlevel,
    coverageform,
    updateuserid,
    cancellationdate,
    origeffectivedate,
    accountnumber,
    businessdescription_icare,
    foreigncoverage,
    businessgstregistration_icare,
    legacypolicynumber_icare,
    notes,
    participation,
    publicid,
    wcotherstates,
    verified,
    createtime,
    policyperiodid_icare,
    otherinsurance,
    policysource,
    wcstates,
    labourhire_icare,
    businessorganisationtype_icare,
    retroactivedate,
    effectivedate,
    producername_icare,
    reportingdate,
    expirationdate,
    businessyearstarted_icare,
    policytype_icare,
    archivepartition,
    policytype,
    underwritingco,
    status,
    employercategory_icare,
    totalproperties,
    policyratingplan,
    customerservicetier,
    manualverify_icare,
    policynumber,
    underwritinggroup,
    policysystemperiodid,
    producercode,
    tariffrate_icare,
    specialistcsp_extid,
    generalistcsp_extid,
    tmfcostcentrerequiredflag_ext,
    agencycode_ext,
    offering_ext,
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
FROM {{ ref('stg_raw_cc_policy') }}

{% endsnapshot %}