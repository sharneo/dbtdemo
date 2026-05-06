{% snapshot pc_policyline_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_policyline.
                                                Source: ref('stg_raw_pc_policyline')
                                                unique_key: policyline_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='policyline_sk',
    strategy='check',
    alias='pc_policyline',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pc_policyline']
) }}

SELECT
    policyline_sk,
    hash_key,
    legacyappincentivecost,
    smallbusinesstype,
    legacyappincentivecost_cur,
    locationlimits,
    fixedid,
    glcoverageform,
    premiumadjustmentcontribution,
    updatetime,
    id,
    initialexclusionscreated,
    createuserid,
    pollutioncleanupexp,
    beanversion,
    legacyrtwicost,
    legacyclaimsperfadjcost,
    legacyrtwicost_cur,
    legacyclaimsperfadjcost_cur,
    updateuserid,
    numaddinsured,
    manuscriptpremium,
    manuscriptpremium_cur,
    splitlimits,
    referencedateinternal,
    legacybasictariffcost,
    legacybasictariffcost_cur,
    businessvehicleautonumberseq,
    personalvehicleautonumberseq,
    legacyminesafetycost,
    legacyminesafetycost_cur,
    blankettype,
    branchid,
    isexemptemployer_icare,
    totalpremiumadjustment,
    legacyclaimpermeasurecost,
    initialcoveragescreated,
    legacyclaimpermeasurecost_cur,
    publicid,
    customautosymboldesc,
    createtime,
    autosymbolsmanualeditdate,
    legacypdcost,
    legacypdcost_cur,
    fleet,
    viewbundledcoverages,
    retroactivedate,
    legacypremadjcontcost,
    legacypremadjcontcost_cur,
    effectivedate,
    legacyclaimsperfratecost,
    legacyclaimsperfratecost_cur,
    equipmentautonumberseq,
    expirationdate,
    legacyesicost,
    legacyesicost_cur,
    claimsmadeorigeffdate,
    archivepartition,
    legacyesrcost,
    governingclass,
    legacyesrcost_cur,
    changetype,
    manuscriptoptiondesc,
    policytype,
    initialconditionscreated,
    basedonid,
    legacydustdiseasecost,
    legacydustdiseasecost_cur,
    cpblanketautonumberseq,
    subtype,
    preferredcoveragecurrency,
    patterncode,
    crmigrated,
    premiumcapped_icare,
    overridereason,
    adjpremiumpercrate_icare,
    finalpremiumpercrate_icare,
    mixedwicpremiumpercrate_icare,
    premiumpercrate_icare,
    notcappedforuwreason_icare,
    cappingthreshold_icare,
    manadjpremiumpercrate_icare,
    isadaptivemaxapplied_icare,
    cappingtype_icare,
    changesinpprandcpr,
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
FROM {{ ref('stg_raw_pc_policyline') }}

{% endsnapshot %}