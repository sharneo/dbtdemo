{{
  config(
    materialized='incremental',
    unique_key=['src_claim_id', 'src_liability_status_hist_id'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 20_LIABILITY_STATUS.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A20
  TBL_NM: MSC_QLK_ASPIRE_LIABILITY_STATUS
-#}

with ccx_liabilitystatushist_icare as (
    select
        id,
        publicid,
        claimworkcompid,
        liabilitystatus,
        createtime,
        liabilitystatusdate,
        ctmliabilitystatusdecisiondate,
        provisionalweeks,
        weeklybenefitenddate,
        noticeperiod,
        medicalbenefitenddate,
        createuserid,
        refid,
        file_ingestion_timestamp
    from {{ ref('v_ccx_liabilitystatushist_icare_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cctl_compensabilitydecision as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_compensabilitydecision_current') }}
),

cc_claim as (
    select
        id,
        claimnumber,
        claimworkcompid,
        source_system
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cc_workcomp as (
    select
        id,
        reasonableexcuse_icare
    from {{ ref('v_cc_workcomp_current') }}
    where retired = 0
),

cctl_reasonableexcuse_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_reasonableexcuse_icare_current') }}
),

cc_user as (
    select
        id,
        publicid
    from {{ ref('v_cc_user_current') }}
),

cctl_liabilitystatnotper_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_liabilitystatnotper_icare_current') }}
),
cte_join as
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as source_system,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    libl.id as src_liability_status_hist_id,
    libl.publicid as claim_public_id,
    libl.refid as claim_liability_status_id,
    liblstus.typecode as claim_liability_status_cd,
    liblstus.name as claim_liability_status_desc,
    rsn.typecode as reasonable_excuse_cd,
    rsn.name as reasonable_excuse_desc,
    libl.createtime as src_create_dttm,
    cast(libl.createtime as date) as src_create_dt,
    libl.liabilitystatusdate as claim_liability_status_eff_dttm,
    cast(libl.liabilitystatusdate as date) as claim_liability_status_eff_dt,
    libl.ctmliabilitystatusdecisiondate as claim_liability_status_ent_dttm,
    cast(libl.ctmliabilitystatusdecisiondate as date) as claim_liability_status_ent_dt,
    libl.provisionalweeks as provisional_liab_appr_weeks_qty,
    cast(libl.weeklybenefitenddate as date) as liability_wkly_benf_end_dt,
    notp.name as notice_period,
    cast(libl.medicalbenefitenddate as date) as mbed,
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'usr.publicid'
    ]) }} as varchar(150)) as liability_status_create_user_sk,
    row_number() over (
        partition by clm.id
        order by libl.ctmliabilitystatusdecisiondate desc, libl.createtime desc, libl.refid desc
    ) as latest_liab_status_record_rank,
    row_number() over (
        partition by clm.id
        order by libl.ctmliabilitystatusdecisiondate asc, libl.createtime asc, libl.refid asc
    ) as earlst_liab_status_record_rank,
    current_date() as extract_date,
    libl.file_ingestion_timestamp

from ccx_liabilitystatushist_icare libl

left join cctl_compensabilitydecision liblstus
    on libl.liabilitystatus = liblstus.id

inner join cc_claim clm
    on libl.claimworkcompid = clm.claimworkcompid

inner join cc_workcomp wc
    on libl.claimworkcompid = wc.id

left join cctl_reasonableexcuse_icare rsn
    on wc.reasonableexcuse_icare = rsn.id

inner join cc_user usr
    on usr.id = libl.createuserid

left join cctl_liabilitystatnotper_icare notp
    on notp.id = libl.noticeperiod
)

select 
    claim_sk,
    source_system,
    claim_nbr,
    src_claim_id,
    src_liability_status_hist_id,
    claim_public_id,
    claim_liability_status_id,
    claim_liability_status_cd,
    claim_liability_status_desc,
    reasonable_excuse_cd,
    reasonable_excuse_desc,
    CAST(src_create_dttm AS TIMESTAMP_NTZ) as src_create_dttm,
    src_create_dt,
    CAST(claim_liability_status_eff_dttm AS TIMESTAMP_NTZ) as claim_liability_status_eff_dttm,
    claim_liability_status_eff_dt,
    CAST(claim_liability_status_ent_dttm AS TIMESTAMP_NTZ) as claim_liability_status_ent_dttm,
    claim_liability_status_ent_dt,
    provisional_liab_appr_weeks_qty,
    liability_wkly_benf_end_dt,
    notice_period,
    mbed,
    liability_status_create_user_sk,
    latest_liab_status_record_rank,
    earlst_liab_status_record_rank,
    file_ingestion_timestamp
from
    cte_join