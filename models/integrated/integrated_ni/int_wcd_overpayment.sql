{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Aspire - original table materialization
2026-07-13      1.0                             Converted to incremental with merge strategy
2027-07-13      2.0                             Version 3 

-#}

{{
  config(
    materialized='incremental',
    unique_key='wcd_overpayment_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: S03_WCD_OVERPAYMENT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_S03
  TBL_NM: MSC_QLK_ASPIRE_S03_WCD_OVERPAYMENT
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        managingentity_icare,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp > (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

base_ccx_workcapdecision_icare as (
    select
        id,
        claimid,
        referencenumber,
        reviewtype,
        status,
        effectivedatedecision,
        weeklypaymentimpact,
        section38eligible
    from {{ ref('v_ccx_workcapdecision_icare_current') }}
    where retired = 0
),

base_cctl_reviewtype_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_reviewtype_icare_current') }}
    where retired = 0
),

base_cctl_status_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_status_icare_current') }}
    where retired = 0
      and typecode = 'completed'
),

base_cctl_proboutcome_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_proboutcome_icare_current') }}
    where retired = 0
),

base_ccx_wcdlistwrapper_icare as (
    select
        id,
        workcapacitydecisionid,
        decision
    from {{ ref('v_ccx_wcdlistwrapper_icare_current') }}
    where retired = 0
),

base_cctl_wcdlist_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_wcdlist_icare_current') }}
    where retired = 0
),

base_ccx_wcdreviewdetails_icare as (
    select
        id,
        workcapacitydecisionid
    from {{ ref('v_ccx_wcdreviewdetails_icare_current') }}
    where retired = 0
),

base_ccx_wcdwccreview_icare as (
    select
        id,
        reviewdetailsid,
        reviewoutcome,
        stayapplicable
    from {{ ref('v_ccx_wcdwccreview_icare_current') }}
    where retired = 0
),

base_cctl_wcdreviewoutcome_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_wcdreviewoutcome_icare_current') }}
    where retired = 0
),

base_ccx_wcdinternalreview_icare as (
    select
        id,
        reviewdetailsid,
        reviewoutcome
    from {{ ref('v_ccx_wcdinternalreview_icare_current') }}
    where retired = 0
),

base_cc_check as (
    select
        id,
        claimid,
        status
    from {{ ref('v_cc_check_current') }}
    where retired = 0
      and status in (2, 5, 17)
),

base_cc_transaction as (
    select
        id,
        checkid,
        costcategory
    from {{ ref('v_cc_transaction_current') }}
),

base_cctl_costcategory as (
    select
        id,
        typecode
    from {{ ref('v_cctl_costcategory_current') }}
    where retired = 0
      and typecode = '50'
),

base_cc_transactionlineitem as (
    select
        id,
        transactionid,
        datefrom_icare,
        dateto_icare,
        transactionamount
    from {{ ref('v_cc_transactionlineitem_current') }}
    where retired = 0
      and transactionamount > 0
),

base_ccx_managingentity_icare as (
    select
        id,
        publicid,
        code
    from {{ ref('v_ccx_managingentity_icare_current') }}
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_wcd01 as (
    select
        wcd.claimid,
        wcd.id as wcdid,
        wcd.referencenumber,
        wcdtyp.typecode as wcdtype,
        cast(wcd.effectivedatedecision as date) as wcdeffdt,
        coalesce(wpimpact.typecode, ' ') as wcdweeklypaymentimpact,
        sum(case when coalesce(lst.typecode, '0') = '04' then 0 else 1 end) as cntother
    from base_ccx_workcapdecision_icare as wcd
    inner join base_cctl_reviewtype_icare as wcdtyp
        on wcdtyp.id = wcd.reviewtype
        and wcdtyp.typecode in ('01', 's38_decision')
    inner join base_cctl_status_icare as wcdsts
        on wcdsts.id = wcd.status
    left join base_cctl_proboutcome_icare as wpimpact
        on wpimpact.id = wcd.weeklypaymentimpact
    left join base_ccx_wcdlistwrapper_icare as wrp
        on wrp.workcapacitydecisionid = wcd.id
    left join base_cctl_wcdlist_icare as lst
        on lst.id = wrp.decision
    where (wcdtyp.typecode = '01'
           and coalesce(wpimpact.typecode, ' ') <> 'no_change'
           and cast(coalesce(wcd.effectivedatedecision, '1000-01-01') as date) <= current_date())
        or (wcdtyp.typecode = 's38_decision' and wcd.section38eligible = 1)
    group by wcd.claimid, wcd.id, wcd.referencenumber, wcdtyp.typecode,
             cast(wcd.effectivedatedecision as date), coalesce(wpimpact.typecode, ' ')
),

cte_wcd02 as (
    select
        claimid, wcdid, referencenumber, wcdtype, wcdeffdt, wcdweeklypaymentimpact,
        row_number() over (partition by claimid order by referencenumber desc, wcdeffdt desc) as wcdrank
    from cte_wcd01
    where (wcdtype = '01' and cntother > 0)
        or wcdtype = 's38_decision'
),

cte_wcd_latest as (
    select
        wcd.*,
        picout.name as picrvwoutcome
    from cte_wcd02 as wcd
    left join base_ccx_wcdreviewdetails_icare as rvw
        on rvw.workcapacitydecisionid = wcd.wcdid
    left join base_ccx_wcdwccreview_icare as picrvw
        on picrvw.reviewdetailsid = rvw.id
    left join base_cctl_wcdreviewoutcome_icare as picout
        on picout.id = picrvw.reviewoutcome
    left join base_ccx_workcapdecision_icare as nxtwcd
        on nxtwcd.claimid = wcd.claimid
        and nxtwcd.referencenumber > wcd.referencenumber
        and cast(nxtwcd.effectivedatedecision as date) < wcd.wcdeffdt
    left join base_cctl_reviewtype_icare as nxtwcdtyp
        on nxtwcdtyp.id = nxtwcd.reviewtype
        and nxtwcdtyp.typecode = '01'
    left join base_cctl_proboutcome_icare as nxtwpimpact
        on nxtwpimpact.id = nxtwcd.weeklypaymentimpact
        and nxtwpimpact.typecode = 'no_change'
    left join base_ccx_wcdinternalreview_icare as ir
        on ir.reviewdetailsid = wcd.wcdid
    left join base_cctl_wcdreviewoutcome_icare as irout
        on irout.id = ir.reviewoutcome
        and irout.typecode in ('23', '24')
    where wcd.wcdrank = 1
      and wcd.wcdtype = '01'
      and wcd.wcdweeklypaymentimpact in ('reduction_to_0', 'no_entitlement')
      and coalesce(picrvw.stayapplicable, 0) = 0
      and coalesce(picout.typecode, '00') not in ('23', '24')
      and nxtwcd.id is null
      and ir.id is null
),

cte_overpymt as (
    select
        wcd.claimid,
        wcd.referencenumber,
        wcd.wcdeffdt,
        wcd.wcdweeklypaymentimpact,
        cast(max(ln.dateto_icare) as date) as latest_date_paid_to,
        count(*) as overpaid_week_count,
        sum(ln.transactionamount) as overpaid_amount
    from cte_wcd_latest as wcd
    inner join base_cc_check as chq
        on chq.claimid = wcd.claimid
    inner join base_cc_transaction as trn
        on trn.checkid = chq.id
    inner join base_cctl_costcategory as ccat
        on ccat.id = trn.costcategory
    inner join base_cc_transactionlineitem as ln
        on ln.transactionid = trn.id
        and cast(ln.datefrom_icare as date) >= wcd.wcdeffdt
    group by wcd.claimid, wcd.referencenumber, wcd.wcdeffdt, wcd.wcdweeklypaymentimpact
),

final as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber',
            'wcd.referencenumber'
        ]) }} as varchar(150)) as wcd_overpayment_sk,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.managingentity_icare as managing_entity_id,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'ent.publicid'
        ]) }} as varchar(150)) as managing_entity_sk,
        coalesce(ent.code, 'NI_ICARE') as managing_entity_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        wcd.referencenumber as wcd_reference_nbr,
        wcd.wcdeffdt as wcd_effective_dt,
        wcd.wcdweeklypaymentimpact as wcd_weekly_pymt_impact,
        wcd.latest_date_paid_to,
        wcd.overpaid_week_count,
        wcd.overpaid_amount,
        clm.file_ingestion_timestamp
    from cte_overpymt as wcd
    inner join base_cc_claim as clm
        on clm.id = wcd.claimid
    left join base_ccx_managingentity_icare as ent
        on ent.id = clm.managingentity_icare
)

select
    wcd_overpayment_sk,
    claim_sk,
    src_system_cd,
    managing_entity_id,
    managing_entity_sk,
    managing_entity_cd,
    claim_nbr,
    src_claim_id,
    wcd_reference_nbr,
    wcd_effective_dt,
    wcd_weekly_pymt_impact,
    latest_date_paid_to,
    overpaid_week_count,
    overpaid_amount,
    file_ingestion_timestamp
from final
