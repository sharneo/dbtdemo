-- Converted from W01_WCD_TRACKING_RPT.sas to Snowflake-compatible dbt incremental model
-- Co-authored with CoCo
{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Aspire - original table materialization
2026-07-13      1.0                             Converted to incremental with merge strategy
                                                NOTE: Original SAS used SQL Server temp tables.
                                                Restructured as CTEs for Snowflake compatibility.

-#}

{{
  config(
    materialized='incremental',
    unique_key='claim_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: W01_WCD_TRACKING_RPT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_W01
  TBL_NM: MSC_QLK_ASPIRE_WCD_TRACKING_RPT
  NOTE: This is a complex report that originally used multiple temp tables.
        The logic has been restructured into CTEs for Snowflake/dbt compatibility.
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        managingentity_icare,
        segment,
        assignedgroupid,
        assigneduserid,
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
        weeklypaymentimpact
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

base_cc_transaction as (
    select
        id,
        claimid,
        checkid,
        costcategory
    from {{ ref('v_cc_transaction_current') }}
    where retired = 0
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
),

base_cc_check as (
    select
        id,
        claimid,
        status
    from {{ ref('v_cc_check_current') }}
    where retired = 0
),

base_ccx_managingentity_icare as (
    select
        id,
        publicid,
        code
    from {{ ref('v_ccx_managingentity_icare_current') }}
    where retired = 0
),

base_cctl_claimsegment as (
    select
        id,
        name
    from {{ ref('v_cctl_claimsegment_current') }}
    where retired = 0
),

base_cc_group as (
    select
        id,
        name
    from {{ ref('v_cc_group_current') }}
),

base_cc_user as (
    select
        id,
        contactid
    from {{ ref('v_cc_user_current') }}
    where retired = 0
),

base_cc_contact as (
    select
        id,
        firstname,
        lastname
    from {{ ref('v_cc_contact_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_workcap as (
    select
        wcd.claimid,
        wcd.id as wcdid,
        wcd.referencenumber as wcdreferencenumber,
        cast(wcd.effectivedatedecision as date) as wcdeffdt,
        wpimpact.typecode as wcdweeklypaymentimpact,
        wpimpact.name as wcdweeklypaymentimpact_desc,
        row_number() over (
            partition by wcd.claimid
            order by wcd.referencenumber desc, wcd.effectivedatedecision desc
        ) as workcap_rank
    from base_ccx_workcapdecision_icare as wcd
    inner join base_cctl_reviewtype_icare as wcdtyp
        on wcdtyp.id = wcd.reviewtype
        and wcdtyp.typecode = '01'
    inner join base_cctl_status_icare as wcdsts
        on wcdsts.id = wcd.status
    left join base_cctl_proboutcome_icare as wpimpact
        on wpimpact.id = wcd.weeklypaymentimpact
),

cte_claim_wcd as (
    select
        c.id as claimid,
        c.claimnumber,
        c.managingentity_icare,
        c.segment,
        c.assignedgroupid,
        c.assigneduserid,
        c.source_system,
        c.file_ingestion_timestamp,
        wc.wcdreferencenumber,
        wc.wcdeffdt,
        wc.wcdweeklypaymentimpact,
        wc.wcdweeklypaymentimpact_desc
    from base_cc_claim as c
    inner join cte_workcap as wc
        on wc.claimid = c.id
        and wc.workcap_rank = 1
),

cte_payments as (
    select
        cw.claimid,
        cw.wcdreferencenumber,
        sum(case
            when cast(trnln.datefrom_icare as date) >= cw.wcdeffdt
            then trnln.transactionamount
            else 0
        end) as post_wcd_payment_amount,
        max(trnln.dateto_icare) as last_payment_to_dt
    from cte_claim_wcd as cw
    inner join base_cc_transaction as t
        on t.claimid = cw.claimid
    inner join base_cc_transactionlineitem as trnln
        on t.id = trnln.transactionid
    group by cw.claimid, cw.wcdreferencenumber
),

cte_final as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'cw.source_system',
            'cw.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        cw.source_system as src_system_cd,
        cw.managingentity_icare as managing_entity_id,
        cast({{ dbt_utils.generate_surrogate_key([
            'cw.source_system',
            'ent.publicid'
        ]) }} as varchar(150)) as managing_entity_sk,
        coalesce(ent.code, 'NI_ICARE') as managing_entity_cd,
        cw.claimnumber as claim_nbr,
        cw.claimid as src_claim_id,
        cw.wcdreferencenumber as wcd_reference_nbr,
        cw.wcdeffdt as wcd_effective_dt,
        cw.wcdweeklypaymentimpact as wcd_weekly_pymt_impact,
        cw.wcdweeklypaymentimpact_desc as wcd_weekly_pymt_impact_desc,
        seg.name as segment,
        team.name as team,
        concat(cttownr.firstname, ' ', cttownr.lastname) as case_owner_name,
        pymt.post_wcd_payment_amount,
        pymt.last_payment_to_dt,
        cw.file_ingestion_timestamp
    from cte_claim_wcd as cw
    left join base_ccx_managingentity_icare as ent
        on ent.id = cw.managingentity_icare
    left join base_cctl_claimsegment as seg
        on seg.id = cw.segment
    left join base_cc_group as team
        on team.id = cw.assignedgroupid
    left join base_cc_user as csownr
        on csownr.id = cw.assigneduserid
    left join base_cc_contact as cttownr
        on cttownr.id = csownr.contactid
    left join cte_payments as pymt
        on pymt.claimid = cw.claimid
        and pymt.wcdreferencenumber = cw.wcdreferencenumber
)

select
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
    wcd_weekly_pymt_impact_desc,
    segment,
    team,
    case_owner_name,
    post_wcd_payment_amount,
    last_payment_to_dt,
    file_ingestion_timestamp
from cte_final
qualify row_number() over (partition by claim_sk order by file_ingestion_timestamp desc) = 1
