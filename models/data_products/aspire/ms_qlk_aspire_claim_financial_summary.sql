{{
  config(
    materialized='incremental',
    unique_key='claim_sk, src_exp_id',
    incremental_strategy='merge'
  )
}}

{#
  Source: 07_CLAIM_FINANCIAL_SUMMARY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A07
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_FINANCIAL_SUMMARY

  Version History:
  0.1  13/06/19  V.Lau  Initial Version
  0.2  27/08/19  V.Lau  Added TOTAL_WEEKLY_BENEFIT_PAID_WK_COUNT
  0.3  29/03/21  Yu Yain  EST10-1875: Added Worker_Payer
  0.4  01/08/23  Yu Yain  TDAREP-3066: Added Exposure Status, Outcome, CloseDate, AssignedUser, AssignedTeam
#}

with cc_claim as (
    select
        id,
        claimnumber,
        retired
    from {{ ref('v_cc_claim_current') }}
),
cc_exposurerpt as (
    select
        id,
        claimid,
        exposureid,
        availablereserves,
        openreserves,
        openrecoveryreserves,
        remainingreserves,
        futurepayments,
        totalpayments,
        totalrecoveries,
        updatetime,
        retired,
        file_ingestion_timestamp
    from {{ ref('v_cc_exposurerpt_current') }}
),
cc_exposure as (
    select
        id,
        exposuretype,
        workerpayer_icare,
        state,
        closedoutcome,
        closedate,
        assigneduserid,
        assignedgroupid,
        retired
    from {{ ref('v_cc_exposure_current') }}
),
cctl_exposuretype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_exposuretype_current') }}
),
ccx_benefitsaccrual_icare as (
    select
        exposureid,
        totalweekspaid,
        retired
    from {{ ref('v_ccx_benefitsaccrual_icare_current') }}
),
cctl_workerpayer_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_workerpayer_icare_current') }}
),
cctl_exposurestate as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_exposurestate_current') }}
),
cctl_exposureclosedoutcometype as (
    select
        id,
        name
    from {{ ref('v_cctl_exposureclosedoutcometype_current') }}
)

select
    md5('GWCC' || clm.claimnumber) as claim_sk,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    exprpt.id as src_exp_rpt_id,
    exps.id as src_exp_id,
    dim_exp.typecode as exposure_type_cd,
    dim_exp.name as exposure_type_desc,
    exprpt.availablereserves as available_reserves,
    exprpt.openreserves as open_reserves,
    exprpt.openrecoveryreserves as open_recovery_reserves,
    exprpt.remainingreserves as remaining_reserves,
    exprpt.futurepayments as future_payments,
    exprpt.totalpayments as total_payments_made,
    exprpt.totalrecoveries as total_recoveries,
    exprpt.updatetime as summary_eff_dttm,
    pyr.name as worker_payer,
    benacc.totalweekspaid as total_weekly_benefit_paid_wk_count,
    sts.typecode as exp_status_cd,
    sts.name as exp_status_desc,
    cast(exps.closedate as date) as exp_close_dt,
    outc.name as exp_closed_outcome,
    exps.assigneduserid as assigned_user_id,
    exps.assignedgroupid as assigned_team_id,
    current_date() as extract_date,
    exprpt.file_ingestion_timestamp

from cc_claim clm

join cc_exposurerpt exprpt
    on clm.id = exprpt.claimid
    and exprpt.retired = 0

join cc_exposure exps
    on exprpt.exposureid = exps.id
    and exps.retired = 0

join cctl_exposuretype dim_exp
    on exps.exposuretype = dim_exp.id

left join ccx_benefitsaccrual_icare benacc
    on benacc.exposureid = exps.id
    and benacc.retired = 0

left join cctl_workerpayer_icare pyr
    on pyr.id = exps.workerpayer_icare
    and pyr.retired = 0

left join cctl_exposurestate sts
    on sts.id = exps.state

left join cctl_exposureclosedoutcometype outc
    on outc.id = exps.closedoutcome

{% if is_incremental() %}
where exprpt.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% else %}
where 1=1
{% endif %}
