{{
  config(
    materialized='incremental',
    unique_key='claim_sk',
    incremental_strategy='merge'
  )
}}

with cc_claim as (
    select
        id,
        claimnumber,
        retired,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cc_exposurerpt as (
    select
        claimid,
        id,
        exposureid,
        availablereserves,
        openreserves,
        openrecoveryreserves,
        remainingreserves,
        futurepayments,
        totalpayments,
        totalrecoveries,
        updatetime
    from {{ ref('v_cc_exposurerpt_current') }}
    where retired = 0
),

cc_exposure as (
    select
        id,
        exposuretype,
        closedate,
        state,
        closedoutcome,
        assigneduserid,
        assignedgroupid,
        workerpayer_icare
    from {{ ref('v_cc_exposure_current') }}
    where retired = 0
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
        totalweekspaid
    from {{ ref('v_ccx_benefitsaccrual_icare_current') }}
    where retired = 0
),

cctl_workerpayer_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_workerpayer_icare_current') }}
    where retired = 0
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
    md5(concat('GWCC', clm.claimnumber)) as claim_sk,
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
    clm.file_ingestion_timestamp

from cc_claim clm

inner join cc_exposurerpt exprpt
    on clm.id = exprpt.claimid

inner join cc_exposure exps
    on exprpt.exposureid = exps.id

inner join cctl_exposuretype dim_exp
    on exps.exposuretype = dim_exp.id

left join ccx_benefitsaccrual_icare benacc
    on benacc.exposureid = exps.id

left join cctl_workerpayer_icare pyr
    on pyr.id = exps.workerpayer_icare

left join cctl_exposurestate sts
    on sts.id = exps.state

left join cctl_exposureclosedoutcometype outc
    on outc.id = exps.closedoutcome

{% if is_incremental() %}
where clm.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% else %}
where 1=1
{% endif %}
