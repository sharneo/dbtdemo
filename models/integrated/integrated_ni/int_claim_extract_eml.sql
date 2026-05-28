{{
  config(
    materialized='incremental',
    unique_key='src_claim_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 06_CLAIM_EXTRACT_EML.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A06
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_EXTRACT_EML
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        claimworkcompid,
        policyid,
        state,
        lossdate,
        closedate,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cc_exposure as (
    select
        id,
        claimid,
        exposuretype,
        state,
        closedoutcome
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
        typecode,
        name
    from {{ ref('v_cctl_exposureclosedoutcometype_current') }}
),

cc_workcomp as (
    select
        id,
        timelossreport
    from {{ ref('v_cc_workcomp_current') }}
    where retired = 0
),

cc_policy as (
    select
        id,
        policynumber
    from {{ ref('v_cc_policy_current') }}
    where retired = 0
),

cctl_claimstate as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_claimstate_current') }}
),

ccx_benefitsaccrual_icare as (
    select
        exposureid,
        totalweekspaid
    from {{ ref('v_ccx_benefitsaccrual_icare_current') }}
    where retired = 0
)

select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as source_system,
    clm.id as src_claim_id,
    clm.claimnumber as claim_nbr,
    pol.policynumber as policy_nbr,
    clmstate.typecode as claim_state_cd,
    clmstate.name as claim_state_desc,
    clm.lossdate as loss_dttm,
    cast(clm.lossdate as date) as loss_dt,
    clm.closedate as claim_close_dttm,
    cast(clm.closedate as date) as claim_close_dt,
    exptype.typecode as exposure_type_cd,
    exptype.name as exposure_type_desc,
    expstate.typecode as exposure_state_cd,
    expstate.name as exposure_state_desc,
    expoutc.typecode as exposure_closed_outcome_cd,
    expoutc.name as exposure_closed_outcome_desc,
    case
        when wc.timelossreport = 1 then 'Y'
        else 'N'
    end as lost_time_ind,
    bacc.totalweekspaid as total_weekly_benefit_paid_wk_count,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_claim clm

inner join cc_exposure exp
    on clm.id = exp.claimid

left join cctl_exposuretype exptype
    on exp.exposuretype = exptype.id

left join cctl_exposurestate expstate
    on exp.state = expstate.id

left join cctl_exposureclosedoutcometype expoutc
    on exp.closedoutcome = expoutc.id

left join cc_workcomp wc
    on clm.claimworkcompid = wc.id

left join cc_policy pol
    on clm.policyid = pol.id

left join cctl_claimstate clmstate
    on clm.state = clmstate.id

left join ccx_benefitsaccrual_icare bacc
    on bacc.exposureid = exp.id

{% if is_incremental() %}
where clm.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}
