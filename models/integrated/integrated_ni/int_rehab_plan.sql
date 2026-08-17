{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for Rehab Plan

-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_rehab_plan_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 24_REHAB_PLAN.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A24
  TBL_NM: MSC_QLK_ASPIRE_REHAB_PLAN
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

ccx_rehabplan_ext as (
    select
        id,
        claimid,
        goal_icare,
        claimsstrategygoal_icare,
        strategygoaldate_icare,
        createtime,
        estcompletiondate,
        compleximp_icare,
        summary,
        status,
        latestriskassessmentdate_ext,
        wcdstrategy,
        proposedwcddate
    from {{ ref('v_ccx_rehabplan_ext_current') }}
    where retired = 0
),

cctl_rtwgoal_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_rtwgoal_icare_current') }}
),

cctl_claimsstrategygoal_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_claimsstrategygoal_icare_current') }}
),

cctl_rehabplanstatus_ext as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_rehabplanstatus_ext_current') }}
),

cctl_wcdstrategy_ext as (
    select
        id,
        name
    from {{ ref('v_cctl_wcdstrategy_ext_current') }}
),
cte_union as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as src_system_cd,
    clm.id as src_claim_id,
    rehb.id as src_rehab_plan_id,
    clm.claimnumber as claim_nbr,
    dimgoal.name as rtw_goal_desc,
    dimstrat.name as rtw_strategy_goal_desc,
    cast(rehb.strategygoaldate_icare as date) as strategygoaldate_icare,
    CAST(rehb.createtime as TIMESTAMP_NTZ) AS  rehab_plan_create_dttm,
    cast(rehb.createtime as date) as rehab_plan_create_dt,
    cast(rehb.estcompletiondate as date) as rtw_strategy_est_completion_dt,
    case
        when rehb.compleximp_icare = 0 then 'N'
        when rehb.compleximp_icare = 1 then 'Y'
        else null
    end as complex_imp_ind,
    rehb.summary as claims_summary_goal,
    dimstat.name as rehab_plan_status,
    row_number() over (
        partition by clm.id
        order by coalesce(rehb.strategygoaldate_icare, '1900-01-01') desc,
            rehb.createtime desc, rehb.id desc
    ) as latest_rehab_plan,
    case
        when dimstat.typecode in ('active_icare', 'revised')
            and row_number() over (
                partition by clm.id
                order by coalesce(rehb.strategygoaldate_icare, '1900-01-01') asc,
                    rehb.createtime asc, rehb.id asc
            ) > 1 then 'Y'
        else 'N'
    end as subsequent_non_draft_imp_ind,
    case
        when dimstat.typecode in ('active_icare', 'revised')
            and row_number() over (
                partition by clm.id
                order by coalesce(rehb.strategygoaldate_icare, '1900-01-01') asc,
                    rehb.createtime asc, rehb.id asc
            ) = 1 then 'Y'
        else 'N'
    end as initial_non_draft_imp_ind,
    cast(rehb.latestriskassessmentdate_ext as date) as latest_risk_assessment_date,
    wcdstrat.name as wcd_strategy,
    cast(rehb.proposedwcddate as date) as proposed_wcd_date,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_claim clm
inner join ccx_rehabplan_ext rehb
    on clm.id = rehb.claimid
left join cctl_rtwgoal_icare dimgoal
    on rehb.goal_icare = dimgoal.id
left join cctl_claimsstrategygoal_icare dimstrat
    on rehb.claimsstrategygoal_icare = dimstrat.id
left join cctl_rehabplanstatus_ext dimstat
    on rehb.status = dimstat.id
left join cctl_wcdstrategy_ext wcdstrat
    on rehb.wcdstrategy = wcdstrat.id
)

select 
    claim_sk,
    src_system_cd,
    src_claim_id,
    src_rehab_plan_id,
    claim_nbr,
    rtw_goal_desc,
    rtw_strategy_goal_desc,
    strategygoaldate_icare,
    rehab_plan_create_dttm,
    rehab_plan_create_dt,
    rtw_strategy_est_completion_dt,
    complex_imp_ind,
    claims_summary_goal,
    rehab_plan_status,
    latest_rehab_plan,
    subsequent_non_draft_imp_ind,
    initial_non_draft_imp_ind,
    latest_risk_assessment_date,
    wcd_strategy,
    proposed_wcd_date,
    file_ingestion_timestamp   
from
    cte_union