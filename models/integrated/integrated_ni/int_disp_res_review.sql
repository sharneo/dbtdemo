{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Aspire - original table materialization
2026-04-20      1.0                             Converted to incremental with merge strategy

-#}

{{
  config(
    materialized='incremental',
    unique_key='claim_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

with cc_claim as (
    select
          id
        , claimnumber
        , source_system
        , retired
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}

),

ccx_dispute_icare as (
    select
          id
        , claimid
        , referencenumber
        , type
        , reviewoutcome
        , reviewoutcomereasoning
        , wpi
        , medentitlementperiod
        , weeklypaymentimpact
        , draftdecisioncompleted
        , decisionissuedate
        , decisioneffectivedate
        , retired
    from {{ ref('v_ccx_dispute_icare_current') }}
    where retired = 0
),

cctl_dispreviewoutcome_icare as (
    select
          id
        , description
        , retired
    from {{ ref('v_cctl_dispreviewoutcome_icare_current') }}
    where retired = 0
),

cctl_medentitlement_icare as (
    select
          id
        , description
        , retired
    from {{ ref('v_cctl_medentitlement_icare_current') }}
    where retired = 0
),

cctl_paymentimpact_icare as (
    select
          id
        , description
        , retired
    from {{ ref('v_cctl_paymentimpact_icare_current') }}
    where retired = 0
),

ccx_disputerevdecwrap_icare as (
    select
          disputeid
        , decision
        , retired
    from {{ ref('v_ccx_disputerevdecwrap_icare_current') }}
    where retired = 0
),

cctl_dispreviewreqlist_icare as (
    select
          id
        , description
        , retired
    from {{ ref('v_cctl_dispreviewreqlist_icare_current') }}
    where retired = 0
),

final as (
    select
          cast({{ dbt_utils.generate_surrogate_key(['clm.source_system', 'clm.claimnumber']) }} as varchar(150)) as claim_sk
        , clm.source_system              as src_system_cd
        , clm.claimnumber                as claim_nbr
        , clm.id                         as src_claim_id
        , dsp.referencenumber            as reference_no
        , dsp.type                       as type_cd
        , dsp.reviewoutcome              as review_outcome_cd
        , rvwout.description             as review_outcome_desc
        , dsp.reviewoutcomereasoning     as reasoning
        , rvwdcsn.description            as review_decision
        , dsp.wpi                        as wpi
        , dsp.medentitlementperiod       as med_entitlement_period_cd
        , mdlent.description             as medical_entitlement_period
        , dsp.weeklypaymentimpact        as weekly_payment_impact_cd
        , pymt.description               as weekly_payment_impact_desc
        , case when dsp.draftdecisioncompleted = 0 then 'No'
               when dsp.draftdecisioncompleted = 1 then 'Yes'
               else null
          end                            as draft_decision_completed
        , cast(dsp.decisionissuedate as date)      as decision_issue_date
        , cast(dsp.decisioneffectivedate as date)  as decision_effective_date
    from cc_claim as clm
    join ccx_dispute_icare as dsp
        on clm.id = dsp.claimid
    left join cctl_dispreviewoutcome_icare as rvwout
        on rvwout.id = dsp.reviewoutcome
    left join cctl_medentitlement_icare as mdlent
        on mdlent.id = dsp.medentitlementperiod
    left join cctl_paymentimpact_icare as pymt
        on pymt.id = dsp.weeklypaymentimpact
    left join ccx_disputerevdecwrap_icare as dcsn
        on dcsn.disputeid = dsp.id
    left join cctl_dispreviewreqlist_icare as rvwdcsn
        on dcsn.decision = rvwdcsn.id
)

select
      claim_sk
    , src_system_cd
    , claim_nbr
    , src_claim_id
    , reference_no
    , type_cd
    , review_outcome_cd
    , review_outcome_desc
    , reasoning
    , review_decision
    , wpi
    , med_entitlement_period_cd
    , medical_entitlement_period
    , weekly_payment_impact_cd
    , weekly_payment_impact_desc
    , draft_decision_completed
    , decision_issue_date
    , decision_effective_date
from final
