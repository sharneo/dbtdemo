{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for Work Capacity 

-#}   

{{
  config(
    materialized='incremental',
    unique_key=['src_claim_id', 'decision_id'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 30_WORK_CAPACITY_DECISION.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A30
  TBL_NM: MSC_QLK_ASPIRE_WORK_CAPACITY_DECISION
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

ccx_workcapdecision_icare as (
    select
        id,
        claimid,
        publicid,
        referencenumber,
        issuedatedecision,
        effectivedatedecision,
        status,
        reviewtype,
        reviewperiod,
        relatedto,
        evaluationprobableoutcome,
        currentworkcapacity,
        currentworkstatus,
        fairnoticerequired,
        draftfairnoticeready,
        issuedatefairnotice,
        effectivedatefairnotice,
        assessedhours,
        earningse,
        earningscapacity,
        wcdpiawe,
        wpipercentage,
        medentitlement,
        weeklypaymentimpact,
        decisionreasoning,
        draftdecisioncompleted,
        furtherinfosubmitted,
        furtherinfoimpactsdecision,
        furtherinforeasoning,
        working15hourperweek,
        working15hourperweektext,
        earningperweek,
        earningperweektext,
        assessedasindefinitelyunable,
        assessedasunabletext,
        section38eligible,
        eligibilityeffectivedate,
        s38eligibilityissuedate,
        createtime,
        updatetime,
        documentlinkableid
    from {{ ref('v_ccx_workcapdecision_icare_current') }}
    where retired = 0
),

ccx_wcdreviewdetails_icare as (
    select
        id,
        workcapacitydecisionid,
        status
    from {{ ref('v_ccx_wcdreviewdetails_icare_current') }}
    where retired = 0
),

ccx_wcdinternalreview_icare as (
    select
        id,
        reviewdetailsid,
        internalreviewcompleteddate
    from {{ ref('v_ccx_wcdinternalreview_icare_current') }}
),

ccx_propwcdlistwrapper_icare as (
    select
        id,
        workcapacitydecisionid,
        decision
    from {{ ref('v_ccx_propwcdlistwrapper_icare_current') }}
    where retired = 0
),

ccx_wcdlistwrapper_icare as (
    select
        id,
        workcapacitydecisionid,
        decision
    from {{ ref('v_ccx_wcdlistwrapper_icare_current') }}
    where retired = 0
),

ccx_wcdpeerreview_icare as (
    select
        id,
        workcapacitydecisionid,
        date,
        reviewtype,
        outcome,
        requesterid,
        reviewerid
    from {{ ref('v_ccx_wcdpeerreview_icare_current') }}
    where retired = 0
),

ccx_wcdapplication_icare as (
    select
        id,
        workcapacitydecisionid,
        dateapplicationsent,
        applicationreceived
    from {{ ref('v_ccx_wcdapplication_icare_current') }}
    where retired = 0
),

cc_user as (
    select
        id,
        contactid
    from {{ ref('v_cc_user_current') }}
),

cc_contact as (
    select
        id,
        publicid,
        firstname,
        lastname
    from {{ ref('v_cc_contact_current') }}
    where retired = 0
),

cctl_status_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_status_icare_current') }}
),

cctl_reviewtype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_reviewtype_icare_current') }}
),

cctl_reviewperiod_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_reviewperiod_icare_current') }}
),

cctl_proboutcome_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_proboutcome_icare_current') }}
),

cctl_fitness_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_fitness_icare_current') }}
),

cctl_workcapacity as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_workcapacity_current') }}
),

cctl_wcdlist_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_wcdlist_icare_current') }}
),

cctl_earningse_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_earningse_icare_current') }}
),

cctl_medentitlement_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_medentitlement_icare_current') }}
),

cctl_reviewoutcome_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_reviewoutcome_icare_current') }}
),
cte_join as 
(
select distinct
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as src_system_cd,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    wcd.publicid as decision_id,
    wcd.referencenumber as decision_ref_no,
    cast(wcd.issuedatedecision as date) as decision_dt,
    dim_wcpdstatus.typecode as decision_status_cd,
    dim_wcpdstatus.name as decision_status_desc,
    dim_wcrtype.typecode as review_type_cd,
    dim_wcrtype.name as review_type_desc,
    dim_wcrstatus.typecode as review_status_cd,
    dim_wcrstatus.name as review_status_desc,
    cast(wcd.issuedatedecision as date) as decision_issue_dt,
    cast(wcd.effectivedatedecision as date) as decision_eff_dt,
    cast(wcrinternal.internalreviewcompleteddate as date) as review_completed_dt,
    wcd.relatedto as related_to_decision_ref,
    dim_reviewperiod.typecode as review_period_cd,
    dim_reviewperiod.name as review_period_desc,
    case
        when dim_wcpdstatus.typecode in ('admin_error', 'void') then 'N'
        when rank() over (
            partition by case when dim_wcpdstatus.typecode not in ('admin_error', 'void') then clm.claimnumber end
            order by wcd.referencenumber desc
        ) = 1 then 'Y'
        else 'N'
    end as latest_wcd_ind,
    dim_pre78proboutcome.typecode as pre78_wk_probable_outcome_cd,
    dim_pre78proboutcome.name as pre78_wk_probable_outcome_desc,
    case
        when wcd.fairnoticerequired = 1 then 'Y'
        when wcd.fairnoticerequired = 0 then 'N'
        else null
    end as pre78_fair_notice_req_ind,
    dim_fairnoticewcdlist.typecode as fair_notice_prob_decision_cd,
    dim_fairnoticewcdlist.name as fair_notice_prob_decision_desc,
    case
        when wcd.draftfairnoticeready = 1 then 'Y'
        when wcd.draftfairnoticeready = 0 then 'N'
        else null
    end as draft_fair_notice_review_ind,
    cast(wcd.issuedatefairnotice as date) as fair_notice_commencement_dt,
    cast(wcd.effectivedatefairnotice as date) as fair_notice_completion_dt,
    dim_wcd.typecode as wcd_cd,
    dim_wcd.name as wcd_desc,
    wcd.assessedhours as assessed_hours,
    dim_earnings.typecode as earnings_e_cd,
    dim_earnings.name as earnings_e_desc,
    wcd.earningscapacity as earnings_capacity,
    wcd.wcdpiawe as piawe,
    wcd.wpipercentage as wpi_percentage,
    dim_medentitlementperiod.typecode as med_entitlement_period_cd,
    dim_medentitlementperiod.name as med_entitlement_period_desc,
    dim_wklypaymentimpact.typecode as wkly_payment_impact_cd,
    dim_wklypaymentimpact.name as wkly_payment_impact_desc,
    wcd.decisionreasoning as wcd_reasoning,
    case
        when wcd.draftdecisioncompleted = 1 then 'Y'
        when wcd.draftdecisioncompleted = 0 then 'N'
        else null
    end as draft_decision_completed_ind,
    wcd.furtherinfosubmitted as further_info_submit_by_worker,
    wcd.furtherinfoimpactsdecision as further_info_impacts_decisions,
    wcd.furtherinforeasoning as further_info_reason,
    CAST(wcdapplication.dateapplicationsent AS TIMESTAMP_NTZ) as  s38_app_sent_dttm,
    cast(wcdapplication.dateapplicationsent as date) as s38_app_sent_dt,
    case
        when wcdapplication.applicationreceived = 1 then 'Y'
        when wcdapplication.applicationreceived = 0 then 'N'
        else null
    end as s38_app_received_ind,
    case
        when wcd.working15hourperweek = 1 then 'Y'
        when wcd.working15hourperweek = 0 then 'N'
        else null
    end as working_15hpw_or_more_ind,
    wcd.working15hourperweektext as working_15hpw_or_more_desc,
    case
        when wcd.earningperweek = 1 then 'Y'
        when wcd.earningperweek = 0 then 'N'
        else null
    end as earning_155pw_or_more_ind,
    wcd.earningperweektext as earning_155pw_or_more_desc,
    case
        when wcd.assessedasindefinitelyunable = 1 then 'Y'
        when wcd.assessedasindefinitelyunable = 0 then 'N'
        else null
    end as assessed_indef_unable_ind,
    wcd.assessedasunabletext as assessed_indef_unable_desc,
    case
        when wcd.section38eligible = 1 then 'Y'
        when wcd.section38eligible = 0 then 'N'
        else null
    end as s38_eligibility_ind,
    CAST(wcd.eligibilityeffectivedate as TIMESTAMP_NTZ) as s38_eligibility_issue_dttm,
    cast(wcd.eligibilityeffectivedate as date) as s38_eligibility_issue_dt,
    CAST(peerreview.date AS TIMESTAMP_NTZ) as peer_review_dttm,
    cast(peerreview.date as date) as peer_review_dt,
    dim_peerreviewtype.typecode as peer_review_type_cd,
    dim_peerreviewtype.name as peer_review_type_desc,
    dim_peerreviewoutcome.typecode as peer_review_outcome_cd,
    dim_peerreviewoutcome.name as peer_review_outcome_desc,
    requester_contact.publicid as requester_contact_id,
    concat(requester_contact.firstname, ' ', requester_contact.lastname) as requester_name,
    reviewer_contact.publicid as reviewer_contact_id,
    concat(reviewer_contact.firstname, ' ', reviewer_contact.lastname) as reviewer_name,
    CAST(wcd.createtime as TIMESTAMP_NTZ) as decision_create_dttm,
    cast(wcd.createtime as date) as decision_create_dt,
    CAST(wcd.updatetime as TIMESTAMP_NTZ) as decision_update_dttm,
    cast(wcd.updatetime as date) as decision_update_dt,
    cast(wcd.s38eligibilityissuedate as date) as new_s38_elig_issue_dt,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_claim clm

inner join ccx_workcapdecision_icare wcd
    on clm.id = wcd.claimid

left join cctl_status_icare dim_wcpdstatus
    on wcd.status = dim_wcpdstatus.id

left join cctl_reviewtype_icare dim_wcrtype
    on wcd.reviewtype = dim_wcrtype.id

left join ccx_wcdreviewdetails_icare wcr
    on wcr.workcapacitydecisionid = wcd.id

left join cctl_status_icare dim_wcrstatus
    on wcr.status = dim_wcrstatus.id

left join ccx_wcdinternalreview_icare wcrinternal
    on wcrinternal.reviewdetailsid = wcr.workcapacitydecisionid

left join cctl_reviewperiod_icare dim_reviewperiod
    on wcd.reviewperiod = dim_reviewperiod.id

left join cctl_proboutcome_icare dim_pre78proboutcome
    on wcd.evaluationprobableoutcome = dim_pre78proboutcome.id

left join ccx_propwcdlistwrapper_icare wcdproplistwrapper
    on wcdproplistwrapper.workcapacitydecisionid = wcd.id

left join cctl_wcdlist_icare dim_fairnoticewcdlist
    on wcdproplistwrapper.decision = dim_fairnoticewcdlist.id

left join ccx_wcdlistwrapper_icare wcdlistwrapper
    on wcdlistwrapper.workcapacitydecisionid = wcd.id

left join cctl_wcdlist_icare dim_wcd
    on wcdlistwrapper.decision = dim_wcd.id

left join cctl_earningse_icare dim_earnings
    on wcd.earningse = dim_earnings.id

left join cctl_medentitlement_icare dim_medentitlementperiod
    on wcd.medentitlement = dim_medentitlementperiod.id

left join cctl_proboutcome_icare dim_wklypaymentimpact
    on wcd.weeklypaymentimpact = dim_wklypaymentimpact.id

left join ccx_wcdpeerreview_icare peerreview
    on peerreview.workcapacitydecisionid = wcd.id

left join cctl_reviewtype_icare dim_peerreviewtype
    on peerreview.reviewtype = dim_peerreviewtype.id

left join cctl_reviewoutcome_icare dim_peerreviewoutcome
    on peerreview.outcome = dim_peerreviewoutcome.id

left join cc_user requester
    on peerreview.requesterid = requester.id

left join cc_contact requester_contact
    on requester.contactid = requester_contact.id

left join cc_user reviewer
    on peerreview.reviewerid = reviewer.id

left join cc_contact reviewer_contact
    on reviewer.contactid = reviewer_contact.id

left join ccx_wcdapplication_icare wcdapplication
    on wcdapplication.workcapacitydecisionid = wcd.id
)
select 
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    decision_id,
    decision_ref_no,
    decision_dt,
    decision_status_cd,
    decision_status_desc,
    review_type_cd,
    review_type_desc,
    review_status_cd,
    review_status_desc,
    decision_issue_dt,
    decision_eff_dt,
    review_completed_dt,
    related_to_decision_ref,
    review_period_cd,
    review_period_desc,
    latest_wcd_ind,
    pre78_wk_probable_outcome_cd,
    pre78_wk_probable_outcome_desc,
    pre78_fair_notice_req_ind,
    fair_notice_prob_decision_cd,
    fair_notice_prob_decision_desc,
    draft_fair_notice_review_ind,
    fair_notice_commencement_dt,
    fair_notice_completion_dt,
    wcd_cd,
    wcd_desc,
    assessed_hours,
    earnings_e_cd,
    earnings_e_desc,
    earnings_capacity,
    piawe,
    wpi_percentage,
    med_entitlement_period_cd,
    med_entitlement_period_desc,
    wkly_payment_impact_cd,
    wkly_payment_impact_desc,
    wcd_reasoning,
    draft_decision_completed_ind,
    further_info_submit_by_worker,
    further_info_impacts_decisions,
    further_info_reason,
    s38_app_sent_dttm,
    s38_app_sent_dt,
    s38_app_received_ind,
    working_15hpw_or_more_ind,
    working_15hpw_or_more_desc,
    earning_155pw_or_more_ind,
    earning_155pw_or_more_desc,
    assessed_indef_unable_ind,
    assessed_indef_unable_desc,
    s38_eligibility_ind,
    s38_eligibility_issue_dttm,
    s38_eligibility_issue_dt,
    peer_review_dttm,
    peer_review_dt,
    peer_review_type_cd,
    peer_review_type_desc,
    peer_review_outcome_cd,
    peer_review_outcome_desc,
    requester_contact_id,
    requester_name,
    reviewer_contact_id,
    reviewer_name,
    decision_create_dttm,
    decision_create_dt,
    decision_update_dttm,
    decision_update_dt,
    new_s38_elig_issue_dt,
    file_ingestion_timestamp
from
    cte_join