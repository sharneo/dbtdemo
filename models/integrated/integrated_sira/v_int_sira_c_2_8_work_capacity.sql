{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Work Capacity Decision View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with cc_claim as (
    select
        id,
        claim_sk,
        claimnumber,
        lodgingagent_icare,
        claimsagent_icare
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cctl_status_icare as (
    select
        id
    from {{ ref('v_cctl_status_icare_current') }}
    where retired = 0 and typecode != 'admin_error'
),

ccx_workcapdecision_icare as (
    select
        w.id,
        w.claimid,
        w.issuedatedecision,
        w.referencenumber,
        w.weeklypaymentimpact,
        w.reviewtype,
        w.retired,
        w.createtime,
        w.updatetime
    from {{ ref('v_ccx_workcapdecision_icare_current') }} as w
    inner join cctl_status_icare as st on st.id = w.status
    where w.retired = 0
),

cctl_proboutcome_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_proboutcome_icare_current') }}
    where typecode in ('reduction_partial', 'reduction_to_0', 'no_entitlement')
),

cctl_claimagent_icare_lag as (
    select
        id,
        typecode
    from {{ ref('v_cctl_claimagent_icare_current') }}
),

cctl_claimagent_icare_mag as (
    select
        id,
        typecode
    from {{ ref('v_cctl_claimagent_icare_current') }}
),

cctl_reviewtype_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_reviewtype_icare_current') }}
),

ccx_wcdlistwrapper_icare as (
    select
        workcapacitydecisionid,
        decision
    from {{ ref('v_ccx_wcdlistwrapper_icare_current') }}
    where retired = 0
),

cctl_wcdlist_icare as (
    select
        id
    from {{ ref('v_cctl_wcdlist_icare_current') }}
),

ccx_wcdreviewdetails_icare as (
    select
        id,
        workcapacitydecisionid,
        reviewtype
    from {{ ref('v_ccx_wcdreviewdetails_icare_current') }}
    where retired = 0
),

ccx_wcdinternalreview_icare as (
    select
        id,
        reviewoutcome,
        dateapplicationreceived,
        acknowledgementletterdate,
        decisionissuedate
    from {{ ref('v_ccx_wcdinternalreview_icare_current') }}
    where retired = 0
),

ccx_wcdintrevlistwrap_icare as (
    select
        internalreviewid,
        decision
    from {{ ref('v_ccx_wcdintrevlistwrap_icare_current') }}
    where retired = 0
),

cctl_wcdreviewoutcome_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_wcdreviewoutcome_icare_current') }}
),

ccx_wcdjudicialreview_icare as (
    select
        id,
        reviewoutcome,
        datenotified
    from {{ ref('v_ccx_wcdjudicialreview_icare_current') }}
    where retired = 0
),

cctl_judwirorevoutcome_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_judwirorevoutcome_icare_current') }}
),

ccx_wcdmeritreview_icare as (
    select
        id,
        reviewdetailsid,
        reviewoutcome,
        dateapplicationreceived,
        responsedate,
        meritreviewissuedate
    from {{ ref('v_ccx_wcdmeritreview_icare_current') }}
    where retired = 0
),

ccx_wcdmeritrevlistwrap_icare as (
    select
        meritreviewid,
        decision
    from {{ ref('v_ccx_wcdmeritrevlistwrap_icare_current') }}
    where retired = 0
),

wrapper as (
    select
        wrap.workcapacitydecisionid,
        wcdlist.id
    from ccx_wcdlistwrapper_icare as wrap
    inner join cctl_wcdlist_icare as wcdlist on wcdlist.id = wrap.decision
),

internal_review as (
    select
        ir.id,
        ir.dateapplicationreceived,
        ir.acknowledgementletterdate,
        ir.decisionissuedate,
        outcome.typecode,
        wcdlist.id as rev_decision_id
    from ccx_wcdinternalreview_icare as ir
    inner join ccx_wcdintrevlistwrap_icare as wrap on wrap.internalreviewid = ir.id
    inner join cctl_wcdlist_icare as wcdlist on wcdlist.id = wrap.decision
    left join cctl_wcdreviewoutcome_icare as outcome on ir.reviewoutcome = outcome.id
),

judicial_review as (
    select
        jr.id,
        jr.datenotified,
        outcome.typecode
    from ccx_wcdjudicialreview_icare as jr
    left join cctl_judwirorevoutcome_icare as outcome on jr.reviewoutcome = outcome.id
),

merit_review as (
    select
        mr.id,
        mr.reviewdetailsid,
        mr.dateapplicationreceived,
        mr.responsedate,
        mr.meritreviewissuedate,
        outcome.typecode,
        wcdlist.id as rev_decision_id
    from ccx_wcdmeritreview_icare as mr
    inner join ccx_wcdmeritrevlistwrap_icare as wrap on wrap.meritreviewid = mr.id
    inner join cctl_wcdlist_icare as wcdlist on wcdlist.id = wrap.decision
    left join cctl_wcdreviewoutcome_icare as outcome on mr.reviewoutcome = outcome.id
),

{#- Branch 1: Original Insurer Decision (reviewtype='01', datetype='01') -#}
original_decision as (
    select distinct
        c.claim_sk,
        c.claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        c.claimnumber || '^' || 'GWCC' as claimbk,
        c.claimnumber || coalesce(lag_agent.typecode, '701') as siraclaimnumber,
        coalesce(mag_agent.typecode, lag_agent.typecode) as agentcode,
        w.issuedatedecision as originaldecisiondt,
        w.referencenumber as wcdreferencenumber,
        case
            when wpr.id = 10001 then '01'
            when wpr.id = 10002 then '02'
            when wpr.id = 10003 then '03'
            when wpr.id = 10004 then '04'
            when wpr.id = 10005 then '05'
            when wpr.id = 10006 then '06'
            when wpr.id = 10007 then '07'
        end as wcdtype,
        rt.typecode as wcreviewstage,
        '01' as wcdatetype,
        w.issuedatedecision as wcdactivitydt,
        case when w.issuedatedecision is not null then '11' else '99' end as wcdoutcome,
        w.retired,
        w.createtime,
        w.updatetime
    from ccx_workcapdecision_icare as w
    inner join cc_claim as c on c.id = w.claimid
    inner join cctl_proboutcome_icare as cpi on w.weeklypaymentimpact = cpi.id
    left join cctl_claimagent_icare_lag as lag_agent on c.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on c.claimsagent_icare = mag_agent.id
    inner join cctl_reviewtype_icare as rt on w.reviewtype = rt.id
    inner join wrapper as wpr on wpr.workcapacitydecisionid = w.id
    where rt.typecode = '01'
        and w.issuedatedecision is not null
),

{#- Branch 2: Internal Review - Date Received (reviewtype='02', datetype='02') -#}
internal_review_date_received as (
    select distinct
        c.claim_sk,
        c.claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        c.claimnumber || '^' || 'GWCC' as claimbk,
        c.claimnumber || coalesce(lag_agent.typecode, '701') as siraclaimnumber,
        coalesce(mag_agent.typecode, lag_agent.typecode) as agentcode,
        w.issuedatedecision as originaldecisiondt,
        w.referencenumber as wcdreferencenumber,
        case
            when ir.rev_decision_id = 10001 then '01'
            when ir.rev_decision_id = 10002 then '02'
            when ir.rev_decision_id = 10003 then '03'
            when ir.rev_decision_id = 10004 then '04'
            when ir.rev_decision_id = 10005 then '05'
            when ir.rev_decision_id = 10006 then '06'
            when ir.rev_decision_id = 10007 then '07'
        end as wcdtype,
        rt2.typecode as wcreviewstage,
        '02' as wcdatetype,
        ir.dateapplicationreceived as wcdactivitydt,
        case
            when ir.typecode is not null and ir.dateapplicationreceived >= ir.decisionissuedate then ir.typecode
            when ir.typecode is not null and ir.dateapplicationreceived < ir.decisionissuedate then '01'
            else '01'
        end as wcdoutcome,
        w.retired,
        w.createtime,
        w.updatetime
    from ccx_workcapdecision_icare as w
    inner join cc_claim as c on c.id = w.claimid
    inner join cctl_proboutcome_icare as cpi on w.weeklypaymentimpact = cpi.id
    left join cctl_claimagent_icare_lag as lag_agent on c.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on c.claimsagent_icare = mag_agent.id
    inner join ccx_wcdreviewdetails_icare as wi on w.id = wi.workcapacitydecisionid
    inner join cctl_reviewtype_icare as rt2 on wi.reviewtype = rt2.id
    left join internal_review as ir on ir.id = w.id
    where rt2.typecode = '02'
        and ir.dateapplicationreceived is not null
        and w.issuedatedecision is not null
),

{#- Branch 3: Internal Review - Acknowledgement (reviewtype='02', datetype='05') -#}
internal_review_acknowledgement as (
    select distinct
        c.claim_sk,
        c.claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        c.claimnumber || '^' || 'GWCC' as claimbk,
        c.claimnumber || coalesce(lag_agent.typecode, '701') as siraclaimnumber,
        coalesce(mag_agent.typecode, lag_agent.typecode) as agentcode,
        w.issuedatedecision as originaldecisiondt,
        w.referencenumber as wcdreferencenumber,
        case
            when ir.rev_decision_id = 10001 then '01'
            when ir.rev_decision_id = 10002 then '02'
            when ir.rev_decision_id = 10003 then '03'
            when ir.rev_decision_id = 10004 then '04'
            when ir.rev_decision_id = 10005 then '05'
            when ir.rev_decision_id = 10006 then '06'
            when ir.rev_decision_id = 10007 then '07'
        end as wcdtype,
        rt2.typecode as wcreviewstage,
        '05' as wcdatetype,
        ir.acknowledgementletterdate as wcdactivitydt,
        case
            when ir.typecode is not null and ir.acknowledgementletterdate >= ir.decisionissuedate then ir.typecode
            when ir.typecode is not null and ir.acknowledgementletterdate < ir.decisionissuedate then '01'
            else '01'
        end as wcdoutcome,
        w.retired,
        w.createtime,
        w.updatetime
    from ccx_workcapdecision_icare as w
    inner join cc_claim as c on c.id = w.claimid
    inner join cctl_proboutcome_icare as cpi on w.weeklypaymentimpact = cpi.id
    left join cctl_claimagent_icare_lag as lag_agent on c.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on c.claimsagent_icare = mag_agent.id
    inner join ccx_wcdreviewdetails_icare as wi on w.id = wi.workcapacitydecisionid
    inner join cctl_reviewtype_icare as rt2 on wi.reviewtype = rt2.id
    left join internal_review as ir on ir.id = w.id
    where rt2.typecode = '02'
        and ir.acknowledgementletterdate is not null
        and w.issuedatedecision is not null
),

{#- Branch 4: Internal Review - Decision Outcome (reviewtype='02', datetype='01') -#}
internal_review_outcome as (
    select distinct
        c.claim_sk,
        c.claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        c.claimnumber || '^' || 'GWCC' as claimbk,
        c.claimnumber || coalesce(lag_agent.typecode, '701') as siraclaimnumber,
        coalesce(mag_agent.typecode, lag_agent.typecode) as agentcode,
        w.issuedatedecision as originaldecisiondt,
        w.referencenumber as wcdreferencenumber,
        case
            when ir.rev_decision_id = 10001 then '01'
            when ir.rev_decision_id = 10002 then '02'
            when ir.rev_decision_id = 10003 then '03'
            when ir.rev_decision_id = 10004 then '04'
            when ir.rev_decision_id = 10005 then '05'
            when ir.rev_decision_id = 10006 then '06'
            when ir.rev_decision_id = 10007 then '07'
        end as wcdtype,
        rt2.typecode as wcreviewstage,
        '01' as wcdatetype,
        ir.decisionissuedate as wcdactivitydt,
        case when ir.typecode is not null then ir.typecode else '01' end as wcdoutcome,
        w.retired,
        w.createtime,
        w.updatetime
    from ccx_workcapdecision_icare as w
    inner join cc_claim as c on c.id = w.claimid
    inner join cctl_proboutcome_icare as cpi on w.weeklypaymentimpact = cpi.id
    left join cctl_claimagent_icare_lag as lag_agent on c.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on c.claimsagent_icare = mag_agent.id
    inner join ccx_wcdreviewdetails_icare as wi on w.id = wi.workcapacitydecisionid
    inner join cctl_reviewtype_icare as rt2 on wi.reviewtype = rt2.id
    left join internal_review as ir on ir.id = w.id
    where rt2.typecode = '02'
        and ir.decisionissuedate is not null
        and w.issuedatedecision is not null
),

{#- Branch 5: Judicial Review - Date Notified (reviewtype='04', datetype='04') -#}
judicial_review_notified as (
    select distinct
        c.claim_sk,
        c.claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        c.claimnumber || '^' || 'GWCC' as claimbk,
        c.claimnumber || coalesce(lag_agent.typecode, '701') as siraclaimnumber,
        coalesce(mag_agent.typecode, lag_agent.typecode) as agentcode,
        w.issuedatedecision as originaldecisiondt,
        w.referencenumber as wcdreferencenumber,
        case
            when wpr.id = 10001 then '01'
            when wpr.id = 10002 then '02'
            when wpr.id = 10003 then '03'
            when wpr.id = 10004 then '04'
            when wpr.id = 10005 then '05'
            when wpr.id = 10006 then '06'
            when wpr.id = 10007 then '07'
        end as wcdtype,
        rt2.typecode as wcreviewstage,
        '04' as wcdatetype,
        jr.datenotified as wcdactivitydt,
        case when jr.typecode is not null and upper(jr.typecode) = 'VALID' then '11' else '99' end as wcdoutcome,
        w.retired,
        w.createtime,
        w.updatetime
    from ccx_workcapdecision_icare as w
    inner join cc_claim as c on c.id = w.claimid
    inner join cctl_proboutcome_icare as cpi on w.weeklypaymentimpact = cpi.id
    left join cctl_claimagent_icare_lag as lag_agent on c.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on c.claimsagent_icare = mag_agent.id
    inner join wrapper as wpr on wpr.workcapacitydecisionid = w.id
    inner join ccx_wcdreviewdetails_icare as wi on w.id = wi.workcapacitydecisionid
    inner join cctl_reviewtype_icare as rt2 on wi.reviewtype = rt2.id
    left join judicial_review as jr on jr.id = w.id
    where rt2.typecode = '04'
        and jr.datenotified is not null
        and w.issuedatedecision is not null
),

{#- Branch 6: Judicial Review - Decision Outcome (reviewtype='04', datetype='01') -#}
judicial_review_outcome as (
    select distinct
        c.claim_sk,
        c.claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        c.claimnumber || '^' || 'GWCC' as claimbk,
        c.claimnumber || coalesce(lag_agent.typecode, '701') as siraclaimnumber,
        coalesce(mag_agent.typecode, lag_agent.typecode) as agentcode,
        w.issuedatedecision as originaldecisiondt,
        w.referencenumber as wcdreferencenumber,
        case
            when wpr.id = 10001 then '01'
            when wpr.id = 10002 then '02'
            when wpr.id = 10003 then '03'
            when wpr.id = 10004 then '04'
            when wpr.id = 10005 then '05'
            when wpr.id = 10006 then '06'
            when wpr.id = 10007 then '07'
        end as wcdtype,
        rt2.typecode as wcreviewstage,
        '01' as wcdatetype,
        w.issuedatedecision as wcdactivitydt,
        case when jr.typecode is not null and upper(jr.typecode) = 'VALID' then '11' else '99' end as wcdoutcome,
        w.retired,
        w.createtime,
        w.updatetime
    from ccx_workcapdecision_icare as w
    inner join cc_claim as c on c.id = w.claimid
    inner join cctl_proboutcome_icare as cpi on w.weeklypaymentimpact = cpi.id
    left join cctl_claimagent_icare_lag as lag_agent on c.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on c.claimsagent_icare = mag_agent.id
    inner join wrapper as wpr on wpr.workcapacitydecisionid = w.id
    inner join ccx_wcdreviewdetails_icare as wi on w.id = wi.workcapacitydecisionid
    inner join cctl_reviewtype_icare as rt2 on wi.reviewtype = rt2.id
    left join judicial_review as jr on jr.id = w.id
    where rt2.typecode = '04'
        and w.issuedatedecision is not null
),

{#- Branch 7: Merit Review - Date Received (reviewtype='03', datetype='03') -#}
merit_review_received as (
    select distinct
        c.claim_sk,
        c.claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        c.claimnumber || '^' || 'GWCC' as claimbk,
        c.claimnumber || coalesce(lag_agent.typecode, '701') as siraclaimnumber,
        coalesce(mag_agent.typecode, lag_agent.typecode) as agentcode,
        w.issuedatedecision as originaldecisiondt,
        w.referencenumber as wcdreferencenumber,
        case
            when wpr.id = 10001 then '01'
            when wpr.id = 10002 then '02'
            when wpr.id = 10003 then '03'
            when wpr.id = 10004 then '04'
            when wpr.id = 10005 then '05'
            when wpr.id = 10006 then '06'
            when wpr.id = 10007 then '07'
        end as wcdtype,
        rt2.typecode as wcreviewstage,
        '03' as wcdatetype,
        mr.dateapplicationreceived as wcdactivitydt,
        case
            when mr.typecode is not null and mr.dateapplicationreceived >= mr.meritreviewissuedate then mr.typecode
            when mr.typecode is not null and mr.dateapplicationreceived < mr.meritreviewissuedate then '01'
            else '01'
        end as wcdoutcome,
        w.retired,
        w.createtime,
        w.updatetime
    from ccx_workcapdecision_icare as w
    inner join cc_claim as c on c.id = w.claimid
    inner join cctl_proboutcome_icare as cpi on w.weeklypaymentimpact = cpi.id
    left join cctl_claimagent_icare_lag as lag_agent on c.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on c.claimsagent_icare = mag_agent.id
    inner join ccx_wcdreviewdetails_icare as wi on w.id = wi.workcapacitydecisionid
    inner join cctl_reviewtype_icare as rt2 on wi.reviewtype = rt2.id
    inner join ccx_wcdmeritreview_icare as wcdmerit on wcdmerit.reviewdetailsid = wi.id
    inner join (
        select wrap.meritreviewid, wcdlist.id
        from ccx_wcdmeritrevlistwrap_icare as wrap
        inner join cctl_wcdlist_icare as wcdlist on wcdlist.id = wrap.decision
    ) as wpr on wpr.meritreviewid = wcdmerit.id
    left join merit_review as mr on mr.id = w.id
    where rt2.typecode = '03'
        and mr.dateapplicationreceived is not null
        and w.issuedatedecision is not null
),

{#- Branch 8: Merit Review - Response Date (reviewtype='03', datetype='06') -#}
merit_review_response as (
    select distinct
        c.claim_sk,
        c.claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        c.claimnumber || '^' || 'GWCC' as claimbk,
        c.claimnumber || coalesce(lag_agent.typecode, '701') as siraclaimnumber,
        coalesce(mag_agent.typecode, lag_agent.typecode) as agentcode,
        w.issuedatedecision as originaldecisiondt,
        w.referencenumber as wcdreferencenumber,
        case
            when wpr.id = 10001 then '01'
            when wpr.id = 10002 then '02'
            when wpr.id = 10003 then '03'
            when wpr.id = 10004 then '04'
            when wpr.id = 10005 then '05'
            when wpr.id = 10006 then '06'
            when wpr.id = 10007 then '07'
        end as wcdtype,
        rt2.typecode as wcreviewstage,
        '06' as wcdatetype,
        mr.responsedate as wcdactivitydt,
        case
            when mr.typecode is not null and mr.responsedate >= mr.meritreviewissuedate then mr.typecode
            when mr.typecode is not null and mr.responsedate < mr.meritreviewissuedate then '01'
            else '01'
        end as wcdoutcome,
        w.retired,
        w.createtime,
        w.updatetime
    from ccx_workcapdecision_icare as w
    inner join cc_claim as c on c.id = w.claimid
    inner join cctl_proboutcome_icare as cpi on w.weeklypaymentimpact = cpi.id
    left join cctl_claimagent_icare_lag as lag_agent on c.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on c.claimsagent_icare = mag_agent.id
    inner join ccx_wcdreviewdetails_icare as wi on w.id = wi.workcapacitydecisionid
    inner join cctl_reviewtype_icare as rt2 on wi.reviewtype = rt2.id
    inner join ccx_wcdmeritreview_icare as wcdmerit on wcdmerit.reviewdetailsid = wi.id
    inner join (
        select wrap.meritreviewid, wcdlist.id
        from ccx_wcdmeritrevlistwrap_icare as wrap
        inner join cctl_wcdlist_icare as wcdlist on wcdlist.id = wrap.decision
    ) as wpr on wpr.meritreviewid = wcdmerit.id
    left join merit_review as mr on mr.id = w.id
    where rt2.typecode = '03'
        and mr.responsedate is not null
        and w.issuedatedecision is not null
),

{#- Branch 9: Merit Review - Decision Outcome (reviewtype='03', datetype='01') -#}
merit_review_outcome as (
    select distinct
        c.claim_sk,
        c.claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        c.claimnumber || '^' || 'GWCC' as claimbk,
        c.claimnumber || coalesce(lag_agent.typecode, '701') as siraclaimnumber,
        coalesce(mag_agent.typecode, lag_agent.typecode) as agentcode,
        w.issuedatedecision as originaldecisiondt,
        w.referencenumber as wcdreferencenumber,
        case
            when wpr.id = 10001 then '01'
            when wpr.id = 10002 then '02'
            when wpr.id = 10003 then '03'
            when wpr.id = 10004 then '04'
            when wpr.id = 10005 then '05'
            when wpr.id = 10006 then '06'
            when wpr.id = 10007 then '07'
        end as wcdtype,
        rt2.typecode as wcreviewstage,
        '01' as wcdatetype,
        mr.meritreviewissuedate as wcdactivitydt,
        case when mr.typecode is not null then mr.typecode else '01' end as wcdoutcome,
        w.retired,
        w.createtime,
        w.updatetime
    from ccx_workcapdecision_icare as w
    inner join cc_claim as c on c.id = w.claimid
    inner join cctl_proboutcome_icare as cpi on w.weeklypaymentimpact = cpi.id
    left join cctl_claimagent_icare_lag as lag_agent on c.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on c.claimsagent_icare = mag_agent.id
    inner join ccx_wcdreviewdetails_icare as wi on w.id = wi.workcapacitydecisionid
    inner join cctl_reviewtype_icare as rt2 on wi.reviewtype = rt2.id
    inner join ccx_wcdmeritreview_icare as wcdmerit on wcdmerit.reviewdetailsid = wi.id
    inner join (
        select wrap.meritreviewid, wcdlist.id
        from ccx_wcdmeritrevlistwrap_icare as wrap
        inner join cctl_wcdlist_icare as wcdlist on wcdlist.id = wrap.decision
    ) as wpr on wpr.meritreviewid = wcdmerit.id
    left join merit_review as mr on mr.id = w.id
    where rt2.typecode = '03'
        and mr.meritreviewissuedate is not null
        and w.issuedatedecision is not null
),

base as (
    select * from original_decision
    union
    select * from internal_review_date_received
    union
    select * from internal_review_acknowledgement
    union
    select * from internal_review_outcome
    union
    select * from judicial_review_notified
    union
    select * from judicial_review_outcome
    union
    select * from merit_review_received
    union
    select * from merit_review_response
    union
    select * from merit_review_outcome
)

select
    cast(
        ltrim(rtrim(cast(siraclaimnumber as varchar(100)))) || '^' ||
        ltrim(rtrim(to_char(originaldecisiondt, 'YYYYMMDD'))) || '^' ||
        ltrim(rtrim(cast(wcdreferencenumber as varchar(100)))) || '^' ||
        ltrim(rtrim(cast(wcdtype as varchar(100)))) || '^' ||
        ltrim(rtrim(cast(wcreviewstage as varchar(100)))) || '^' ||
        ltrim(rtrim(cast(wcdatetype as varchar(100))))
    as varchar(100)) as wc_hlp_key,
    cast(claim_sk as varchar(40)) as claim_sk,
    cast(claimnumber as varchar(100)) as claimnumber,
    cast(srcsystemcd as varchar(10)) as srcsystemcd,
    cast(claimbk as varchar(1000)) as claimbk,
    cast(siraclaimnumber as varchar(1000)) as siraclaimnumber,
    cast(agentcode as varchar(100)) as agentcode,
    originaldecisiondt,
    cast(wcdreferencenumber as varchar(100)) as wcdreferencenumber,
    cast(wcdtype as varchar(100)) as wcdtype,
    cast(wcreviewstage as varchar(100)) as wcreviewstage,
    cast(wcdatetype as varchar(100)) as wcdatetype,
    wcdactivitydt,
    cast(wcdoutcome as varchar(100)) as wcdoutcome,
    cast(retired as bigint) as retired,
    createtime,
    updatetime
from base
