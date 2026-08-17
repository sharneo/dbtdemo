{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a WPI View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with cc_exposure as (
    select
        id,
        claimid
    from {{ ref('v_cc_exposure_current') }}
    where retired = 0
),

ccx_wpiassessment_icare as (
    select
        id,
        exposureid,
        estpermimpairment_icare,
        createtime
    from {{ ref('v_ccx_wpiassessment_icare_current') }}
    where retired = 0
),

ccx_wpiassessrecord_icare as (
    select
        id,
        wpiassessment_icareid,
        settlementtype_icare,
        actiontype_icare,
        letterofofferdate_icare,
        createtime,
        updatetime
    from {{ ref('v_ccx_wpiassessrecord_icare_current') }}
    where retired = 0
),

cctl_actiontype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_actiontype_icare_current') }}
),

cctl_wpisettlementtype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_wpisettlementtype_icare_current') }}
),

cctl_estpermimpairment_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_estpermimpairment_icare_current') }}
),

cc_claim as (
    select
        id,
        claimnumber
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

submission_period as (
    select
        submission_period_end_dt
    from {{ ref('v_sira_submission_period_current') }}
    where current_submission_flag = 'Y'
),

base as (
    select
        e.claimid,
        wst.typecode,
        wst.name,
        est.typecode as estpermimpairment_icare,
        assrec.letterofofferdate_icare,
        assrec.actiontype_icare,
        ai.typecode as actiontypenamefixed,
        ai.name as actiontypename,
        assrec.settlementtype_icare,
        c.claimnumber,
        row_number() over (
            partition by e.claimid
            order by assrec.createtime desc, assrec.id desc, ass.createtime desc, assrec.updatetime desc, e.id desc
        ) as rownum
    from cc_exposure as e
    inner join cc_claim as c on c.id = e.claimid
    left join ccx_wpiassessment_icare as ass on ass.exposureid = e.id
    left join ccx_wpiassessrecord_icare as assrec on assrec.wpiassessment_icareid = ass.id
    left join cctl_actiontype_icare as ai on ai.id = assrec.actiontype_icare
    left join cctl_wpisettlementtype_icare as wst on wst.id = assrec.settlementtype_icare
    left join cctl_estpermimpairment_icare as est on est.id = ass.estpermimpairment_icare
    where coalesce(assrec.createtime, ass.createtime) <= (select submission_period_end_dt from submission_period)
)

select
    claimid,
    typecode,
    name,
    estpermimpairment_icare,
    letterofofferdate_icare,
    actiontype_icare,
    actiontypenamefixed,
    actiontypename,
    settlementtype_icare,
    claimnumber,
    rownum
from base
