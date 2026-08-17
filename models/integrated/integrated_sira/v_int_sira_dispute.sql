{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Dispute View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with cc_claim as (
    select
        id,
        claimnumber,
        claimworkcompid
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

ccx_liabilitystatushist_icare as (
    select
        id,
        claimworkcompid,
        ctmliabilitystatusdecisiondate
    from {{ ref('v_ccx_liabilitystatushist_icare_current') }}
    where retired = 0
),

ccx_disreasonwrapper_icare as (
    select
        liabilitystatusid,
        disputereason
    from {{ ref('v_ccx_disreasonwrapper_icare_current') }}
    where retired = 0
),

cctl_disputereasons_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_disputereasons_icare_current') }}
),

submission_period as (
    select
        submission_period,
        submission_period_end_dt
    from {{ ref('v_sira_submission_period_current') }}
    where current_submission_flag = 'Y'
),

base as (
    select
        subperiod.submission_period,
        dp.typecode,
        c.id as claimid,
        c.claimnumber,
        lsh.ctmliabilitystatusdecisiondate
    from cc_claim as c
    inner join ccx_liabilitystatushist_icare as lsh on c.claimworkcompid = lsh.claimworkcompid
    inner join ccx_disreasonwrapper_icare as dw on dw.liabilitystatusid = lsh.id
    inner join cctl_disputereasons_icare as dp on dp.id = dw.disputereason
    inner join submission_period as subperiod
        on left(to_char(lsh.ctmliabilitystatusdecisiondate, 'YYYYMMDD'), 6) = subperiod.submission_period
)

select
    submission_period,
    claimid,
    claimnumber,
    case when count(*) > 1 then '17' else max(typecode) end as typecode,
    max(cast(ctmliabilitystatusdecisiondate as date)) as disputedate
from base
group by
    submission_period,
    claimid,
    claimnumber
