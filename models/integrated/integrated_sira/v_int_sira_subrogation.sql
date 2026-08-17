{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Subrogation View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with cc_subrogationsummary as (
    select
        id,
        claimid
    from {{ ref('v_cc_subrogationsummary_current') }}
    where retired = 0
),

cc_subrogation as (
    select
        subrogationsummaryid,
        status,
        outcome
    from {{ ref('v_cc_subrogation_current') }}
    where retired = 0
),

cc_subroadverseparty as (
    select
        subrogationsummaryid,
        subrogationstatus_icare,
        fault
    from {{ ref('v_cc_subroadverseparty_current') }}
    where retired = 0
),

cctl_subrogationstatus_ssts as (
    select
        id,
        typecode
    from {{ ref('v_cctl_subrogationstatus_current') }}
    where typecode != 'review'
),

cctl_subrogationstatus_sst as (
    select
        id,
        typecode
    from {{ ref('v_cctl_subrogationstatus_current') }}
    where typecode != 'review'
),

cctl_subroclosedoutcome as (
    select
        id,
        typecode
    from {{ ref('v_cctl_subroclosedoutcome_current') }}
),

cc_claim as (
    select
        id
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

base as (
    select
        sbs.claimid,
        s.outcome,
        outc.typecode as outcome_typecode,
        sp.fault,
        sp.subrogationstatus_icare
    from cc_subrogationsummary as sbs
    inner join cc_subrogation as s on s.subrogationsummaryid = sbs.id
    left join cc_subroadverseparty as sp on sp.subrogationsummaryid = sbs.id
    left join cctl_subrogationstatus_ssts as ssts on ssts.id = s.status
    left join cctl_subrogationstatus_sst as sst on sst.id = sp.subrogationstatus_icare
    left join cctl_subroclosedoutcome as outc on outc.id = s.outcome
    inner join cc_claim as c on c.id = sbs.claimid
    where ssts.id is not null
        and sst.id is not null
)

select
    claimid,
    min(outcome_typecode) as outcome_icare,
    left(max(outcome_typecode), 2) as recovery_indicator,
    sum(fault) as fault
from base
group by
    claimid,
    outcome_typecode
