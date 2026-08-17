{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Claim Screening View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with ccx_claimscreening_icare as (
    select
        id,
        claimid,
        claimscreeningdate,
        claimscreenactioncode
    from {{ ref('v_ccx_claimscreening_icare_current') }}
    where retired = 0
        and claimscreeningdate is not null
),

cctl_claimscreenaction_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_claimscreenaction_icare_current') }}
    where retired = 0
),

submission_period as (
    select
        submission_period,
        submission_period_end_dt,
        submission_period_start_dt
    from {{ ref('v_sira_submission_period_current') }}
    where current_submission_flag = 'Y'
),

base as (
    select
        subperiod.submission_period,
        ci.claimid,
        ci.claimscreeningdate,
        ci.claimscreenactioncode,
        csa.typecode,
        csa.name,
        row_number() over (
            partition by ci.claimid, subperiod.submission_period
            order by ci.claimscreeningdate desc, ci.id desc
        ) as row_num
    from ccx_claimscreening_icare as ci
    inner join cctl_claimscreenaction_icare as csa on csa.id = ci.claimscreenactioncode
    inner join submission_period as subperiod
        on ci.claimscreeningdate >= subperiod.submission_period_start_dt
        and ci.claimscreeningdate <= subperiod.submission_period_end_dt
)

select
    submission_period,
    claimid,
    claimscreeningdate,
    claimscreenactioncode,
    typecode,
    name,
    row_num
from base
