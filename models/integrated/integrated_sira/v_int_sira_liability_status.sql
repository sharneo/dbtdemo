{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Liability Status View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with submission_period as (
    select
        submission_period,
        submission_period_end_dt
    from {{ ref('v_sira_submission_period_current') }}
    where current_submission_flag = 'Y'
),

cctl_compensabilitydecision as (
    select
        id,
        typecode
    from {{ ref('v_cctl_compensabilitydecision_current') }}
    where retired = 0
),

ccx_liabilitystatushist_adjusted as (
    select
        ls.id,
        ls.claimworkcompid,
        ls.liabilitystatus,
        ls.liabilitystatusdate,
        case
            when ld.typecode = '06'
                then dateadd(day, 1, ls.liabilitystatusdecisiondate)
            else ls.liabilitystatusdecisiondate
        end as liabilitystatusdecisiondate,
        case
            when ld.typecode = '06'
                then dateadd(day, 1, ls.liabilitystatusdate)
            else ls.liabilitystatusdate
        end as liabilitystatusdate_adjusted
    from {{ ref('v_ccx_liabilitystatushist_icare_current') }} as ls
    left join cctl_compensabilitydecision as ld on ld.id = ls.liabilitystatus
    where ls.retired = 0
        and ls.liabilitystatus is not null
),

cc_claim_series2 as (
    select distinct
        claimworkcompid,
        claimnumber
    from {{ ref('v_cc_claim_current') }}
    where left(claimnumber, 1) != '1'
),

cc_claim_series1 as (
    select distinct
        claimworkcompid,
        claimnumber
    from {{ ref('v_cc_claim_current') }}
    where left(claimnumber, 1) = '1'
),

series2_status as (
    select
        subperiod.submission_period,
        cast(ls.liabilitystatusdecisiondate as date) as liabilitystatusday,
        ls.liabilitystatusdecisiondate as liabilitystatusdate,
        ls.liabilitystatus,
        ls.claimworkcompid,
        ls.id,
        rank() over (
            partition by ls.claimworkcompid,
                cast(ls.liabilitystatusdecisiondate as date),
                left(to_char(coalesce(ls.liabilitystatusdecisiondate, '9999-12-31'), 'YYYYMMDD'), 6)
            order by ls.liabilitystatusdecisiondate desc, ls.id desc
        ) as ranking,
        ld.typecode,
        left(to_char(coalesce(ls.liabilitystatusdecisiondate, '9999-12-31'), 'YYYYMMDD'), 6) as liabilitystatusdateint,
        c.claimnumber
    from ccx_liabilitystatushist_adjusted as ls
    inner join cctl_compensabilitydecision as ld on ld.id = ls.liabilitystatus
    inner join cc_claim_series2 as c on c.claimworkcompid = ls.claimworkcompid
    inner join submission_period as subperiod
        on left(to_char(ls.liabilitystatusdecisiondate, 'YYYYMMDD'), 6) = subperiod.submission_period
    where ld.typecode in ('01', '02', '05', '06', '07', '08', '09', '10', '11', '12')
),

series1_status as (
    select
        subperiod.submission_period,
        cast(ls.liabilitystatusdate_adjusted as date) as liabilitystatusday,
        ls.liabilitystatusdate_adjusted as liabilitystatusdate,
        ls.liabilitystatus,
        ls.claimworkcompid,
        ls.id,
        rank() over (
            partition by c.claimworkcompid,
                cast(ls.liabilitystatusdate_adjusted as date),
                left(to_char(coalesce(ls.liabilitystatusdate_adjusted, '9999-12-31'), 'YYYYMMDD'), 6)
            order by ls.liabilitystatusdate_adjusted desc, ls.id desc
        ) as ranking,
        ld.typecode,
        left(to_char(coalesce(ls.liabilitystatusdate_adjusted, '9999-12-31'), 'YYYYMMDD'), 6) as liabilitystatusdateint,
        c.claimnumber
    from ccx_liabilitystatushist_adjusted as ls
    inner join cctl_compensabilitydecision as ld on ld.id = ls.liabilitystatus
    inner join cc_claim_series1 as c on c.claimworkcompid = ls.claimworkcompid
    inner join submission_period as subperiod
        on left(to_char(ls.liabilitystatusdate_adjusted, 'YYYYMMDD'), 6) = subperiod.submission_period
    where ld.typecode in ('01', '02', '05', '06', '07', '08', '09', '10', '11', '12')
),

base as (
    select * from series2_status
    union all
    select * from series1_status
)

select
    submission_period,
    liabilitystatusday,
    liabilitystatusdate,
    liabilitystatus,
    claimworkcompid,
    id,
    ranking,
    typecode,
    liabilitystatusdateint,
    claimnumber
from base
