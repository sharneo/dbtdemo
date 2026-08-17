{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Liability Status Current View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with submission_period as (
    select
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
        claimworkcompid
    from {{ ref('v_cc_claim_current') }}
    where left(claimnumber, 1) = '2'
),

cc_claim_series1 as (
    select distinct
        claimworkcompid
    from {{ ref('v_cc_claim_current') }}
    where left(claimnumber, 1) = '1'
),

series2_status as (
    select
        cast(ls.liabilitystatusdecisiondate as date) as liabilitystatusday,
        ls.liabilitystatusdecisiondate as liabilitystatusdate,
        ls.liabilitystatus,
        ls.claimworkcompid,
        ls.id,
        rank() over (
            partition by ls.claimworkcompid
            order by ls.liabilitystatusdecisiondate desc, ls.id desc
        ) as ranking,
        ld.typecode,
        left(to_char(coalesce(ls.liabilitystatusdecisiondate, '9999-12-31'), 'YYYYMMDD'), 6) as liabilitystatusdateint
    from ccx_liabilitystatushist_adjusted as ls
    inner join cctl_compensabilitydecision as ld on ld.id = ls.liabilitystatus
    inner join cc_claim_series2 as c on c.claimworkcompid = ls.claimworkcompid
    inner join submission_period as sp on ls.liabilitystatusdecisiondate <= sp.submission_period_end_dt
    where ld.typecode in ('01', '02', '05', '06', '07', '08', '09', '10', '11', '12')
),

series1_status as (
    select
        cast(ls.liabilitystatusdate_adjusted as date) as liabilitystatusday,
        ls.liabilitystatusdate_adjusted as liabilitystatusdate,
        ls.liabilitystatus,
        ls.claimworkcompid,
        ls.id,
        rank() over (
            partition by ls.claimworkcompid
            order by ls.liabilitystatusdate_adjusted desc, ls.id desc
        ) as ranking,
        ld.typecode,
        left(to_char(coalesce(ls.liabilitystatusdate_adjusted, '9999-12-31'), 'YYYYMMDD'), 6) as liabilitystatusdateint
    from ccx_liabilitystatushist_adjusted as ls
    inner join cctl_compensabilitydecision as ld on ld.id = ls.liabilitystatus
    inner join cc_claim_series1 as c on c.claimworkcompid = ls.claimworkcompid
    inner join submission_period as sp on ls.liabilitystatusdate_adjusted <= sp.submission_period_end_dt
    where ld.typecode in ('01', '02', '05', '06', '07', '08', '09', '10', '11', '12')
),

base as (
    select * from series2_status
    union all
    select * from series1_status
)

select
    liabilitystatusday,
    liabilitystatusdate,
    liabilitystatus,
    claimworkcompid,
    id,
    ranking,
    typecode,
    liabilitystatusdateint
from base
