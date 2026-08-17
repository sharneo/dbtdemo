{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Liability Status CON View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with submission_period_current as (
    select
        submission_period,
        submission_period_end_dt,
        submission_period_start_dt
    from {{ ref('v_sira_submission_period_current') }}
    where current_submission_flag = 'Y'
),

submission_period_all as (
    select
        submission_period
    from {{ ref('v_sira_submission_period_current') }}
),

insurer_control as (
    select
        insurer_number
    from {{ ref('v_insurer_control_current') }}
    where active_ind = 'Y'
),

cctl_compensabilitydecision as (
    select
        id,
        typecode
    from {{ ref('v_cctl_compensabilitydecision_current') }}
    where retired = 0
),

cctl_claimagent_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_claimagent_icare_current') }}
),

cctl_claimreopenedreason as (
    select
        id,
        typecode
    from {{ ref('v_cctl_claimreopenedreason_current') }}
),

cc_claim as (
    select
        id,
        claim_sk,
        claimnumber,
        claimworkcompid,
        state,
        closedate_icare,
        reopendate,
        reopenedreason,
        lodgingagent_icare
    from {{ ref('v_cc_claim_current') }}
),

ccr as (
    select
        c.claim_sk,
        c.claimnumber,
        c.claimworkcompid,
        c.claimnumber || coalesce(ag.typecode, '701') as c_2_2_2_claim_number,
        sp.submission_period_end_dt
    from cc_claim as c
    inner join submission_period_current as sp on 1 = 1
    left join cctl_claimagent_icare as ag on ag.id = c.lodgingagent_icare
    left join insurer_control as ic on ic.insurer_number = ag.typecode
),

ccls as (
    select
        claimnumber,
        max(closedate_icare) as c_2_2_6_date_claim_closed
    from cc_claim as c
    inner join submission_period_current as sp
        on c.closedate_icare <= sp.submission_period_end_dt
    where c.closedate_icare is not null
    group by claimnumber
),

cro as (
    select
        c.claimnumber,
        c.reopendate as c_2_2_7_date_claim_re_opened,
        coalesce(cor.typecode, '0') as c_2_2_8_reason_for_re_opening_claim_code
    from cc_claim as c
    inner join (
        select
            claimnumber,
            max(reopendate) as max_reopendate
        from cc_claim
        where reopendate is not null
        group by claimnumber
    ) as latest on latest.claimnumber = c.claimnumber and latest.max_reopendate = c.reopendate
    left join cctl_claimreopenedreason as cor on cor.id = c.reopenedreason
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
        ld.typecode
    from {{ ref('v_ccx_liabilitystatushist_icare_current') }} as ls
    left join cctl_compensabilitydecision as ld on ld.id = ls.liabilitystatus
    where ls.retired = 0
        and ls.liabilitystatus is not null
        and ld.typecode in ('01', '02', '05', '06', '07', '08', '09', '10', '11', '12')
),

ls_with_period as (
    select
        ls.id,
        ls.claimworkcompid,
        ls.liabilitystatus,
        ls.liabilitystatusdecisiondate,
        ls.typecode,
        sp_all.submission_period,
        left(to_char(coalesce(ls.liabilitystatusdecisiondate, '9999-12-31'), 'YYYYMMDD'), 6) as liabilitystatusdateint
    from ccx_liabilitystatushist_adjusted as ls
    left join submission_period_all as sp_all
        on sp_all.submission_period = left(to_char(ls.liabilitystatusdecisiondate, 'YYYYMMDD'), 6)
    inner join submission_period_current as sp_cur
        on ls.liabilitystatusdecisiondate <= sp_cur.submission_period_end_dt
),

ls_ranked as (
    select
        *,
        rank() over (
            partition by claimworkcompid
            order by liabilitystatusdecisiondate desc, id desc
        ) as life_ranking,
        rank() over (
            partition by claimworkcompid, cast(liabilitystatusdecisiondate as date)
            order by liabilitystatusdecisiondate desc, id desc
        ) as day_ranking,
        case
            when submission_period is null and rank() over (partition by claimworkcompid order by liabilitystatusdecisiondate desc, id desc) = 1
                then 'Outside Submission'
            when submission_period is not null and rank() over (partition by claimworkcompid, cast(liabilitystatusdecisiondate as date) order by liabilitystatusdecisiondate desc, id desc) = 1
                then 'Inside Submission'
        end as insideoutsidelogic
    from ls_with_period
),

ls as (
    select *
    from ls_ranked
    where (submission_period is null and life_ranking = 1)
        or (submission_period is not null and day_ranking = 1)
)

select
    ccr.c_2_2_2_claim_number,
    ccr.claimworkcompid,
    ls.id,
    coalesce(to_char(ls.liabilitystatusdecisiondate, 'YYYYMMDD'), '00000000') as liabilitystatusdate,
    ls.liabilitystatus,
    ls.typecode,
    case
        when coalesce(ccls.c_2_2_6_date_claim_closed, '1900-01-01') = '1900-01-01' then 'N'
        when coalesce(ccls.c_2_2_6_date_claim_closed, '1900-01-01') < coalesce(cro.c_2_2_7_date_claim_re_opened, '1900-01-01') then 'N'
        else 'Y'
    end as c_2_2_5_claim_closed_flag,
    coalesce(to_char(ccls.c_2_2_6_date_claim_closed, 'YYYYMMDD'), '00000000') as c_2_2_6_date_claim_closed,
    coalesce(to_char(cro.c_2_2_7_date_claim_re_opened, 'YYYYMMDD'), '00000000') as c_2_2_7_date_claim_re_opened,
    coalesce(cro.c_2_2_8_reason_for_re_opening_claim_code, '0') as c_2_2_8_reason_for_re_opening_claim_code,
    ls.life_ranking,
    ls.insideoutsidelogic
from ccr
left join ccls on ccr.claimnumber = ccls.claimnumber
left join cro on ccr.claimnumber = cro.claimnumber
inner join ls on ls.claimworkcompid = ccr.claimworkcompid
