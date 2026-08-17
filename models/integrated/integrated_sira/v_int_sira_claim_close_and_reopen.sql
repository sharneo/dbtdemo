{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Claim Close and Reopen View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with submission_period as (
    select
        submission_period,
        submission_period_end_dt,
        submission_period_start_dt
    from {{ ref('v_sira_submission_period_current') }}
    where current_submission_flag = 'Y'
),

insurer_control as (
    select
        insurer_number
    from {{ ref('v_insurer_control_current') }}
    where active_ind = 'Y'
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
        lodgingagent_icare,
        updatetime
    from {{ ref('v_cc_claim_current') }}
),

cctl_claimstate as (
    select
        id
    from {{ ref('v_cctl_claimstate_current') }}
),

cctl_claimreopenedreason as (
    select
        id,
        typecode
    from {{ ref('v_cctl_claimreopenedreason_current') }}
),

cctl_claimagent_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_claimagent_icare_current') }}
),

ccr as (
    select
        c.claimnumber || coalesce(ag.typecode, '') as c_2_2_2_claim_number,
        c.claimworkcompid,
        case when st.id = 3 then 'Y' else 'N' end as c_2_2_5_claim_closed_flag,
        c.closedate_icare as c_2_2_6_date_claim_closed,
        c.reopendate as c_2_2_7_date_claim_re_opened,
        coalesce(cor.typecode, '0') as c_2_2_8_reason_for_re_opening_claim_code,
        subperiod.submission_period_end_dt
    from cc_claim as c
    inner join submission_period as subperiod on 1 = 1
    left join cctl_claimstate as st on st.id = c.state
    left join cctl_claimreopenedreason as cor on cor.id = c.reopenedreason
    inner join cctl_claimagent_icare as ag on ag.id = c.lodgingagent_icare
    inner join insurer_control as ic on ic.insurer_number = ag.typecode
),

ls_combined as (
    select
        claimworkcompid,
        liabilitystatusdate,
        liabilitystatus,
        typecode,
        ranking,
        row_number() over (partition by claimworkcompid order by liabilitystatusdate desc) as row_num
    from (
        select
            claimworkcompid,
            liabilitystatusdate,
            liabilitystatus,
            typecode,
            ranking
        from {{ ref('v_int_sira_liability_status') }}
        union all
        select
            claimworkcompid,
            liabilitystatusdate,
            liabilitystatus,
            typecode,
            ranking
        from {{ ref('v_int_sira_liability_status') }}
    )
),

ls as (
    select
        claimworkcompid,
        to_char(liabilitystatusdate, 'YYYYMMDD') as liabilitystatusdate,
        liabilitystatus,
        typecode,
        ranking
    from ls_combined
    where row_num = 1
)

select
    ccr.c_2_2_2_claim_number,
    ls.claimworkcompid,
    ls.liabilitystatusdate,
    ls.liabilitystatus,
    ls.typecode,
    case
        when ccr.c_2_2_6_date_claim_closed is not null
            and ccr.c_2_2_6_date_claim_closed > ccr.submission_period_end_dt
            then 'N'
        else ccr.c_2_2_5_claim_closed_flag
    end as c_2_2_5_claim_closed_flag,
    case
        when ccr.c_2_2_6_date_claim_closed is not null
            and ccr.c_2_2_6_date_claim_closed > ccr.submission_period_end_dt
            then '00000000'
        else coalesce(to_char(ccr.c_2_2_6_date_claim_closed, 'YYYYMMDD'), '00000000')
    end as c_2_2_6_date_claim_closed,
    case
        when ccr.c_2_2_7_date_claim_re_opened is not null
            and ccr.c_2_2_7_date_claim_re_opened > ccr.submission_period_end_dt
            then '00000000'
        else coalesce(to_char(ccr.c_2_2_7_date_claim_re_opened, 'YYYYMMDD'), '00000000')
    end as c_2_2_7_date_claim_re_opened,
    case
        when ccr.c_2_2_7_date_claim_re_opened is not null
            and ccr.c_2_2_7_date_claim_re_opened > ccr.submission_period_end_dt
            then '0'
        else ccr.c_2_2_8_reason_for_re_opening_claim_code
    end as c_2_2_8_reason_for_re_opening_claim_code,
    ls.ranking
from ls
left join ccr on ls.claimworkcompid = ccr.claimworkcompid
