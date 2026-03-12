
{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates Helper View of the C_2_2_CLAIM_ACTIVITY

-#}

{{ config(
    materialized='view',
    tags=["sira", "business_critical","hlper_views"]
) }}

with cc_claim as (
    select
        hash_key as claim_sk,
        id,
        claimnumber,
        state,
        reopenedreason,
        closedate_icare,
        reopendate,
        description,
        retired,
        reportedbytype,
        claimworkcompid,
        losstype,
        lodgingagent_icare,
        claimsagent_icare
    from {{ ref('vw_cc_claim_current') }}
),

submission_period as (
    select
        '202602' as submission_period,
        '2026-01-28' as submission_period_end_dt,
        '2026-01-01' as submission_period_start_dt
),

cctl_claimstate as (
    select id
    from {{ ref('vw_cctl_claimstate_current') }}
),

cctl_claimreopenedreason as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_claimreopenedreason_current') }}
),

cc_claimempdata as (
    select
        ownerid,
        foreignentityid
    from {{ ref('vw_cc_claimempdata_current') }}
),

cc_incident as (
    select
        claimid,
        retired,
        claimincident,
        description
    from {{ ref('vw_cc_incident_current') }}
    where
        retired = 0
        and claimincident = 1
),

cc_workcomp as (
    select
        id,
        retired,
        reasonableexcuse_icare
    from {{ ref('vw_cc_workcomp_current') }}
    where retired = 0
),

cctl_reasonableexcuse_icare as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_reasonableexcuse_icare_current') }}
),

cctl_personrelationtype as (
    select
        id,
        typecode,
        retired
    from {{ ref('vw_cctl_personrelationtype_current') }}
),

cc_employmentdata as (
    select
        id,
        retired
    from {{ ref('vw_cc_employmentdata_current') }}
    where retired = 0
),

cctl_losstype as (
    select id
    from {{ ref('vw_cctl_losstype_current') }}
),

cctl_claimagent_icare as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_claimagent_icare_current') }}
),

insurer_control as (
    select
        '016' as insurer_number,
        'Y' as active_ind
),

base as (
    select
        c.claim_sk,
        subperiod.submission_period,
        2 as c_2_2_1_record_type,
        2 as c_2_2_3_record_identifier,
        '' as c_2_2_10_no_longer_in_use,
        '' as c_2_2_12_no_longer_in_use,
        '' as c_2_2_14_no_longer_in_use,
        'N' as c_2_2_15_second_injury_claim_flag,
        '' as c_2_2_18_no_longer_in_use,
        '' as c_2_2_19_no_longer_in_use,
        '000' as c_2_2_31_result_of_the_permanent_impairment_assessment_pi_perc,
        '00' as c_2_2_39_section_52a_code,
        '00000000' as c_2_2_42_work_capacity_transition_date,
        '00' as c_2_2_43_work_capacity_transition_outcome,
        '000' as c_2_2_45_assessed_percentage_of_permanent_impairment_for_paid_s66_benefits,
        cast(coalesce(mag.typecode, lag.typecode) as varchar(5)) as insurer_number,
        'Y' as omd_curr_record_ind,
        'N' as omd_del_record_ind,
        'N' as sira_processing_ind,
        0 as sira_extract_rule_cd,
        'P' as processing_ind,
        cast(subperiod.submission_period as varchar) || '^' || c.claimnumber as ca_key,
        c.claimnumber || coalesce(lag.typecode, '701') as c_2_2_2_claim_number,
        case when st.id = 3 then 'Y' else 'N' end as c_2_2_5_claim_closed_flag,
        coalesce(to_char(c.closedate_icare, 'YYYYMMDD'), '00000000') as c_2_2_6_date_claim_closed,
        coalesce(to_char(c.reopendate, 'YYYYMMDD'), '00000000') as c_2_2_7_date_claim_re_opened,
        coalesce(cor.typecode, '0') as c_2_2_8_reason_for_re_opening_claim_code,
        coalesce(prrt.typecode, '00') as c_2_2_16_initial_notifier_code,
        coalesce(rexc.typecode, '00') as c_2_2_17_reasonable_excuse_code,
        replace(replace(rtrim(ltrim(c.description)), chr(13), ' '), chr(10), ' ') as c_2_2_25_description_of_incident,
        replace(replace(rtrim(ltrim(i.description)), chr(13), ' '), chr(10), ' ')
            as c_2_2_26_description_of_injury_illness,
        case
            when -2 = 1 then '00'
            when 4 = 1001 then '00'
            else '00'
        end as c_2_2_34_recovery_investigation_indicator,
        case c.retired when 0 then 'N' else 'Y' end as claim_retired,
        coalesce(c.claimworkcompid, 0) as fk_claimworkcompid,
        coalesce(c.id, 0) as fk_claimid,
        coalesce(ce.foreignentityid, 0) as fk_foreignentityid
    from cc_claim as c
    cross join submission_period as subperiod
    left join cctl_claimstate as st on c.state = st.id
    left join cctl_claimreopenedreason as cor on c.reopenedreason = cor.id
    left join cc_claimempdata as ce on c.id = ce.ownerid
    left join cc_incident as i on c.id = i.claimid and i.claimincident = 1 and i.retired = 0
    left join cctl_personrelationtype as prt on c.reportedbytype = prt.id and prt.retired = 0
    left join cc_workcomp as wcs on c.claimworkcompid = wcs.id and wcs.retired = 0
    left join cctl_reasonableexcuse_icare as rexc on wcs.reasonableexcuse_icare = rexc.id
    left join cctl_personrelationtype as prrt on c.reportedbytype = prrt.id
    left join cc_employmentdata as ed on ce.foreignentityid = ed.id and ed.retired = 0
    left join cctl_losstype as lt on c.losstype = lt.id
    left join cctl_claimagent_icare as lag on c.lodgingagent_icare = lag.id
    left join cctl_claimagent_icare as mag on c.claimsagent_icare = mag.id
    inner join insurer_control as ic on mag.typecode = ic.insurer_number
)

select
    claim_sk,
    submission_period,
    ca_key,
    c_2_2_1_record_type,
    c_2_2_2_claim_number,
    c_2_2_3_record_identifier,
    c_2_2_5_claim_closed_flag,
    c_2_2_6_date_claim_closed,
    c_2_2_7_date_claim_re_opened,
    c_2_2_8_reason_for_re_opening_claim_code,
    c_2_2_10_no_longer_in_use,
    c_2_2_12_no_longer_in_use,
    c_2_2_14_no_longer_in_use,
    c_2_2_15_second_injury_claim_flag,
    c_2_2_16_initial_notifier_code,
    c_2_2_17_reasonable_excuse_code,
    c_2_2_18_no_longer_in_use,
    c_2_2_19_no_longer_in_use,
    c_2_2_25_description_of_incident,
    c_2_2_26_description_of_injury_illness,
    c_2_2_31_result_of_the_permanent_impairment_assessment_pi_perc,
    c_2_2_34_recovery_investigation_indicator,
    c_2_2_39_section_52a_code,
    c_2_2_42_work_capacity_transition_date,
    c_2_2_43_work_capacity_transition_outcome,
    c_2_2_45_assessed_percentage_of_permanent_impairment_for_paid_s66_benefits,
    claim_retired,
    insurer_number,
    fk_claimworkcompid,
    fk_claimid,
    fk_foreignentityid,
    sira_processing_ind,
    sira_extract_rule_cd,
    processing_ind
from base
