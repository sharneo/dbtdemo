{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Claim Activity View for SIRA Reporting  

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
        claimnumber,
        state,
        closedate_icare,
        reopendate,
        reopenedreason,
        reportedbytype,
        claimworkcompid,
        description,
        losstype,
        lodgingagent_icare,
        claimsagent_icare,
        retired
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cctl_claimstate as (
    select
        id
    from {{ ref('v_cctl_claimstate_current') }}
    where id = 3
),

cctl_claimreopenedreason as (
    select
        id,
        typecode
    from {{ ref('v_cctl_claimreopenedreason_current') }}
),

cc_claimempdata as (
    select
        ownerid,
        foreignentityid
    from {{ ref('v_cc_claimempdata_current') }}
),

cc_incident as (
    select
        claimid,
        description
    from {{ ref('v_cc_incident_current') }}
    where retired = 0 and claimincident = 1
),

cctl_personrelationtype_prt as (
    select
        id
    from {{ ref('v_cctl_personrelationtype_current') }}
    where retired = 0
),

cctl_personrelationtype as (
    select
        id,
        typecode
    from {{ ref('v_cctl_personrelationtype_current') }}
),

cc_workcomp as (
    select
        id,
        reasonableexcuse_icare
    from {{ ref('v_cc_workcomp_current') }}
    where retired = 0
),

cctl_reasonableexcuse_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_reasonableexcuse_icare_current') }}
),

cc_employmentdata as (
    select
        id
    from {{ ref('v_cc_employmentdata_current') }}
    where retired = 0
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

base as (
    select
        c.id as claim_sk,
        subperiod.submission_period,
        cast(subperiod.submission_period as varchar) || '^' || c.claimnumber as ca_key,
        2 as c_2_2_1_record_type,
        c.claimnumber || coalesce(lag_agent.typecode, '701') as c_2_2_2_claim_number,
        2 as c_2_2_3_record_identifier,
        case when st.id is not null then 'Y' else 'N' end as c_2_2_5_claim_closed_flag,
        coalesce(to_char(c.closedate_icare, 'YYYYMMDD'), '00000000') as c_2_2_6_date_claim_closed,
        coalesce(to_char(c.reopendate, 'YYYYMMDD'), '00000000') as c_2_2_7_date_claim_re_opened,
        coalesce(cor.typecode, '0') as c_2_2_8_reason_for_re_opening_claim_code,
        '' as c_2_2_10_no_longer_in_use,
        '' as c_2_2_12_no_longer_in_use,
        '' as c_2_2_14_no_longer_in_use,
        'N' as c_2_2_15_second_injury_claim_flag,
        coalesce(prrt.typecode, '00') as c_2_2_16_initial_notifier_code,
        coalesce(rexc.typecode, '00') as c_2_2_17_reasonable_excuse_code,
        '' as c_2_2_18_no_longer_in_use,
        '' as c_2_2_19_no_longer_in_use,
        replace(replace(rtrim(ltrim(c.description)), chr(13), ' '), chr(10), ' ') as c_2_2_25_description_of_incident,
        replace(replace(rtrim(ltrim(i.description)), chr(13), ' '), chr(10), ' ') as c_2_2_26_description_of_injury_illness,
        '000' as c_2_2_31_result_of_the_permanent_impairment_assessment_pi_perc,
        '00' as c_2_2_34_recovery_investigation_indicator,
        '00' as c_2_2_39_section_52a_code,
        '00000000' as c_2_2_42_work_capacity_transition_date,
        '00' as c_2_2_43_work_capacity_transition_outcome,
        '000' as c_2_2_45_assessed_percentage_of_permanent_impairment_for_paid_s66_benefits,
        case c.retired when 0 then 'N' else 'Y' end as claim_retired,
        cast(coalesce(mag_agent.typecode, lag_agent.typecode) as varchar(5)) as insurer_number,
        coalesce(c.claimworkcompid, 0) as fk_claimworkcompid,
        coalesce(c.id, 0) as fk_claimid,
        coalesce(ce.foreignentityid, 0) as fk_foreignentityid,
        'Y' as omd_curr_record_ind,
        'N' as omd_del_record_ind,
        'N' as sira_processing_ind,
        0 as sira_extract_rule_cd,
        'P' as processing_ind
    from cc_claim as c
    inner join submission_period as subperiod on 1 = 1
    left join cctl_claimstate as st on st.id = c.state
    left join cctl_claimreopenedreason as cor on cor.id = c.reopenedreason
    left join cc_claimempdata as ce on ce.ownerid = c.id
    left join cc_incident as i on c.id = i.claimid
    left join cctl_personrelationtype_prt as prt on prt.id = c.reportedbytype
    left join cc_workcomp as wcs on wcs.id = c.claimworkcompid
    left join cctl_reasonableexcuse_icare as rexc on rexc.id = wcs.reasonableexcuse_icare
    left join cctl_personrelationtype as prrt on prrt.id = c.reportedbytype
    left join cc_employmentdata as ed on ed.id = ce.foreignentityid
    left join cctl_claimagent_icare_lag as lag_agent on lag_agent.id = c.lodgingagent_icare
    left join cctl_claimagent_icare_mag as mag_agent on mag_agent.id = c.claimsagent_icare
    inner join insurer_control as ic on ic.insurer_number = mag_agent.typecode
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
    omd_curr_record_ind,
    omd_del_record_ind,
    sira_processing_ind,
    sira_extract_rule_cd,
    processing_ind
from base
