{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Aspire

-#}   

{{ config(
    materialized='table',
    tags=["aspire","daily","sas","legacy"]
) }}

with base_cc_incident as (
    select
        publicid,
        id,
        claimid,
        subtype,
        description,
        multipleinjuries_icare,
        claimincident,
        deceaseddate_icare,
        significantinjurydate_icare,
        contactcompletedate_icare,
        fatalitynotificationdate_icare,
        resultofselfharm_icare,
        fatalityliabdecisiondate_icare,
        odgrtwdate_icare,
        odgduration_icare,
        breakdownagencycode_icare,
        breakdownagencydesc_icare,
        natureofinjurycode_icare,
        natureofinjurydesc_icare,
        mechanismofinjurycode_icare,
        mechanismofinjurydesc_icare,
        agencyofinjurycode_icare,
        agencyofinjurydesc_icare,
        dutystatus_icare,
        severity,
        fatalityliabdec_icare,
        createtime,
        updatetime
    from {{ ref('vw_cc_incident_current') }}
    where retired = 0
),

base_cc_claim as (
    select
        id,
        claimnumber
    from {{ ref('vw_cc_claim_current') }}
    where retired = 0
),

base_cctl_dutystatus_icare as (
    select id, typecode, name
    from {{ ref('vw_cctl_dutystatus_icare_current') }}
),

base_cctl_severitytype as (
    select id, typecode, name
    from {{ ref('vw_cctl_severitytype_current') }}
),

base_cctl_fatalityliabdec_icare as (
    select id, typecode, name
    from {{ ref('vw_cctl_fatalityliabdec_icare_current') }}
),

base_cctl_incident as (
    select id, typecode, name
    from {{ ref('vw_cctl_incident_current') }}
),

base_ccx_toocsbloiconnector_icare as (
    select
        injuryincident_icareid,
        injurycode,
        injurydescription
    from {{ ref('vw_ccx_toocsbloiconnector_icare_current') }}
    where retired = 0
        and selectedasprimary = true
),

base_cc_injurydiagnosis as (
    select
        injuryincidentid,
        icdcode
    from {{ ref('vw_cc_injurydiagnosis_current') }}
    where retired = 0
        and isprimary = true
),

base_cc_icdcode as (
    select id, code, codedesc
    from {{ ref('vw_cc_icdcode_current') }}
    where retired = 0
)

select distinct
    md5(concat('GWCC', inc.publicid))               as loss_sk,
    md5(concat('GWCC', clm.claimnumber))             as claim_sk,
    clm.claimnumber                                  as claim_nbr,
    inc.id                                           as src_incident_id,
    inc.claimid                                      as src_claim_id,
    inc.description                                  as injury_desc,
    iff(inc.multipleinjuries_icare = true, 'Y', 'N') as multiple_injuries_ind,
    iff(inc.claimincident = true, 'Y', 'N')          as primary_claimable_incident_ind,
    dimincident.typecode                             as incident_type_cd,
    dimincident.name                                 as incident_type_desc,
    inc.deceaseddate_icare                           as deceased_dt,
    inc.significantinjurydate_icare                  as significant_injury_dt,
    inc.contactcompletedate_icare                    as contact_complete_dt,
    inc.fatalitynotificationdate_icare               as fatality_notification_dt,
    inc.resultofselfharm_icare                       as fatality_result_of_self_harm_ind,
    inc.fatalityliabdecisiondate_icare               as fatality_liability_decision_dt,
    inc.odgrtwdate_icare                             as odg_rtw_dt,
    inc.odgduration_icare                            as odg_rtw_day_cnt,
    inc.breakdownagencycode_icare                    as breakdown_agency_cd,
    inc.breakdownagencydesc_icare                    as breakdown_agency_desc,
    inc.natureofinjurycode_icare                     as nature_of_injury_cd,
    inc.natureofinjurydesc_icare                     as nature_of_injury_desc,
    inc.mechanismofinjurycode_icare                  as mechanism_of_injury_cd,
    inc.mechanismofinjurydesc_icare                  as mechanism_of_injury_desc,
    inc.agencyofinjurycode_icare                     as agency_of_injury_cd,
    inc.agencyofinjurydesc_icare                     as agency_of_injury_desc,
    toocs.injurycode                                 as location_of_injury_cd,
    toocs.injurydescription                          as location_of_injury_desc,
    dimdty.typecode                                  as claimant_duty_status_cd,
    dimdty.name                                      as claimant_duty_status_desc,
    dimsev.typecode                                  as result_of_injury_cd,
    dimsev.name                                      as result_of_injury_desc,
    dimfat.typecode                                  as fatality_liability_decision_s1,
    dimfat.name                                      as fatality_liability_decision_s2,
    icd.code                                         as icd_cd,
    icd.codedesc                                     as icd_desc,
    case
        when substring(icd.code, 1, 1) = 'F'
            and (try_cast(substring(icd.code, 2, 2) as int) between 1 and 59
                or try_cast(substring(icd.code, 2, 2) as int) = 99)
        then 'Y'
        else 'N'
    end                                              as mental_stress_ind,
    inc.createtime                                   as src_create_dttm,
    inc.createtime::date                             as src_create_dt,
    inc.updatetime                                   as src_eff_dttm,
    inc.updatetime::date                             as src_eff_dt
from base_cc_incident as inc
inner join base_cc_claim as clm
    on clm.id = inc.claimid
left join base_cctl_incident as dimincident
    on dimincident.id = inc.subtype
left join base_cctl_dutystatus_icare as dimdty
    on dimdty.id = inc.dutystatus_icare
left join base_cctl_severitytype as dimsev
    on dimsev.id = inc.severity
left join base_cctl_fatalityliabdec_icare as dimfat
    on dimfat.id = inc.fatalityliabdec_icare
left join base_ccx_toocsbloiconnector_icare as toocs
    on toocs.injuryincident_icareid = inc.id
left join base_cc_injurydiagnosis as injdiagnosis
    on injdiagnosis.injuryincidentid = inc.id
left join base_cc_icdcode as icd
    on icd.id = injdiagnosis.icdcode