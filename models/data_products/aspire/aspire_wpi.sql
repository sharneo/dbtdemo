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


with base_cc_claim as (
    select
        id,
        claimnumber
    from {{ ref('vw_cc_claim_current') }}
    where retired = 0
),

base_cc_exposure as (
    select
        id,
        claimid
    from {{ ref('vw_cc_exposure_current') }}
    where retired = 0
),

base_ccx_wpiassessment_icare as (
    select
        id,
        exposureid
    from {{ ref('vw_ccx_wpiassessment_icare_current') }}
    where retired = 0
),

base_ccx_wpiassessrecord_icare as (
    select
        id,
        wpiassessment_icareid,
        wpiassessmentstate_icare,
        wpiresult_icare,
        claimedpercentage_icare,
        offeredwpipercentage_icare,
        assessedwpifors66,
        s66receiveddate_icare,
        bhlresult_icare,
        settlementtype_icare,
        createtime
    from {{ ref('vw_ccx_wpiassessrecord_icare_current') }}
    where retired = 0
),

base_ccx_wpidoctorassessment_icare as (
    select
        id,
        wpiassessrecord_icareid,
        reviewer_icareid,
        assessmentdate_icare,
        createtime
    from {{ ref('vw_ccx_wpidoctorassessment_icare_current') }}
    where retired = 0
        and reviewer_icareid is not null
),

base_cctl_wpisettlementtype_icare as (
    select id, name
    from {{ ref('vw_cctl_wpisettlementtype_icare_current') }}
)

select
    md5(concat('GWCC', clm.claimnumber))             as claim_sk,
    'GWCC'                                            as src_system_cd,
    clm.claimnumber                                  as claim_nbr,
    dim_settlementtype.name                          as wpi_claim_type,
    iff(asrcd.wpiassessmentstate_icare = true, 'Closed', 'Open')
                                                     as wpi_status,
    asrcd.wpiresult_icare                            as wpi_result_pct,
    asrcd.claimedpercentage_icare                    as wpi_claimed_pct,
    asrcd.offeredwpipercentage_icare                 as wpi_offered_pct,
    asrcd.assessedwpifors66                          as wpi_assessed_pct,
    asrcd.s66receiveddate_icare::date                as s66_received_dt,
    asrcd.bhlresult_icare                            as binaural_hearing_loss_pct,
    docassess.reviewer_icareid                       as med_assess_src_review_user_id,
    case
        when row_number() over (
            partition by clm.claimnumber, asrcd.id
            order by docassess.assessmentdate_icare desc, docassess.createtime desc
        ) = 1 then 'Y'
        else 'N'
    end                                              as latest_med_assess_review_ind,
    case
        when row_number() over (
            partition by clm.claimnumber
            order by asrcd.s66receiveddate_icare desc, asrcd.createtime desc
        ) = 1 then 'Y'
        else 'N'
    end                                              as latest_wpi_record_ind,
    case
        when count(clm.claimnumber) over (partition by clm.claimnumber) > 1 then 'Y'
        else 'N'
    end                                              as multiple_wpi_ind
from base_cc_claim as clm
inner join base_cc_exposure as exps
    on exps.claimid = clm.id
inner join base_ccx_wpiassessment_icare as asmt
    on asmt.exposureid = exps.id
inner join base_ccx_wpiassessrecord_icare as asrcd
    on asrcd.wpiassessment_icareid = asmt.id
left join base_ccx_wpidoctorassessment_icare as docassess
    on docassess.wpiassessrecord_icareid = asrcd.id
left join base_cctl_wpisettlementtype_icare as dim_settlementtype
    on dim_settlementtype.id = asrcd.settlementtype_icare