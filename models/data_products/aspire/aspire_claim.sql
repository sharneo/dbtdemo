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
{# ============================================================
   BASE TABLE CTEs — all filtering pushed down
   ============================================================ #}

with base_cc_claim as (
    select
        id,
        claimnumber,
        policyid,
        managingentity_icare,
        claimsagent_icare,
        branchinsurer_icare,
        state,
        reportedbytype,
        assignmentstatus,
        closedoutcome,
        reopenedreason,
        sharedclaim_icare,
        losstype,
        losscause,
        description,
        segment,
        liabilitystatusdenorm_icare,
        howreported,
        claimworkcompid,
        assigneduserid,
        assignedgroupid,
        externalidentifier_icareid,
        permissionrequired,
        employersize_icare,
        litigationstatus,
        assignmentdate,
        createtime,
        reporteddate,
        daterptdtoemployer AS daterprdtoemployer,
        datemade_icare,
        closedate,
        reopendate,
        closedate_icare,
        lossdate,
        incidentreport,
        isclaimmigrated_icare,
        reimbschagrmnt_icare,
        iscovidclaim_ext,
        isconflictofinterest_ext,
        externalfactorsevent_ext
    from {{ ref('vw_cc_claim_current') }}
    where retired = 0
),

base_cc_policy as (
    select
        id,
        policynumber,
        verified,
        manualverify_icare,
        legacypolicynumber_icare,
        employercategory_icare,
        policytype_icare,
        tariffrate_icare,
        groupnumber_icare,
        labourhire_icare
    from {{ ref('vw_cc_policy_current') }}
    where retired = 0
),

base_cc_claimcontact as (
    select
        claimid,
        contactid,
        contactprohibited
    from {{ ref('vw_cc_claimcontact_current') }}
    where retired = 0
        and claimantflag = true
),

base_cc_contact as (
    select
        id,
        dateofbirth,
        cellphone,
        cellphonecountry,
        smsnotification_icare
    from {{ ref('vw_cc_contact_current') }}
    where retired = 0
),

base_cc_matter as (
    select
        claimid,
        raiseddate_icare
    from {{ ref('vw_cc_matter_current') }}
    where retired = 0
),

base_ccx_triagehistory_icare as (
    select
        claimid,
        triagedate
    from {{ ref('vw_ccx_triagehistory_icare_current') }}
    where retired = 0
),

base_cc_workcomp as (
    select
        id,
        employerliability,
        timelossreport,
        medicalreport,
        accidentlocationtype_icare,
        overallriskrating_ext
    from {{ ref('vw_cc_workcomp_current') }}
    where retired = 0
),

base_ccx_employmentcapacity_icare as (
    select
        claimworkcompid,
        cocstatus,
        hoursperday,
        daysperweek,
        consultationdate,
        startdate,
        enddate,
        createtime,
        id
    from {{ ref('vw_ccx_employmentcapacity_icare_current') }}
    where retired = 0
),

base_cctl_cocstatus_icare as (
    select id, typecode
    from {{ ref('vw_cctl_cocstatus_icare_current') }}
    where typecode = 'valid'
),

base_ccx_managingentity_icare as (
    select id, publicid, code
    from {{ ref('vw_ccx_managingentity_icare_current') }}
),

base_cctl_claimagent_icare as (
    select id, typecode, name
    from {{ ref('vw_cctl_claimagent_icare_current') }}
),

base_cctl_insurerbranch_icare as (
    select id, typecode, name
    from {{ ref('vw_cctl_insurerbranch_icare_current') }}
),

base_cctl_claimstate as (
    select id, typecode, name
    from {{ ref('vw_cctl_claimstate_current') }}
),

base_cctl_personrelationtype as (
    select id, typecode, name
    from {{ ref('vw_cctl_personrelationtype_current') }}
),

base_cctl_assignmentstatus as (
    select id, typecode, name
    from {{ ref('vw_cctl_assignmentstatus_current') }}
),

base_cctl_claimclosedoutcometype as (
    select id, typecode, name
    from {{ ref('vw_cctl_claimclosedoutcometype_current') }}
),

base_cctl_claimreopenedreason as (
    select id, typecode, name
    from {{ ref('vw_cctl_claimreopenedreason_current') }}
),

base_cctl_sharedclaim_icare as (
    select id, typecode, name
    from {{ ref('vw_cctl_sharedclaim_icare_current') }}
),

base_cctl_losstype as (
    select id, typecode, name
    from {{ ref('vw_cctl_losstype_current') }}
),

base_cctl_losscause as (
    select id, typecode, name
    from {{ ref('vw_cctl_losscause_current') }}
),

base_cctl_compensabilitydecision as (
    select id, typecode, name
    from {{ ref('vw_cctl_compensabilitydecision_current') }}
),

base_cctl_claimsegment as (
    select id, typecode, name
    from {{ ref('vw_cctl_claimsegment_current') }}
),

base_cctl_howreportedtype as (
    select id, typecode, name
    from {{ ref('vw_cctl_howreportedtype_current') }}
    where retired = 0
),

base_cctl_claimsecuritytype as (
    select id, typecode, name
    from {{ ref('vw_cctl_claimsecuritytype_current') }}
    where retired = 0
),

base_cctl_accidentloctype_icare as (
    select id, typecode, name
    from {{ ref('vw_cctl_accidentloctype_icare_current') }}
    where retired = 0
),

base_cctl_employmentstatustype as (
    select id, name
    from {{ ref('vw_cctl_employmentstatustype_current') }}
    where retired = 0
),

base_cctl_litigationstatus as (
    select id, typecode, name
    from {{ ref('vw_cctl_litigationstatus_current') }}
    where retired = 0
),

base_cctl_groupemployersize_icare as (
    select id, name
    from {{ ref('vw_cctl_groupemployersize_icare_current') }}
),

base_cctl_employercategory_icare as (
    select id, name
    from {{ ref('vw_cctl_employercategory_icare_current') }}
),

base_cctl_policytype_icare as (
    select id, name
    from {{ ref('vw_cctl_policytype_icare_current') }}
),

base_cctl_phonecountrycode as (
    select id, description
    from {{ ref('vw_cctl_phonecountrycode_current') }}
),

base_cctl_overallriskrating_ext as (
    select id, name
    from {{ ref('vw_cctl_overallriskrating_ext_current') }}
),

base_cctl_severitytype as (
    select id, typecode
    from {{ ref('vw_cctl_severitytype_current') }}
),

base_cctl_labourhire_icare as (
    select id, name
    from {{ ref('vw_cctl_labourhire_icare_current') }}
),

base_ccx_externalidentifier_icare as (
    select id, uniqueid, systemname
    from {{ ref('vw_ccx_externalidentifier_icare_current') }}
    where retired = 0
),

base_cc_user as (
    select id, publicid
    from {{ ref('vw_cc_user_current') }}
    where retired = 0
),

base_cc_group as (
    select id, publicid
    from {{ ref('vw_cc_group_current') }}
    where retired = 0
),

base_ccx_claimcostcentreicare as (
    select ownerid, foreignentityid
    from {{ ref('vw_ccx_claimcostcentreicare_current') }}
),

base_ccx_costcentre_icare as (
    select id, number, name, othername
    from {{ ref('vw_ccx_costcentre_icare_current') }}
    where retired = 0
),

base_cc_incident as (
    select claimid, deceaseddate_icare, severity
    from {{ ref('vw_cc_incident_current') }}
),

base_ccx_claimwicicare as (
    select ownerid, foreignentityid
    from {{ ref('vw_ccx_claimwicicare_current') }}
),

base_ccx_wic_icare as (
    select id, code, description
    from {{ ref('vw_ccx_wic_icare_current') }}
),

base_cc_claimempdata as (
    select ownerid, foreignentityid
    from {{ ref('vw_cc_claimempdata_current') }}
),

base_cc_employmentdata as (
    select id, employmentstatus
    from {{ ref('vw_cc_employmentdata_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_claim_contact as (
    select
        clmcon.claimid                          as claim_id,
        ctt.dateofbirth::date                   as birth_dt,
        iff(clmcon.contactprohibited = True, 'Y', 'N')
                                                as contact_prohibited,
        iff(ctt.smsnotification_icare = True, 'Y', 'N')
                                                as contact_sms_allowed,
        ctt.cellphone                           as contact_mobile,
        ctry.description                        as contact_mobile_country
    from base_cc_claimcontact as clmcon
    left join base_cc_contact as ctt
        on ctt.id = clmcon.contactid
    left join base_cctl_phonecountrycode as ctry
        on ctry.id = ctt.cellphonecountry
),

cte_matter as (
    select
        clm.id                                  as claim_id,
        min(mtr.raiseddate_icare)::date         as litigation_identified_dt
    from base_cc_claim as clm
    inner join base_cc_matter as mtr
        on mtr.claimid = clm.id
    where clm.litigationstatus is not null
    group by clm.id
),

cte_triage as (
    select
        trg.claimid                             as claim_id,
        max(trg.triagedate)::date               as last_triage_date
    from base_ccx_triagehistory_icare as trg
    group by trg.claimid
),

cte_risk as (
    select
        clm.id                                  as claim_id,
        orr.name                                as overall_risk_rating
    from base_cc_claim as clm
    inner join base_cc_workcomp as wc
        on wc.id = clm.claimworkcompid
    left join base_cctl_overallriskrating_ext as orr
        on orr.id = wc.overallriskrating_ext
),

cte_hours_per_week as (
    select
        clm.id                                  as claim_id,
        case
            when empcap.hoursperday is null or empcap.daysperweek is null then 0
            else empcap.hoursperday * empcap.daysperweek
        end                                     as hours_per_week_cert_of_cap
    from base_cc_claim as clm
    inner join base_cc_workcomp as wc
        on wc.id = clm.claimworkcompid
    inner join base_ccx_employmentcapacity_icare as empcap
        on empcap.claimworkcompid = wc.id
    where empcap.cocstatus is null
        or empcap.cocstatus in (select id from base_cctl_cocstatus_icare)
    qualify row_number() over (
        partition by clm.claimnumber
        order by
            coalesce(empcap.consultationdate, '1900-01-01'::timestamp) desc,
            coalesce(empcap.startdate, '1900-01-01'::timestamp) desc,
            coalesce(empcap.createtime, '1900-01-01'::timestamp) desc,
            coalesce(empcap.enddate, '1900-01-01'::timestamp) desc,
            empcap.id desc
    ) = 1
),

cte_fatality as (
    select distinct
        inc.claimid                             as claim_id,
        'Y'                                     as claim_fatality_ind
    from base_cc_incident as inc
    left join base_cctl_severitytype as sev
        on sev.id = inc.severity
    where inc.deceaseddate_icare is not null
        or sev.typecode = '1'
),

cte_cost_centre as (
    select
        clmcc.ownerid                           as claim_id,
        cc.number                               as cost_centre_nbr,
        coalesce(cc.othername, cc.name)         as cost_centre_name
    from base_ccx_claimcostcentreicare as clmcc
    inner join base_ccx_costcentre_icare as cc
        on cc.id = clmcc.foreignentityid
),

cte_wic as (
    select
        clmwic.ownerid                          as claim_id,
        wic.code                                as wic_code,
        wic.description                         as wic_desc
    from base_ccx_claimwicicare as clmwic
    inner join base_ccx_wic_icare as wic
        on wic.id = clmwic.foreignentityid
),

cte_employment as (
    select
        clmemp.ownerid                          as claim_id,
        dimempsts.name                          as employment_status
    from base_cc_claimempdata as clmemp
    inner join base_cc_employmentdata as emp
        on emp.id = clmemp.foreignentityid
    left join base_cctl_employmentstatustype as dimempsts
        on dimempsts.id = emp.employmentstatus
),

cte_ims as (
    select
        ims.id,
        ims.uniqueid                            as ims_unique_id,
        ims.systemname                          as ims_name
    from base_ccx_externalidentifier_icare as ims
),

cte_how_reported as (
    select
        clm.id                                  as claim_id,
        howrpt.typecode                         as claim_report_method_cd,
        howrpt.name                             as claim_report_method_desc
    from base_cc_claim as clm
    left join cte_ims as ims
        on ims.id = clm.externalidentifier_icareid
    left join base_cctl_howreportedtype as howrpt
        on howrpt.id = case
            when ims.ims_name is not null then 10009
            else clm.howreported
        end
)

{# ============================================================
   FINAL OUTPUT
   ============================================================ #}

select
    md5(concat('GWCC', clm.claimnumber))        as claim_sk,
    'GWCC'                                       as src_system_cd,
    clm.managingentity_icare                     as managing_entity_id,
    md5(concat('GWCC', ent.publicid))            as managing_entity_sk,
    ent.code                                     as managing_entity_cd,
    clm.claimnumber                              as claim_nbr,
    clm.id                                       as src_claim_id,
    pol.policynumber                             as policy_nbr,
    pol.verified                                 as policy_verified,
    pol.manualverify_icare                       as policy_manual_verify,
    pol.legacypolicynumber_icare                 as legacy_policy_nbr,
    clm.assignmentdate                           as claim_assignment_dttm,
    clm.assignmentdate::date                     as claim_assignment_dt,
    clm.createtime                               as src_create_dttm,
    clm.createtime::date                         as src_create_dt,
    clm.reporteddate                             as claim_report_dttm,
    clm.reporteddate::date                       as claim_report_dt,
    clm.daterprdtoemployer                       as report_to_emp_dttm,
    clm.daterprdtoemployer::date                 as report_to_emp_dt,
    clm.datemade_icare                           as claim_made_dttm,
    clm.datemade_icare::date                     as claim_made_dt,
    clm.closedate                                as claim_close_dttm,
    clm.closedate::date                          as claim_close_dt,
    clm.reopendate                               as claim_reopen_dttm,
    clm.reopendate::date                         as claim_reopen_dt,
    clm.closedate_icare                          as claim_reopen_close_dttm,
    clm.closedate_icare::date                    as claim_reopen_close_dt,
    clm.lossdate                                 as loss_dttm,
    clm.lossdate::date                           as loss_dt,
    clm.createtime                               as src_created_dttm,
    clm.createtime::date                         as src_created_dt,
    schmagn.typecode                             as claim_scheme_agent_cd,
    schmagn.name                                 as claim_scheme_agent_desc,
    schmagnbr.typecode                           as claim_scheme_agent_branch_cd,
    schmagnbr.name                               as claim_scheme_agent_branch_desc,
    clmstate.typecode                            as claim_state_cd,
    clmstate.name                                as claim_state_desc,
    rptby.typecode                               as claim_report_by_type_cd,
    rptby.name                                   as claim_report_by_type_desc,
    assgnst.typecode                             as claim_assignment_status_cd,
    assgnst.name                                 as claim_assignment_status_desc,
    clmoutc.typecode                             as claim_close_outcome_cd,
    clmoutc.name                                 as claim_close_outcome_desc,
    reopenr.typecode                             as claim_reopen_reason_cd,
    reopenr.name                                 as claim_reopen_reason_desc,
    shrdclm.typecode                             as shared_claim_cd,
    shrdclm.name                                 as shared_claim_desc,
    losstyp.typecode                             as loss_type_cd,
    losstyp.name                                 as loss_type_desc,
    losscse.typecode                             as loss_cause_cd,
    losscse.name                                 as loss_cause_desc,
    clm.description                              as loss_desc,
    seg.typecode                                 as claim_segment_cd,
    seg.name                                     as claim_segment_desc,
    liblstus.typecode                            as liability_status_cd,
    liblstus.name                                as liability_status_desc,
    howrpt.claim_report_method_cd,
    howrpt.claim_report_method_desc,
    iff(wrk.employerliability = true, 'Y', 'N') as common_law_ind,
    iff(clm.incidentreport = true, 'Y', 'N')    as incident_only_ind,
    iff(wrk.timelossreport = true, 'Y', 'N')    as lost_time_ind,
    iff(wrk.medicalreport = true, 'Y', 'N')     as medical_attention_ind,
    iff(pol.verified = true, 'Y', 'N')          as verified_policy_ind,
    md5(concat('GWCC', usrown.publicid))         as case_owner_user_sk,
    md5(concat('GWCC', primgrp.publicid))        as case_owner_team_sk,
    fatality.claim_fatality_ind,
    pol.tariffrate_icare                         as tariff_rate,
    iff(clm.isclaimmigrated_icare = true, 'Y', 'N')
                                                 as claim_migrated_ind,
    case
        when clm.reimbschagrmnt_icare = 1 then 'Y'
        else 'N'
    end                                          as claim_reimbursement_sched_ind,
    coninj.birth_dt                              as injured_worker_dob,
    costctr.cost_centre_nbr,
    costctr.cost_centre_name,
    dim_policytype.name                          as policy_type,
    dim_empsize.name                             as employer_size_desc,
    dim_empcat.name                              as employer_category_desc,
    employment.employment_status,
    clmsec.typecode                              as sensitive_claim_cd,
    clmsec.name                                  as sensitive_claim_desc,
    loctyp.typecode                              as accident_location_cd,
    loctyp.name                                  as accident_location_desc,
    pol.groupnumber_icare                        as policy_group_nbr,
    ims.ims_unique_id,
    ims.ims_name,
    ltg.typecode                                 as litigation_status_cd,
    ltg.name                                     as litigation_status_desc,
    mtr.litigation_identified_dt,
    iff(clm.iscovidclaim_ext = true, 'Y', 'N')  as covid_ind,
    clmwic.wic_code,
    clmwic.wic_desc,
    trg.last_triage_date,
    rsk.overall_risk_rating,
    dim_lh.name                                  as claim_policy_labour_hire_flag,
    hoursperweek.hours_per_week_cert_of_cap      as total_hours_per_week_latest_cert_of_cap,
    iff(clm.isconflictofinterest_ext = True, 'Y', 'N')
                                                 as conflict_of_interest_ind,
    coninj.contact_prohibited,
    coninj.contact_sms_allowed,
    coninj.contact_mobile,
    coninj.contact_mobile_country,
    clm.externalfactorsevent_ext                 as event_type_desc

from base_cc_claim as clm
left join base_cc_policy as pol
    on pol.id = clm.policyid
left join base_cc_workcomp as wrk
    on wrk.id = clm.claimworkcompid
left join base_ccx_managingentity_icare as ent
    on ent.id = clm.managingentity_icare
left join base_cctl_claimagent_icare as schmagn
    on schmagn.id = clm.claimsagent_icare
left join base_cctl_insurerbranch_icare as schmagnbr
    on schmagnbr.id = clm.branchinsurer_icare
left join base_cctl_claimstate as clmstate
    on clmstate.id = clm.state
left join base_cctl_personrelationtype as rptby
    on rptby.id = clm.reportedbytype
left join base_cctl_assignmentstatus as assgnst
    on assgnst.id = clm.assignmentstatus
left join base_cctl_claimclosedoutcometype as clmoutc
    on clmoutc.id = clm.closedoutcome
left join base_cctl_claimreopenedreason as reopenr
    on reopenr.id = clm.reopenedreason
left join base_cctl_sharedclaim_icare as shrdclm
    on shrdclm.id = clm.sharedclaim_icare
left join base_cctl_losstype as losstyp
    on losstyp.id = clm.losstype
left join base_cctl_losscause as losscse
    on losscse.id = clm.losscause
left join base_cctl_compensabilitydecision as liblstus
    on liblstus.id = clm.liabilitystatusdenorm_icare
left join base_cctl_claimsegment as seg
    on seg.id = clm.segment
left join cte_ims as ims
    on ims.id = clm.externalidentifier_icareid
left join cte_how_reported as howrpt
    on howrpt.claim_id = clm.id
left join base_cc_user as usrown
    on usrown.id = clm.assigneduserid
left join base_cc_group as primgrp
    on primgrp.id = clm.assignedgroupid
left join cte_cost_centre as costctr
    on costctr.claim_id = clm.id
left join base_cctl_claimsecuritytype as clmsec
    on clmsec.id = clm.permissionrequired
left join base_cctl_accidentloctype_icare as loctyp
    on loctyp.id = wrk.accidentlocationtype_icare
left join cte_employment as employment
    on employment.claim_id = clm.id
left join base_cctl_litigationstatus as ltg
    on ltg.id = clm.litigationstatus
left join cte_matter as mtr
    on mtr.claim_id = clm.id
left join cte_fatality as fatality
    on fatality.claim_id = clm.id
left join cte_claim_contact as coninj
    on coninj.claim_id = clm.id
left join cte_wic as clmwic
    on clmwic.claim_id = clm.id
left join cte_triage as trg
    on trg.claim_id = clm.id
left join cte_risk as rsk
    on rsk.claim_id = clm.id
left join base_cctl_labourhire_icare as dim_lh
    on dim_lh.id = pol.labourhire_icare
left join cte_hours_per_week as hoursperweek
    on hoursperweek.claim_id = clm.id
left join base_cctl_groupemployersize_icare as dim_empsize
    on dim_empsize.id = clm.employersize_icare
left join base_cctl_employercategory_icare as dim_empcat
    on dim_empcat.id = pol.employercategory_icare
left join base_cctl_policytype_icare as dim_policytype
    on dim_policytype.id = pol.policytype_icare