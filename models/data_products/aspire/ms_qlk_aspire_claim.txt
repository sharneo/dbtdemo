{{
  config(
    materialized='incremental',
    unique_key='claim_sk',
    incremental_strategy='merge'
  )
}}

{#
  Source: 05_CLAIM.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A05
  TBL_NM: MSC_QLK_ASPIRE_CLAIM

  Version History:
  0.1  28/09/18  Q.Nguyen  Initial Draft
  0.2  02/10/18  Q.Nguyen  Remove RETIRED=0 filter from all CCTL lookup joins
  0.3  09/10/18  Q.Nguyen  Fixed up the indicators to display Y/N
  0.4  12/10/18  Q.Nguyen  Addition of columns
  0.5  07/02/19  B.Simmonds  Addition of columns POLICY_VERIFIED, POLICY_MANUAL_VERIFY
  0.6  15/05/19  Q.Nguyen  Addition of fatality flag
  0.7  12/06/19  V.Lau  Addition of Tariff Rate Number
  0.8  28/08/19  V.Lau  Addition of Claim Migrated Ind
  0.9  02/09/19  Yu Yain  Addition of Wage Reimbursement Schedule Ind
  1.0  14/01/20  Yu Yain  Added Managing Entity ID, _SK and _CD
  1.1  20/03/20  Leo Tian  Added Injury worker DOB
  1.2  23/07/20  Yu Yain  EST10-1064: Added Cost Centre
  1.3  01/09/20  V.Lau  EST10-1238: Added Policy related fields
  1.4  02/11/20  Yu Yain  EST10-1328: Added 7 new fields
  1.5  29/03/21  Yu Yain  EST10-1875: Added Legacy_Policy_Nbr
  1.6  24/02/22  Yu Yain  TDAREP-1552: Added LitigationStatus and Covid indicator
  1.7  10/03/22  Yu Yain  TDAREP-1590: Added Litigation_Identified_DT
  1.8  17/03/22  Yu Yain  TDAREP-1595: Amended CLAIM_REPORT_METHOD_CD
  1.9  28/03/23  Sadia Nayeem  TDAREP-2566: Add WIC Code and Description
  2.0  03/04/23  E.McGregor  TDAREP-2599: Added fields: LAST_TRIAGE_DATE, OVERALL_RISK_RATING
  2.1  16/05/23  M.Park  TDAREP-2741: Added fields
  2.2  22/08/23  Yu Yain  TDAREP-2764: Added Conflict of Interest indicator
  2.3  16/11/23  Yu Yain  TDAREP-3531: Added Claimant mobile number, Contact Prohibited, SMS allowed
  2.4  17/04/24  D.Narashiman  TDAREP-4306: Added Event Type desc
  2.5  19/04/24  E.McGregor  TDAREP-4285: Modified code to meet DSE coding standards
  2.6  03/10/24  S.Nayeem  TMFWCT-1038: Added new field COST_CENTRE
  2.7  18/10/24  S.Nayeem  TMFWCT-1124: Added new field OTHER_COST_CENTRE
#}

with cc_claim as (
    select
        id,
        claimnumber,
        policyid,
        managingentity_icare,
        assignmentdate,
        createtime,
        reporteddate,
        daterptdtoemployer,
        datemade_icare,
        closedate,
        reopendate,
        closedate_icare,
        lossdate,
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
        assigneduserid,
        assignedgroupid,
        employersize_icare,
        isclaimmigrated_icare,
        reimbshragrmnt_icare,
        externalidentifier_icareid,
        permissionrequired,
        litigationstatus,
        isconflictofinterest_ext,
        iscovidclaim_ext,
        claimworkcompid,
        claimsagent_icare,
        branchinsurer_icare,
        externalfactorsevent_ext,
        incidentreport,
        retired,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
),
cc_policy as (
    select
        id,
        policynumber,
        verified,
        manualverify_icare,
        legacypolicynumber_icare,
        groupnumber_icare,
        tariffrate_icare,
        labourhire_icare,
        employercategory_icare,
        policytype_icare,
        retired
    from {{ ref('v_cc_policy_current') }}
),
ccx_managingentity_icare as (
    select
        id,
        publicid,
        code
    from {{ ref('v_ccx_managingentity_icare_current') }}
),
cc_workcomp as (
    select
        id,
        employerliability,
        timelossreport,
        medicalreport,
        overallriskrating_ext,
        accidentlocationtype_icare,
        retired
    from {{ ref('v_cc_workcomp_current') }}
),
cctl_claimagent_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_claimagent_icare_current') }}
),
cctl_insurerbranch_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_insurerbranch_icare_current') }}
),
cctl_claimstate as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_claimstate_current') }}
),
cctl_personrelationtype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_personrelationtype_current') }}
),
cctl_assignmentstatus as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_assignmentstatus_current') }}
),
cctl_claimclosedoutcometype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_claimclosedoutcometype_current') }}
),
cctl_claimreopenedreason as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_claimreopenedreason_current') }}
),
cctl_sharedclaim_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_sharedclaim_icare_current') }}
),
cctl_losstype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_losstype_current') }}
),
cctl_losscause as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_losscause_current') }}
),
cctl_compensabilitydecision as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_compensabilitydecision_current') }}
),
ccx_externalidentifier_icare as (
    select
        id,
        systemname,
        uniqueid,
        retired
    from {{ ref('v_ccx_externalidentifier_icare_current') }}
),
cctl_howreportedtype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_howreportedtype_current') }}
),
cc_user as (
    select
        id,
        publicid,
        retired
    from {{ ref('v_cc_user_current') }}
),
cc_group as (
    select
        id,
        publicid,
        retired
    from {{ ref('v_cc_group_current') }}
),
ccx_claimcostcentre_icare as (
    select
        ownerid,
        foreignentityid
    from {{ ref('v_ccx_claimcostcentre_icare_current') }}
),
ccx_costcentre_icare as (
    select
        id,
        name,
        number,
        othername,
        retired
    from {{ ref('v_ccx_costcentre_icare_current') }}
),
cctl_claimsecuritytype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_claimsecuritytype_current') }}
),
cctl_accidentloctype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_accidentloctype_icare_current') }}
),
cc_claimempdata as (
    select
        ownerid,
        foreignentityid
    from {{ ref('v_cc_claimempdata_current') }}
),
cc_employmentdata as (
    select
        id,
        employmentstatus,
        retired
    from {{ ref('v_cc_employmentdata_current') }}
),
cctl_employmentstatustype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_employmentstatustype_current') }}
),
cctl_litigationstatus as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_litigationstatus_current') }}
),
cc_incident as (
    select
        claimid,
        deceaseddate_icare,
        severity
    from {{ ref('v_cc_incident_current') }}
),
cctl_severitytype as (
    select
        id,
        typecode
    from {{ ref('v_cctl_severitytype_current') }}
),
ccx_claimwicicare as (
    select
        ownerid,
        foreignentityid
    from {{ ref('v_ccx_claimwicicare_current') }}
),
ccx_wic_icare as (
    select
        id,
        code,
        description
    from {{ ref('v_ccx_wic_icare_current') }}
),
cctl_labourhire_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_labourhire_icare_current') }}
),
cctl_groupemployersize_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_groupemployersize_icare_current') }}
),
cctl_employercategory_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_employercategory_icare_current') }}
),
cctl_policytype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_policytype_icare_current') }}
),
cctl_overallriskrating_ext as (
    select
        id,
        name
    from {{ ref('v_cctl_overallriskrating_ext_current') }}
),
cc_contact as (
    select
        id,
        dateofbirth,
        cellphone,
        cellphonecountry,
        smsnotification_icare,
        retired
    from {{ ref('v_cc_contact_current') }}
),
cctl_phonecountrycode as (
    select
        id,
        description
    from {{ ref('v_cctl_phonecountrycode_current') }}
),
cc_claimcontact as (
    select
        claimid,
        contactid,
        claimantflag,
        contactprohibited,
        retired
    from {{ ref('v_cc_claimcontact_current') }}
),
cc_matter as (
    select
        claimid,
        raiseddate_icare,
        retired
    from {{ ref('v_cc_matter_current') }}
),
ccx_triagehistory_icare as (
    select
        claimid,
        triagedate,
        retired
    from {{ ref('v_ccx_triagehistory_icare_current') }}
),
ccx_employmentcapacity_icare as (
    select
        claimworkcompid,
        hoursperday,
        daysperweek,
        consultationdate,
        startdate,
        createtime,
        enddate,
        id,
        cocstatus,
        retired
    from {{ ref('v_ccx_employmentcapacity_icare_current') }}
),
cctl_cocstatus_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_cocstatus_icare_current') }}
),
cte_claim_contact as (
    select
        clmcon.claimid as claim_id,
        cast(ctt.dateofbirth as date) as birth_dt,
        case
            when clmcon.contactprohibited = 1 then 'Y'
            else 'N'
        end as contact_prohibited,
        case
            when ctt.smsnotification_icare = 1 then 'Y'
            else 'N'
        end as contact_sms_allowed,
        ctt.cellphone as contact_mobile,
        ctry.description as contact_mobile_country
    from cc_claimcontact clmcon
    left join cc_contact ctt
        on ctt.id = clmcon.contactid
        and ctt.retired = 0
    left join cctl_phonecountrycode ctry
        on ctry.id = ctt.cellphonecountry
    where clmcon.retired = 0 and clmcon.claimantflag = 1
),
cte_matter as (
    select
        clm.id as claim_id,
        cast(min(mtr.raiseddate_icare) as date) as litigation_identified_dt
    from cc_claim clm
    inner join cc_matter mtr
        on mtr.claimid = clm.id
        and mtr.retired = 0
    where clm.litigationstatus is not null
        and clm.retired = 0
    group by clm.id
),
cte_triage as (
    select
        clm.id as claim_id,
        cast(max(trg.triagedate) as date) as last_triage_date
    from cc_claim clm
    inner join ccx_triagehistory_icare trg
        on trg.claimid = clm.id
        and trg.retired = 0
    where clm.retired = 0
    group by clm.id
),
cte_risk as (
    select
        clm.id as claim_id,
        orr.name as overall_risk_rating
    from cc_claim clm
    inner join cc_workcomp wc
        on wc.id = clm.claimworkcompid
        and wc.retired = 0
    left join cctl_overallriskrating_ext orr
        on orr.id = wc.overallriskrating_ext
    where clm.retired = 0
),
cte_hours_per_week as (
    select
        clm.id as claim_id,
        case
            when (empcap.hoursperday is null or empcap.daysperweek is null)
            then 0
            else (empcap.hoursperday * empcap.daysperweek)
        end as hours_per_week_cert_of_cap,
        row_number() over (
            partition by clm.claimnumber
            order by
                coalesce(empcap.consultationdate, '1900-01-01 00:00:00.000') desc,
                coalesce(empcap.startdate, '1900-01-01 00:00:00.000') desc,
                coalesce(empcap.createtime, '1900-01-01 00:00:00.000') desc,
                coalesce(empcap.enddate, '1900-01-01 00:00:00.000') desc,
                empcap.id desc
        ) as row_no
    from cc_claim clm
    inner join cc_workcomp wc
        on wc.id = clm.claimworkcompid
        and wc.retired = 0
    inner join ccx_employmentcapacity_icare empcap
        on empcap.claimworkcompid = wc.id
        and empcap.retired = 0
    left join cctl_cocstatus_icare sta
        on sta.id = empcap.cocstatus
    where clm.retired = 0
        and (empcap.cocstatus is null or sta.typecode = 'valid')
)

select
    md5('GWCC' || clm.claimnumber) as claim_sk,
    'GWCC' as source_system,
    clm.managingentity_icare as managing_entity_id,
    md5('GWCC' || ent.publicid) as managing_entity_sk,
    ent.code as managing_entity_cd,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    pol.policynumber as policy_nbr,
    pol.verified as policy_verified,
    pol.manualverify_icare as policy_manual_verify,
    pol.legacypolicynumber_icare as legacy_policy_nbr,
    clm.assignmentdate as claim_assignment_dttm,
    cast(clm.assignmentdate as date) as claim_assignment_dt,
    clm.createtime as src_create_dttm,
    cast(clm.createtime as date) as src_create_dt,
    clm.reporteddate as claim_report_dttm,
    cast(clm.reporteddate as date) as claim_report_dt,
    clm.daterptdtoemployer as report_to_emp_dttm,
    cast(clm.daterptdtoemployer as date) as report_to_emp_dt,
    clm.datemade_icare as claim_made_dttm,
    cast(clm.datemade_icare as date) as claim_made_dt,
    clm.closedate as claim_close_dttm,
    cast(clm.closedate as date) as claim_close_dt,
    clm.reopendate as claim_reopen_dttm,
    cast(clm.reopendate as date) as claim_reopen_dt,
    clm.closedate_icare as claim_reopen_close_dttm,
    cast(clm.closedate_icare as date) as claim_reopen_close_dt,
    clm.lossdate as loss_dttm,
    cast(clm.lossdate as date) as loss_dt,
    clm.createtime as src_created_dttm,
    cast(clm.createtime as date) as src_created_dt,
    schmagn.typecode as claim_scheme_agent_cd,
    schmagn.name as claim_scheme_agent_desc,
    schmagnbr.typecode as claim_scheme_agent_branch_cd,
    schmagnbr.name as claim_scheme_agent_branch_desc,
    clmstate.typecode as claim_state_cd,
    clmstate.name as claim_state_desc,
    rptby.typecode as claim_report_by_type_cd,
    rptby.name as claim_report_by_type_desc,
    assgnst.typecode as claim_assignment_status_cd,
    assgnst.name as claim_assignment_status_desc,
    clmoutc.typecode as claim_close_outcome_cd,
    clmoutc.name as claim_close_outcome_desc,
    reopenr.typecode as claim_reopen_reason_cd,
    reopenr.name as claim_reopen_reason_desc,
    shrdclm.typecode as shared_claim_cd,
    shrdclm.name as shared_claim_desc,
    losstyp.typecode as loss_type_cd,
    losstyp.name as loss_type_desc,
    losscse.typecode as loss_cause_cd,
    losscse.name as loss_cause_desc,
    clm.description as loss_desc,
    seg.typecode as claim_segment_cd,
    seg.name as claim_segment_desc,
    liblstus.typecode as liability_status_cd,
    liblstus.name as liability_status_desc,
    howrpt.typecode as claim_report_method_cd,
    howrpt.name as claim_report_method_desc,
    case
        when wrk.employerliability = 1
        then 'Y'
        else 'N'
    end as common_law_ind,
    case
        when clm.incidentreport = 1
        then 'Y'
        else 'N'
    end as incident_only_ind,
    case
        when wrk.timelossreport = 1
        then 'Y'
        else 'N'
    end as lost_time_ind,
    case
        when wrk.medicalreport = 1
        then 'Y'
        else 'N'
    end as medical_attention_ind,
    case
        when pol.verified = 1
        then 'Y'
        else 'N'
    end as verified_policy_ind,
    md5('GWCC' || usrown.publicid) as case_owner_user_sk,
    md5('GWCC' || primgrp.publicid) as case_owner_team_sk,
    fatality.claim_fatality_ind,
    pol.tariffrate_icare as tariff_rate,
    case
        when clm.isclaimmigrated_icare = 1
        then 'Y'
        else 'N'
    end as claim_migrated_ind,
    case
        when clm.reimbshragrmnt_icare = 1
        then 'Y'
        else 'N'
    end as claim_reimbursement_sched_ind,
    coninj.birth_dt as injured_worker_dob,
    cc.number as cost_centre_nbr,
    case
        when cc.othername is null
        then cc.name
        else cc.othername
    end as cost_centre_name,
    cc.name as cost_centre,
    cc.othername as other_cost_centre,
    dim_policytype.name as policy_type,
    dim_empsize.name as employer_size_desc,
    dim_empcat.name as employer_category_desc,
    dimempsts.name as employment_status,
    clmsec.typecode as sensitive_claim_cd,
    clmsec.name as sensitive_claim_desc,
    loctyp.typecode as accident_location_cd,
    loctyp.name as accident_location_desc,
    pol.groupnumber_icare as policy_group_nbr,
    ims.uniqueid as ims_unique_id,
    ims.systemname as ims_name,
    ltg.typecode as litigation_status_cd,
    ltg.name as litigation_status_desc,
    mtr.litigation_identified_dt,
    case
        when clm.iscovidclaim_ext = 1
        then 'Y'
        else 'N'
    end as covid_ind,
    wic.code as wic_code,
    wic.description as wic_desc,
    trg.last_triage_date,
    rsk.overall_risk_rating,
    dim_lh.name as claim_policy_labour_hire_flag,
    hoursperweek.hours_per_week_cert_of_cap as total_hours_per_week_latest_cert_of_cap,
    case
        when clm.isconflictofinterest_ext = 1
        then 'Y'
        else 'N'
    end as conflict_of_interest_ind,
    coninj.contact_prohibited,
    coninj.contact_sms_allowed,
    coninj.contact_mobile,
    coninj.contact_mobile_country,
    clm.externalfactorsevent_ext as event_type_desc,
    clm.file_ingestion_timestamp

from cc_claim clm

left join cc_policy pol
    on pol.id = clm.policyid
    and pol.retired = 0

left join cctl_groupemployersize_icare dim_empsize
    on dim_empsize.id = clm.employersize_icare

left join cctl_employercategory_icare dim_empcat
    on dim_empcat.id = pol.employercategory_icare

left join cctl_policytype_icare dim_policytype
    on dim_policytype.id = pol.policytype_icare

left join ccx_managingentity_icare ent
    on ent.id = clm.managingentity_icare

left join cc_workcomp wrk
    on wrk.id = clm.claimworkcompid

left join cctl_claimagent_icare schmagn
    on schmagn.id = clm.claimsagent_icare

left join cctl_insurerbranch_icare schmagnbr
    on schmagnbr.id = clm.branchinsurer_icare

left join cctl_claimstate clmstate
    on clmstate.id = clm.state

left join cctl_personrelationtype rptby
    on rptby.id = clm.reportedbytype

left join cctl_assignmentstatus assgnst
    on assgnst.id = clm.assignmentstatus

left join cctl_claimclosedoutcometype clmoutc
    on clmoutc.id = clm.closedoutcome

left join cctl_claimreopenedreason reopenr
    on reopenr.id = clm.reopenedreason

left join cctl_sharedclaim_icare shrdclm
    on shrdclm.id = clm.sharedclaim_icare

left join cctl_losstype losstyp
    on losstyp.id = clm.losstype

left join cctl_losscause losscse
    on losscse.id = clm.losscause

left join cctl_compensabilitydecision liblstus
    on liblstus.id = clm.liabilitystatusdenorm_icare

left join ccx_externalidentifier_icare ims
    on ims.id = clm.externalidentifier_icareid
    and ims.retired = 0

left join cctl_howreportedtype howrpt
    on howrpt.id = (case when ims.systemname is not null then 10009 else clm.howreported end)
    and howrpt.retired = 0

left join cc_user usrown
    on usrown.id = clm.assigneduserid
    and usrown.retired = 0

left join cc_group primgrp
    on primgrp.id = clm.assignedgroupid
    and primgrp.retired = 0

left join ccx_claimcostcentre_icare clmcc
    on clmcc.ownerid = clm.id

left join ccx_costcentre_icare cc
    on cc.id = clmcc.foreignentityid
    and cc.retired = 0

left join cctl_claimsecuritytype clmsec
    on clmsec.id = clm.permissionrequired
    and clmsec.retired = 0

left join cctl_accidentloctype_icare loctyp
    on loctyp.id = wrk.accidentlocationtype_icare
    and loctyp.retired = 0

left join cc_claimempdata clmemp
    on clmemp.ownerid = clm.id

left join cc_employmentdata emp
    on emp.id = clmemp.foreignentityid
    and emp.retired = 0

left join cctl_employmentstatustype dimempsts
    on dimempsts.id = emp.employmentstatus
    and dimempsts.retired = 0

left join cctl_litigationstatus ltg
    on ltg.id = clm.litigationstatus
    and ltg.retired = 0

left join cte_matter mtr
    on mtr.claim_id = clm.id

left join (
    select
        inc.claimid as claim_id,
        'Y' as claim_fatality_ind
    from cc_incident inc
    left join cctl_severitytype dim_severitytype
        on dim_severitytype.id = inc.severity
    where (
        inc.deceaseddate_icare is not null
        or dim_severitytype.typecode = 1
    )
) fatality
    on fatality.claim_id = clm.id

left join cte_claim_contact coninj
    on coninj.claim_id = clm.id

left join ccx_claimwicicare clmwic
    on clmwic.ownerid = clm.id

left join ccx_wic_icare wic
    on wic.id = clmwic.foreignentityid

left join cte_triage trg
    on trg.claim_id = clm.id

left join cte_risk rsk
    on rsk.claim_id = clm.id

left join cctl_labourhire_icare dim_lh
    on dim_lh.id = pol.labourhire_icare

left join cte_hours_per_week hoursperweek
    on hoursperweek.claim_id = clm.id
    and hoursperweek.row_no = 1

where clm.retired = 0

{% if is_incremental() %}
and clm.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}
