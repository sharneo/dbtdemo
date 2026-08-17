{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Aspire - original table materialization
2026-04-20      1.0                             Converted to incremental with merge strategy

-#}

{{
  config(
    materialized='incremental',
    unique_key='claim_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: S01_SECTION_59A_OVERVIEW.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_S01
  TBL_NM: MSC_QLK_ASPIRE_S01_SECTION_59A_OVERVIEW
-#}

with base_cc_exposure as (
    select
        id,
        claimid,
        mbcd_ext,
        retired
    from {{ ref('v_cc_exposure_current') }}
    where retired = 0
),

base_cc_claim as (
    select
        id,
        claimnumber,
        source_system,
        claimworkcompid,
        reporteddate,
        state,
        managingentity_icare,
        segment,
        assignedgroupid,
        assigneduserid,
        lossdate,
        closedate_icare,
        reopendate,
        reopenedreason,
        retired
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

base_cctl_claimstate as (
    select
        id,
        description,
        retired
    from {{ ref('v_cctl_claimstate_current') }}
    where retired = 0
),

base_cc_claimempdata as (
    select
        ownerid,
        foreignentityid
    from {{ ref('v_cc_claimempdata_current') }}
),

base_cc_employmentdata as (
    select
        id,
        retired
    from {{ ref('v_cc_employmentdata_current') }}
    where retired = 0
),

base_cc_workstatus as (
    select
        employmentdataid,
        status,
        statusdate,
        retired
    from {{ ref('v_cc_workstatus_current') }}
    where retired = 0
),

base_cctl_workcapacity as (
    select
        id,
        typecode,
        retired
    from {{ ref('v_cctl_workcapacity_current') }}
    where retired = 0
),

base_cc_workcomp as (
    select
        id,
        reasonableexcuse_icare,
        retired
    from {{ ref('v_cc_workcomp_current') }}
    where retired = 0
),

base_ccx_liabilitystatushist_icare as (
    select
        claimworkcompid,
        liabilitystatus,
        liabilitystatusdate,
        ctmliabilitystatusdecisiondate,
        createtime,
        retired
    from {{ ref('v_ccx_liabilitystatushist_icare_current') }}
    where retired = 0
),

base_cctl_compensabilitydecision as (
    select
        id,
        typecode,
        retired
    from {{ ref('v_cctl_compensabilitydecision_current') }}
    where retired = 0
),

base_cc_incident as (
    select
        id,
        claimid,
        claimincident,
        deceaseddate_icare,
        severity,
        retired
    from {{ ref('v_cc_incident_current') }}
    where retired = 0
),

base_cctl_severitytype as (
    select
        id,
        typecode,
        retired
    from {{ ref('v_cctl_severitytype_current') }}
    where retired = 0
),

base_cc_injurydiagnosis as (
    select
        id,
        injuryincidentid,
        icdcode,
        isprimary,
        retired
    from {{ ref('v_cc_injurydiagnosis_current') }}
    where retired = 0
),

base_cc_icdcode as (
    select
        id,
        code,
        retired
    from {{ ref('v_cc_icdcode_current') }}
    where retired = 0
),

base_cctl_reasonableexcuse_icare as (
    select
        id,
        typecode,
        retired
    from {{ ref('v_cctl_reasonableexcuse_icare_current') }}
    where retired = 0
),

base_cc_check as (
    select
        id,
        claimid,
        weeklybenefitpayeetype_icare,
        status,
        retired
    from {{ ref('v_cc_check_current') }}
    where retired = 0
),

base_cc_transaction as (
    select
        id,
        checkid,
        costcategory,
        retired
    from {{ ref('v_cc_transaction_current') }}
    where retired = 0
),

base_cctl_costcategory as (
    select
        id,
        typecode,
        retired
    from {{ ref('v_cctl_costcategory_current') }}
    where retired = 0
),

base_cc_transactionlineitem as (
    select
        transactionid,
        transactionamount,
        dateto_icare,
        retired
    from {{ ref('v_cc_transactionlineitem_current') }}
    where retired = 0
),

base_ccx_wpiassessment_icare as (
    select
        id,
        exposureid,
        retired
    from {{ ref('v_ccx_wpiassessment_icare_current') }}
    where retired = 0
),

base_ccx_wpiassessrecord_icare as (
    select
        wpiassessment_icareid,
        wpiresult_icare,
        retired
    from {{ ref('v_ccx_wpiassessrecord_icare_current') }}
    where retired = 0
),

base_ccx_claimscreening_icare as (
    select
        claimid,
        createtime,
        retired
    from {{ ref('v_ccx_claimscreening_icare_current') }}
    where retired = 0
),

base_cc_document as (
    select
        id,
        claimid,
        type,
        datesentreceived_icare,
        retired
    from {{ ref('v_cc_document_current') }}
    where retired = 0
),

base_cctl_documenttype as (
    select
        id,
        typecode,
        retired
    from {{ ref('v_cctl_documenttype_current') }}
    where retired = 0
),

base_ccx_rehabplan_ext as (
    select
        id,
        claimid,
        claimsstrategygoal_icare,
        strategygoaldate_icare,
        impdocument_icareid,
        createtime,
        retired
    from {{ ref('v_ccx_rehabplan_ext_current') }}
    where retired = 0
),

base_cctl_claimsstrategygoal_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_claimsstrategygoal_icare_current') }}
),

base_ccx_managingentity_icare as (
    select
        id,
        publicid,
        code,
        retired
    from {{ ref('v_ccx_managingentity_icare_current') }}
    where retired = 0
),

base_cctl_claimsegment as (
    select
        id,
        name,
        retired
    from {{ ref('v_cctl_claimsegment_current') }}
    where retired = 0
),

base_cc_group as (
    select
        id,
        name
    from {{ ref('v_cc_group_current') }}
),

base_cc_user as (
    select
        id,
        contactid,
        retired
    from {{ ref('v_cc_user_current') }}
    where retired = 0
),

base_cc_contact as (
    select
        id,
        firstname,
        lastname,
        dateofbirth,
        retired
    from {{ ref('v_cc_contact_current') }}
    where retired = 0
),

base_cc_claimcontact as (
    select
        claimid,
        contactid,
        claimantflag
    from {{ ref('v_cc_claimcontact_current') }}
),

base_ccx_retirementageref_icare as (
    select
        retirementageinyears,
        dateofbirthfrom,
        dateofbirthto,
        retired
    from {{ ref('v_ccx_retirementageref_icare_current') }}
    where retired = 0
),

base_cctl_claimreopenedreason as (
    select
        id,
        typecode,
        name,
        retired
    from {{ ref('v_cctl_claimreopenedreason_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_exposure as (
    select
        xpr.claimid,
        clm.claimworkcompid,
        cast(xpr.mbcd_ext as date) as mbcd_ext,
        clm.claimnumber,
        cast(clm.reporteddate as date) as reportdt,
        clmsts.description as claim_status_desc
    from base_cc_exposure as xpr
    inner join base_cc_claim as clm
        on clm.id = xpr.claimid
    left join base_cctl_claimstate as clmsts
        on clmsts.id = clm.state
    where cast(xpr.mbcd_ext as date) between dateadd(month, -2, cast(getdate() as date)) and dateadd(month, 7, cast(getdate() as date))
),

cte_exposure02 as (
    select xpr.*
    from cte_exposure as xpr
    left join (
        select
            clm.claimid,
            wrkcap.typecode as work_status,
            row_number() over (partition by clm.claimid order by wrk.statusdate desc) as ws_rank
        from cte_exposure as clm
        inner join base_cc_claimempdata as clmemp
            on clmemp.ownerid = clm.claimid
        inner join base_cc_employmentdata as emp
            on emp.id = clmemp.foreignentityid
        inner join base_cc_workstatus as wrk
            on wrk.employmentdataid = emp.id
        inner join base_cctl_workcapacity as wrkcap
            on wrkcap.id = wrk.status
    ) as ws
        on ws.claimid = xpr.claimid
        and ws.ws_rank = 1
        and ws.work_status = 13
    where ws.claimid is null
),

cte_clm as (
    select a.*
    from (
        select
            xpr.claimid,
            xpr.claimnumber,
            xpr.mbcd_ext,
            xpr.reportdt,
            xpr.claim_status_desc,
            liabsts.typecode as liabstatuscd,
            icd.code as icdcode,
            exc.typecode as reasonableexcuse,
            liab.liabilitystatusdate,
            row_number() over (partition by xpr.claimid order by liab.ctmliabilitystatusdecisiondate desc, liab.createtime desc) as latestliabstatusrank
        from cte_exposure02 as xpr
        inner join base_cc_workcomp as wc
            on wc.id = xpr.claimworkcompid
        inner join base_ccx_liabilitystatushist_icare as liab
            on liab.claimworkcompid = xpr.claimworkcompid
        inner join base_cctl_compensabilitydecision as liabsts
            on liabsts.id = liab.liabilitystatus
        inner join base_cc_incident as inc
            on inc.claimid = xpr.claimid
            and inc.claimincident = 1
            and inc.deceaseddate_icare is null
        left join base_cctl_severitytype as sev
            on sev.id = inc.severity
        left join base_cc_injurydiagnosis as inj
            on inj.injuryincidentid = inc.id
            and inj.isprimary = 1
        left join base_cc_icdcode as icd
            on icd.id = inj.icdcode
        left join base_cctl_reasonableexcuse_icare as exc
            on exc.id = wc.reasonableexcuse_icare
        where coalesce(sev.typecode, ' ') <> '1'
            and coalesce(icd.code, ' ') not in ('H90', 'H90.0', 'H90.1', 'H90.2', 'H90.03', 'H90.4', 'H90.5', 'H90.6', 'H90.7', 'H90.8', 'H91', 'H91.1', 'H91.8', 'H91.9')
    ) as a
    where a.latestliabstatusrank = 1
        and not (a.liabstatuscd in ('01', '05', '06', '07', '12') or (a.liabstatuscd = '09' and a.reasonableexcuse = '08'))
),

cte_last_pymt as (
    select
        clm.claimid,
        cast(max(ln.dateto_icare) as date) as wkly_benf_paid_to_dt
    from cte_clm as clm
    inner join base_cc_check as chq
        on chq.claimid = clm.claimid
        and chq.weeklybenefitpayeetype_icare is not null
        and chq.status in (5, 2, 14)
    inner join base_cc_transaction as trn
        on trn.checkid = chq.id
    inner join base_cctl_costcategory as cstcatg
        on cstcatg.id = trn.costcategory
        and cstcatg.typecode = '50'
    inner join base_cc_transactionlineitem as ln
        on ln.transactionid = trn.id
        and ln.transactionamount > 0
    group by clm.claimid
),

cte_wpi as (
    select
        clm.claimid,
        max(asrcd.wpiresult_icare) as wpi
    from cte_clm as clm
    inner join base_cc_exposure as xpr
        on xpr.claimid = clm.claimid
    inner join base_ccx_wpiassessment_icare as asmt
        on asmt.exposureid = xpr.id
    inner join base_ccx_wpiassessrecord_icare as asrcd
        on asrcd.wpiassessment_icareid = asmt.id
        and coalesce(asrcd.wpiresult_icare, 0) >= 11
    group by clm.claimid
),

cte_clmreviewdt as (
    select
        clm.claimid,
        cast(max(clmscrn.createtime) as date) as lastreviewdt
    from cte_clm as clm
    inner join base_ccx_claimscreening_icare as clmscrn
        on clmscrn.claimid = clm.claimid
    group by clm.claimid
),

cte_lettersent as (
    select
        clm.claimid,
        max(case when cast(doc.datesentreceived_icare as date) between dateadd(month, -7, clm.mbcd_ext) and dateadd(day, -1, dateadd(week, -26, clm.mbcd_ext))
                      and doctyp.typecode in ('WC914', 'WC915')
             then cast(doc.datesentreceived_icare as date)
             else null
        end) as wc914_6month_send_dt,
        min(case when doctyp.typecode in ('WC914', 'WC915') then cast(doc.datesentreceived_icare as date)
             else null
        end) as wc914_earliest_send_dt,
        max(case when cast(doc.datesentreceived_icare as date) between dateadd(month, -7, clm.mbcd_ext) and dateadd(day, -1, dateadd(week, -26, clm.mbcd_ext))
                      and doctyp.typecode = 'WC917'
             then cast(doc.datesentreceived_icare as date)
             else null
        end) as wc917_6month_send_dt,
        min(case when doctyp.typecode = 'WC917' then cast(doc.datesentreceived_icare as date)
             else null
        end) as wc917_earliest_send_dt,
        max(case when cast(doc.datesentreceived_icare as date) between dateadd(week, -26, clm.mbcd_ext) and dateadd(week, -13, clm.mbcd_ext)
                      and doctyp.typecode in ('WC914', 'WC915')
             then cast(doc.datesentreceived_icare as date)
             else null
        end) as wc914_13week_send_dt,
        min(case when cast(doc.datesentreceived_icare as date) > dateadd(week, -13, clm.mbcd_ext)
                      and doctyp.typecode in ('WC914', 'WC915') then cast(doc.datesentreceived_icare as date)
             else null
        end) as wc914_late_send_dt,
        max(case when cast(doc.datesentreceived_icare as date) between dateadd(week, -26, clm.mbcd_ext) and dateadd(week, -13, clm.mbcd_ext)
                      and doctyp.typecode = 'WC917'
             then cast(doc.datesentreceived_icare as date)
             else null
        end) as wc917_13week_send_dt,
        min(case when cast(doc.datesentreceived_icare as date) > dateadd(week, -13, clm.mbcd_ext)
                      and doctyp.typecode = 'WC917' then cast(doc.datesentreceived_icare as date)
             else null
        end) as wc917_late_send_dt,
        max(case when doctyp.typecode in ('WC900', 'WC960') then cast(doc.datesentreceived_icare as date)
             else null
        end) as finalisation_letter_send_dt,
        max(case when doctyp.typecode in ('WC902', 'WC962') then cast(doc.datesentreceived_icare as date)
             else null
        end) as provider_final_letter_send_dt
    from cte_clm as clm
    inner join base_cc_document as doc
        on doc.claimid = clm.claimid
    inner join base_cctl_documenttype as doctyp
        on doctyp.id = doc.type
        and doctyp.typecode in ('WC914', 'WC915', 'WC917', 'WC900', 'WC960', 'WC902', 'WC962')
    where doctyp.typecode in ('WC917', 'WC900', 'WC960', 'WC902', 'WC962')
        or (doctyp.typecode = 'WC914' and doc.datesentreceived_icare < '2021-04-09 00:00:00.000')
        or (doctyp.typecode = 'WC915' and doc.datesentreceived_icare >= '2021-04-09 00:00:00.000')
    group by clm.claimid
),

cte_rehab as (
    select *
    from (
        select
            clm.claimid,
            rehb.id as src_rehab_plan_id,
            dimstrat.name as rtw_strategy_goal_desc,
            cast(rehb.strategygoaldate_icare as date) as strategy_goal_dt,
            cast(doc.datesentreceived_icare as date) as imp_document_dt,
            row_number() over (partition by clm.claimid
                order by rehb.createtime desc, coalesce(rehb.strategygoaldate_icare, '1900-01-01') desc) as latest_rehab_plan
        from cte_clm as clm
        inner join base_ccx_rehabplan_ext as rehb
            on rehb.claimid = clm.claimid
        left join base_cctl_claimsstrategygoal_icare as dimstrat
            on dimstrat.id = rehb.claimsstrategygoal_icare
        left join base_cc_document as doc
            on doc.id = rehb.impdocument_icareid
    ) as a
    where a.latest_rehab_plan = 1
),

cte_join as (
    select
        cast({{ dbt_utils.generate_surrogate_key(['clm.source_system', 'clm.claimnumber']) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.managingentity_icare as managing_entity_id,
        cast({{ dbt_utils.generate_surrogate_key(['clm.source_system', 'mge.publicid']) }} as varchar(150)) as managing_entity_sk,
        coalesce(mge.code, 'NI_ICARE') as managing_entity_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        seg.name as segment,
        team.name as team,
        concat(cttownr.firstname, ' ', cttownr.lastname) as case_owner_name,
        cast(clm.reporteddate as date) as report_dt,
        py.wkly_benf_paid_to_dt as service_period_end_dt,
        mbcd.claim_status_desc,
        cast(clm.closedate_icare as date) as close_dt,
        cast(clm.reopendate as date) as reopen_dt,
        opnrsn.typecode as reopen_reason_cd,
        opnrsn.name as reopen_reason,
        wpi.wpi as wpi_percent,
        case when left(mbcd.icdcode, 1) = 'F' then 'Y' else 'N' end as primary_psych_ind,
        rvw.lastreviewdt as last_review_dt,
        mbcd.mbcd_ext as medical_benefit_cessation_dt,
        dateadd(month, -6, mbcd.mbcd_ext) as six_month_notice_dt,
        dateadd(week, -13, mbcd.mbcd_ext) as thirteen_weeks_notice_dt,
        letr.wc914_6month_send_dt,
        letr.wc917_6month_send_dt,
        letr.wc914_13week_send_dt,
        letr.wc917_13week_send_dt,
        case when letr.wc914_13week_send_dt is null then letr.wc914_late_send_dt else null end as wc914_late_send_dt,
        case when letr.wc917_13week_send_dt is null then letr.wc917_late_send_dt else null end as wc917_late_send_dt,
        letr.finalisation_letter_send_dt,
        letr.provider_final_letter_send_dt,
        rhb.rtw_strategy_goal_desc,
        rhb.strategy_goal_dt,
        rhb.imp_document_dt,
        cast(clm.lossdate as date) as doi,
        dateadd(month, (ret.retirementageinyears * 12) + 12, cast(clmnt.dateofbirth as date)) as retirement_date_plus_1_yr
    from cte_clm as mbcd
    inner join base_cc_claim as clm
        on clm.id = mbcd.claimid
    left join cte_last_pymt as py
        on py.claimid = clm.id
    left join cte_wpi as wpi
        on wpi.claimid = clm.id
    left join base_ccx_managingentity_icare as mge
        on mge.id = clm.managingentity_icare
    left join base_cctl_claimsegment as seg
        on seg.id = clm.segment
    left join base_cc_group as team
        on team.id = clm.assignedgroupid
    left join base_cc_user as csownr
        on csownr.id = clm.assigneduserid
    left join base_cc_contact as cttownr
        on cttownr.id = csownr.contactid
    left join cte_clmreviewdt as rvw
        on rvw.claimid = mbcd.claimid
    left join cte_lettersent as letr
        on letr.claimid = mbcd.claimid
    left join base_cc_claimcontact as cc
        on cc.claimid = clm.id
        and cc.claimantflag = 1
    left join base_cc_contact as clmnt
        on clmnt.id = cc.contactid
    left join base_ccx_retirementageref_icare as ret
        on cast(clmnt.dateofbirth as date) between ret.dateofbirthfrom and coalesce(ret.dateofbirthto, '2999-12-31')
    left join base_cctl_claimreopenedreason as opnrsn
        on opnrsn.id = clm.reopenedreason
    left join cte_rehab as rhb
        on rhb.claimid = mbcd.claimid
)

select
    claim_sk,
    src_system_cd,
    managing_entity_id,
    managing_entity_sk,
    managing_entity_cd,
    claim_nbr,
    src_claim_id,
    segment,
    team,
    case_owner_name,
    report_dt,
    service_period_end_dt,
    claim_status_desc,
    close_dt,
    reopen_dt,
    reopen_reason_cd,
    reopen_reason,
    wpi_percent,
    primary_psych_ind,
    last_review_dt,
    medical_benefit_cessation_dt,
    six_month_notice_dt,
    thirteen_weeks_notice_dt,
    wc914_6month_send_dt,
    wc917_6month_send_dt,
    wc914_13week_send_dt,
    wc917_13week_send_dt,
    wc914_late_send_dt,
    wc917_late_send_dt,
    finalisation_letter_send_dt,
    provider_final_letter_send_dt,
    rtw_strategy_goal_desc,
    strategy_goal_dt,
    imp_document_dt,
    doi,
    retirement_date_plus_1_yr
from cte_join
