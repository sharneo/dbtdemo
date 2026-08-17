{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Aspire - original table materialization
2026-07-13      1.0                             Converted to incremental with merge strategy

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
  Source: S02_SECTION_59A_EXCEPTION.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_S02
  TBL_NM: MSC_QLK_ASPIRE_S02_SECTION_59A_EXCEPTION
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        claimworkcompid,
        state,
        managingentity_icare,
        segment,
        assignedgroupid,
        assigneduserid,
        reporteddate,
        lossdate,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp > (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

base_cctl_claimstate as (
    select
        id,
        typecode,
        description
    from {{ ref('v_cctl_claimstate_current') }}
    where retired = 0
      and typecode = 'open'
),

base_cc_workcomp as (
    select
        id,
        reasonableexcuse_icare
    from {{ ref('v_cc_workcomp_current') }}
    where retired = 0
),

base_ccx_liabilitystatushist_icare as (
    select
        id,
        claimworkcompid,
        liabilitystatus,
        liabilitystatusdate,
        ctmliabilitystatusdecisiondate,
        createtime
    from {{ ref('v_ccx_liabilitystatushist_icare_current') }}
    where retired = 0
),

base_cctl_compensabilitydecision as (
    select
        id,
        typecode
    from {{ ref('v_cctl_compensabilitydecision_current') }}
    where retired = 0
),

base_cc_incident as (
    select
        id,
        claimid,
        claimincident,
        deceaseddate_icare
    from {{ ref('v_cc_incident_current') }}
    where retired = 0
      and claimincident = 1
      and deceaseddate_icare is null
),

base_cctl_reasonableexcuse_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_reasonableexcuse_icare_current') }}
    where retired = 0
),

base_cc_exposure as (
    select
        id,
        claimid,
        mbcd_ext
    from {{ ref('v_cc_exposure_current') }}
    where retired = 0
      and mbcd_ext is not null
),

base_cc_activity as (
    select
        id,
        claimid,
        activitypatternid,
        targetdate,
        closedate
    from {{ ref('v_cc_activity_current') }}
    where retired = 0
),

base_cc_activitypattern as (
    select
        id,
        code
    from {{ ref('v_cc_activitypattern_current') }}
    where retired = 0
      and code in ('s59_review', 'S59a_medical_cessation')
),

base_cc_document as (
    select
        id,
        claimid,
        type,
        datesentreceived_icare
    from {{ ref('v_cc_document_current') }}
    where retired = 0
),

base_cctl_documenttype as (
    select
        id,
        typecode
    from {{ ref('v_cctl_documenttype_current') }}
    where retired = 0
      and (typecode = 'WC917'
           or (typecode = 'WC914')
           or (typecode = 'WC915'))
),

base_cc_check as (
    select
        id,
        claimid,
        weeklybenefitpayeetype_icare,
        status
    from {{ ref('v_cc_check_current') }}
    where retired = 0
      and weeklybenefitpayeetype_icare is not null
      and status in (5, 2, 14)
),

base_cc_transaction as (
    select
        id,
        checkid,
        costcategory
    from {{ ref('v_cc_transaction_current') }}
    where retired = 0
),

base_cctl_costcategory as (
    select
        id,
        typecode
    from {{ ref('v_cctl_costcategory_current') }}
    where retired = 0
      and typecode = '50'
),

base_cc_transactionlineitem as (
    select
        id,
        transactionid,
        dateto_icare
    from {{ ref('v_cc_transactionlineitem_current') }}
    where retired = 0
),

base_ccx_wpiassessment_icare as (
    select
        id,
        exposureid
    from {{ ref('v_ccx_wpiassessment_icare_current') }}
    where retired = 0
),

base_ccx_wpiassessrecord_icare as (
    select
        id,
        wpiassessment_icareid,
        wpiresult_icare
    from {{ ref('v_ccx_wpiassessrecord_icare_current') }}
    where retired = 0
      and coalesce(wpiresult_icare, 0) >= 11
),

base_ccx_managingentity_icare as (
    select
        id,
        publicid,
        code
    from {{ ref('v_ccx_managingentity_icare_current') }}
    where retired = 0
),

base_cctl_claimsegment as (
    select
        id,
        name
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
        contactid
    from {{ ref('v_cc_user_current') }}
    where retired = 0
),

base_cc_contact as (
    select
        id,
        firstname,
        lastname
    from {{ ref('v_cc_contact_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_mbcd as (
    select
        a.*,
        cast(xpr.mbcd_ext as date) as mbcd
    from (
        select
            clm.id as claimid,
            clm.claimnumber,
            exc.typecode as reasonableexcuse,
            liab.liabilitystatusdate,
            liabsts.typecode as liabstatuscd,
            clmsts.description as claim_status_desc,
            row_number() over (
                partition by clm.id
                order by liab.ctmliabilitystatusdecisiondate desc, liab.createtime desc
            ) as latestliabstatusrank
        from base_cc_claim as clm
        inner join base_cctl_claimstate as clmsts
            on clmsts.id = clm.state
        inner join base_cc_workcomp as wc
            on wc.id = clm.claimworkcompid
        inner join base_ccx_liabilitystatushist_icare as liab
            on liab.claimworkcompid = clm.claimworkcompid
        inner join base_cctl_compensabilitydecision as liabsts
            on liabsts.id = liab.liabilitystatus
        inner join base_cc_incident as inc
            on inc.claimid = clm.id
        left join base_cctl_reasonableexcuse_icare as exc
            on exc.id = wc.reasonableexcuse_icare
    ) as a
    inner join base_cc_exposure as xpr
        on xpr.claimid = a.claimid
    where a.latestliabstatusrank = 1
        and not (a.liabstatuscd in ('01', '05', '06', '07', '12')
                 or (a.liabstatuscd = '09' and a.reasonableexcuse = '08'))
),

cte_exceptions as (
    select distinct
        mbcd.claimid,
        mbcd.mbcd,
        case
            when patt.code = 's59_review' then '6 MONTH EXCEPTION'
            else '13 WEEK EXCEPTION'
        end as exception_type,
        cast(actv.targetdate as date) as activity_target_dt
    from cte_mbcd as mbcd
    inner join base_cc_activity as actv
        on actv.claimid = mbcd.claimid
    inner join base_cc_activitypattern as patt
        on patt.id = actv.activitypatternid
    left join base_cc_document as doc
        on doc.claimid = mbcd.claimid
    left join base_cctl_documenttype as doctyp
        on doctyp.id = doc.type
        and (doctyp.typecode = 'WC917'
             or (doctyp.typecode = 'WC914' and doc.datesentreceived_icare < '2021-04-09 00:00:00.000')
             or (doctyp.typecode = 'WC915' and doc.datesentreceived_icare >= '2021-04-09 00:00:00.000'))
    where cast(actv.targetdate as date) > dateadd(month, -6, mbcd.mbcd)
        and ((actv.closedate is not null and doc.id is null)
             or (actv.closedate is null and cast(actv.targetdate as date) < current_date()))
),

cte_last_pymt as (
    select
        clm.claimid,
        cast(max(ln.dateto_icare) as date) as wkly_benf_paid_to_dt
    from cte_exceptions as clm
    inner join base_cc_check as chq
        on chq.claimid = clm.claimid
    inner join base_cc_transaction as trn
        on trn.checkid = chq.id
    inner join base_cctl_costcategory as cstcatg
        on cstcatg.id = trn.costcategory
    inner join base_cc_transactionlineitem as ln
        on ln.transactionid = trn.id
    group by clm.claimid
),

cte_wpi as (
    select
        clm.claimid,
        max(asrcd.wpiresult_icare) as wpi
    from cte_exceptions as clm
    inner join base_cc_exposure as xpr
        on xpr.claimid = clm.claimid
    inner join base_ccx_wpiassessment_icare as asmt
        on asmt.exposureid = xpr.id
    inner join base_ccx_wpiassessrecord_icare as asrcd
        on asrcd.wpiassessment_icareid = asmt.id
    group by clm.claimid
),

cte_final as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.managingentity_icare as managing_entity_id,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'mge.publicid'
        ]) }} as varchar(150)) as managing_entity_sk,
        coalesce(mge.code, 'NI_ICARE') as managing_entity_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        excp.exception_type,
        cast(clm.reporteddate as date) as claim_reported_dt,
        py.wkly_benf_paid_to_dt as service_period_end_dt,
        wpi.wpi as wpi_percent,
        excp.mbcd as med_benefit_cessation_dt,
        dateadd(month, -6, excp.mbcd) as six_month_notice_dt,
        dateadd(week, -13, excp.mbcd) as thirteen_weeks_notice_dt,
        CAST(clm.lossdate AS TIMESTAMP_NTZ) as doi,
        seg.name as segment,
        team.name as team,
        concat(cttownr.firstname, ' ', cttownr.lastname) as case_owner_name,
        excp.activity_target_dt,
        clm.file_ingestion_timestamp
    from cte_exceptions as excp
    inner join base_cc_claim as clm
        on clm.id = excp.claimid
    left join cte_last_pymt as py
        on py.claimid = excp.claimid
    left join cte_wpi as wpi
        on wpi.claimid = excp.claimid
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
)

select
    claim_sk,
    src_system_cd,
    managing_entity_id,
    managing_entity_sk,
    managing_entity_cd,
    claim_nbr,
    src_claim_id,
    exception_type,
    claim_reported_dt,
    service_period_end_dt,
    wpi_percent,
    med_benefit_cessation_dt,
    six_month_notice_dt,
    thirteen_weeks_notice_dt,
    doi,
    segment,
    team,
    case_owner_name,
    activity_target_dt,
    file_ingestion_timestamp
from cte_final
qualify row_number() over (partition by claim_sk order by file_ingestion_timestamp desc) = 1
