{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Aspire - original table materialization
2026-07-13      1.0                             Converted to incremental with merge strategy
                                                NOTE: Original SAS uses multiple PROC SQL steps and DATA steps.
                                                Restructured as CTEs for Snowflake/dbt compatibility.

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
  Source: W03_ODG_TREATMENT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_W03
  TBL_NM: MSC_QLK_ASPIRE_ODG_TREATMENT
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        managingentity_icare,
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

base_ccx_medpersontreatment_icare as (
    select
        id,
        claimid,
        paycodeid,
        approvalstatus,
        odgflag,
        icd1id,
        odgmax,
        treatmentquantityrequested,
        treatmentquantityapproved,
        requestdate,
        dateapproved,
        reviewrequiredreason,
        treatmenttype
    from {{ ref('v_ccx_medpersontreatment_icare_current') }}
    where retired = 0
      and requestdate is not null
),

base_ccx_managingentity_icare as (
    select
        id,
        code
    from {{ ref('v_ccx_managingentity_icare_current') }}
),

base_ccx_paycode_icare as (
    select
        id,
        paycode,
        paymentsubtype
    from {{ ref('v_ccx_paycode_icare_current') }}
),

base_cctl_approvalstatus_icare as (
    select
        id,
        description
    from {{ ref('v_cctl_approvalstatus_icare_current') }}
),

base_cctl_odgflag_icare as (
    select
        id,
        description
    from {{ ref('v_cctl_odgflag_icare_current') }}
),

base_cc_icdcode as (
    select
        id,
        code,
        codedesc
    from {{ ref('v_cc_icdcode_current') }}
),

base_cc_incident as (
    select
        claimid,
        odgduration_icare
    from {{ ref('v_cc_incident_current') }}
    where odgduration_icare is not null
),

base_cc_employmentdata as (
    select
        id,
        claimid
    from {{ ref('v_cc_employmentdata_current') }}
),

base_cc_workstatus as (
    select
        id,
        employmentdataid,
        status,
        statusdate
    from {{ ref('v_cc_workstatus_current') }}
),

base_cctl_workcapacity as (
    select
        id,
        typecode,
        description
    from {{ ref('v_cctl_workcapacity_current') }}
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_latest_wsc as (
    select
        clm.claimnumber,
        d.description as current_work_status,
        row_number() over (partition by clm.claimnumber order by c.statusdate desc) as rn
    from base_cc_claim as clm
    left join base_cc_employmentdata as b
        on clm.id = b.claimid
    left join base_cc_workstatus as c
        on b.id = c.employmentdataid
    inner join base_cctl_workcapacity as d
        on c.status = d.id
),

cte_incident_data as (
    select
        clm.claimnumber,
        max(inc.odgduration_icare) as odg_rtw
    from base_cc_claim as clm
    left join base_cc_incident as inc
        on clm.id = inc.claimid
    group by clm.claimnumber
),

final as (
    select distinct
        cast({{ dbt_utils.generate_surrogate_key([
            'a.source_system',
            'a.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        a.source_system as src_system_cd,
        a.claimnumber as claim_nbr,
        ent.code as managing_entity,
        cast(a.reporteddate as date) as notification_date,
        cast(a.lossdate as date) as injury_date,
        g.code as icd_code,
        g.codedesc as icd_description,
        inc.odg_rtw,
        case when b.odgmax is null then 'N' else 'Y' end as odg_utilised,
        c.paycode,
        b.odgmax as odg_max,
        b.treatmentquantityrequested,
        b.treatmentquantityapproved,
        e.description as odg_flag,
        cast(b.requestdate as date) as requestdate,
        cast(b.dateapproved as date) as dateapproved,
        datediff(day, cast(b.requestdate as date), cast(b.dateapproved as date)) as approval_delay,
        d.description as approvalstatus,
        b.reviewrequiredreason as review_required_reason,
        b.treatmenttype,
        c.paymentsubtype,
        wsc.current_work_status,
        a.file_ingestion_timestamp
    from base_cc_claim as a
    left join base_ccx_medpersontreatment_icare as b
        on a.id = b.claimid
    left join base_ccx_managingentity_icare as ent
        on a.managingentity_icare = ent.id
    left join base_ccx_paycode_icare as c
        on b.paycodeid = c.id
    left join base_cctl_approvalstatus_icare as d
        on b.approvalstatus = d.id
    left join base_cctl_odgflag_icare as e
        on b.odgflag = e.id
    left join base_cc_icdcode as g
        on b.icd1id = g.id
    left join cte_latest_wsc as wsc
        on a.claimnumber = wsc.claimnumber
        and wsc.rn = 1
    left join cte_incident_data as inc
        on a.claimnumber = inc.claimnumber
)

select
    claim_sk,
    src_system_cd,
    claim_nbr,
    managing_entity,
    notification_date,
    injury_date,
    icd_code,
    icd_description,
    odg_rtw,
    odg_utilised,
    paycode,
    odg_max,
    treatmentquantityrequested,
    treatmentquantityapproved,
    odg_flag,
    requestdate,
    dateapproved,
    approval_delay,
    approvalstatus,
    review_required_reason,
    treatmenttype,
    paymentsubtype,
    current_work_status,
    file_ingestion_timestamp
from final
