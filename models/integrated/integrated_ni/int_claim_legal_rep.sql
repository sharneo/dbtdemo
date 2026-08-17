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
    unique_key='claim_legal_rep_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: L03_CLAIM_LEGAL_REP.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_L03
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_LEGAL_REP
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp > (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

base_cc_matter as (
    select
        id,
        claimid,
        casenumber,
        createtime
    from {{ ref('v_cc_matter_current') }}
    where retired = 0
),

base_cc_claimcontactrole as (
    select
        id,
        matterid,
        claimcontactid,
        role
    from {{ ref('v_cc_claimcontactrole_current') }}
    where retired = 0
),

base_cctl_contactrole as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_contactrole_current') }}
    where retired = 0
),

base_cc_claimcontact as (
    select
        id,
        personfirstnamedenorm,
        personlastnamedenorm,
        contactnamedenorm
    from {{ ref('v_cc_claimcontact_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_applicant_solicitor as (
    select
        ccr.matterid,
        ccr.claimcontactid,
        concat(cccon.personfirstnamedenorm, ' ', cccon.personlastnamedenorm) as applicant_solicitor
    from base_cc_claimcontactrole as ccr
    inner join base_cctl_contactrole as ccrtl
        on ccr.role = ccrtl.id
        and ccrtl.typecode = 'plaintiffatt'
    inner join base_cc_claimcontact as cccon
        on cccon.id = ccr.claimcontactid
),

cte_respondent_solicitor as (
    select
        ccr.matterid,
        ccr.claimcontactid,
        concat(cccon.personfirstnamedenorm, ' ', cccon.personlastnamedenorm) as respondent_solicitor
    from base_cc_claimcontactrole as ccr
    inner join base_cctl_contactrole as ccrtl
        on ccr.role = ccrtl.id
        and ccrtl.typecode = 'defenseattorney'
    inner join base_cc_claimcontact as cccon
        on cccon.id = ccr.claimcontactid
),

cte_applicant_lawfirm as (
    select
        ccr.matterid,
        ccr.claimcontactid,
        cccon.contactnamedenorm as applcntlawfirm
    from base_cc_claimcontactrole as ccr
    inner join base_cctl_contactrole as ccrtl
        on ccr.role = ccrtl.id
        and ccrtl.typecode = 'plaintifffirm'
    inner join base_cc_claimcontact as cccon
        on cccon.id = ccr.claimcontactid
),

cte_respondent_lawfirm as (
    select
        ccr.matterid,
        ccr.claimcontactid,
        cccon.contactnamedenorm as responsdentlawfirm
    from base_cc_claimcontactrole as ccr
    inner join base_cctl_contactrole as ccrtl
        on ccr.role = ccrtl.id
        and ccrtl.typecode = 'defensefirm'
    inner join base_cc_claimcontact as cccon
        on cccon.id = ccr.claimcontactid
),

cte_main as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber',
            'm.casenumber'
        ]) }} as varchar(150)) as claim_legal_rep_sk,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        m.casenumber as matternumber,
        aplcnt.claimcontactid as applcnt_solctor_contctid,
        aplcnt.applicant_solicitor,
        aplntfrm.claimcontactid as applicnt_lawfrm_contctid,
        aplntfrm.applcntlawfirm as applicant_law_firm,
        rspdnt.claimcontactid as rspndt_solctor_contctid,
        rspdnt.respondent_solicitor,
        rspdntfrm.claimcontactid as rspdnt_lawfrm_contctid,
        rspdntfrm.responsdentlawfirm as respondent_law_firm,
        case
            when row_number() over (partition by m.casenumber order by m.createtime desc) = 1 then 'Y'
            else 'N'
        end as latest_record_ind,
        clm.file_ingestion_timestamp
    from base_cc_claim as clm
    inner join base_cc_matter as m
        on clm.id = m.claimid
    left join cte_applicant_solicitor as aplcnt
        on aplcnt.matterid = m.id
    left join cte_respondent_solicitor as rspdnt
        on rspdnt.matterid = m.id
    left join cte_applicant_lawfirm as aplntfrm
        on aplntfrm.matterid = m.id
    left join cte_respondent_lawfirm as rspdntfrm
        on rspdntfrm.matterid = m.id
)

select
    claim_legal_rep_sk,
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    matternumber,
    applcnt_solctor_contctid,
    applicant_solicitor,
    applicnt_lawfrm_contctid,
    applicant_law_firm,
    rspndt_solctor_contctid,
    respondent_solicitor,
    rspdnt_lawfrm_contctid,
    respondent_law_firm,
    latest_record_ind,
    file_ingestion_timestamp
from cte_main
where latest_record_ind = 'Y'
