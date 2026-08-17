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
    unique_key='claim_triage_history_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 13_CLAIM_TRIAGE.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A13
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_TRIAGE
-#}

with base_cc_incident as (
    select
        id,
        claimid,
        claimincident,
        subtype,
        retired
    from {{ ref('v_cc_incident_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

base_cc_injurydiagnosis as (
    select
        id,
        injuryincidentid,
        icdcode,
        createtime
    from {{ ref('v_cc_injurydiagnosis_current') }}
),

base_cc_icdcode as (
    select
        id,
        code
    from {{ ref('v_cc_icdcode_current') }}
),

base_ccx_triagehistory_icare as (
    select
        id,
        claimid,
        publicid,
        createtime,
        updatetime,
        triagedate,
        datereviewed,
        proposedsegment,
        currentsegment,
        source,
        segmentreason,
        outcome,
        locationofinjurytoocs,
        natureofinjurytoocs,
        riskscore,
        comments,
        icdcode,
        userid,
        retired
    from {{ ref('v_ccx_triagehistory_icare_current') }}
    where retired = 0
),

base_cc_claim as (
    select
        id,
        claimnumber,
        source_system,
        retired
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

base_cctl_claimsegment as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_claimsegment_current') }}
),

base_cctl_triagesource_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_triagesource_icare_current') }}
),

base_cctl_segmentreason_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_segmentreason_icare_current') }}
),

base_cctl_triagereviewoutcome_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_triagereviewoutcome_icare_current') }}
),

base_cc_user as (
    select
        id,
        contactid
    from {{ ref('v_cc_user_current') }}
),

base_cc_contact as (
    select
        id,
        firstname,
        lastname
    from {{ ref('v_cc_contact_current') }}
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_icd as (
    select
        inc.claimid,
        inj.row_eff_dttm,
        inj.row_exp_dttm,
        icd.code
    from base_cc_incident as inc
    inner join (
        select
            *,
            createtime as row_eff_dttm,
            coalesce(
                dateadd('microsecond', -1, lead(createtime) over (partition by injuryincidentid order by createtime asc)),
                cast('9999-12-31' as timestamp_ntz)
            ) as row_exp_dttm
        from base_cc_injurydiagnosis
    ) as inj
        on inc.id = inj.injuryincidentid
    inner join base_cc_icdcode as icd
        on inj.icdcode = icd.id
    where inc.claimincident = 1
        and inc.subtype = 5
),

cte_triage as (
    select
        cast({{ dbt_utils.generate_surrogate_key(['clm.source_system', 'clm.claimnumber']) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        cast({{ dbt_utils.generate_surrogate_key(['clm.source_system', 'tri.publicid']) }} as varchar(150)) as triage_history_sk,
        tri.publicid as triage_history_id,
        tri.id as src_triage_history_id,
        tri.createtime as src_create_dttm,
        cast(tri.createtime as date) as src_create_dt,
        tri.updatetime as src_eff_dttm,
        cast(tri.updatetime as date) as src_eff_dt,
        tri.triagedate as triage_dttm,
        cast(tri.triagedate as date) as triage_dt,
        tri.datereviewed as triage_review_dttm,
        cast(tri.datereviewed as date) as triage_review_dt,
        prpseg.typecode as proposed_segment_cd,
        prpseg.name as proposed_segment_name,
        currseg.typecode as current_segment_cd,
        currseg.name as current_segment_name,
        src.typecode as triage_source_cd,
        src.name as triage_source_name,
        segrsn.typecode as segment_reason_cd,
        segrsn.name as segment_reason_name,
        revout.typecode as triage_review_outcome_cd,
        revout.name as triage_review_outcome_name,
        tri.locationofinjurytoocs as location_of_injury_toocs_txt,
        tri.natureofinjurytoocs as nature_of_injury_toocs_txt,
        tri.riskscore as risk_score,
        tri.comments as triage_comments_txt,
        tri.icdcode as icd_code_txt,
        icd.code as inj_icd_code_txt,
        case
            when currseg.name is null and prpseg.name = 'Unassigned' then 'N'
            when prpseg.name is null then 'N'
            when currseg.name = prpseg.name then 'N'
            when coalesce(revout.typecode, 'not_reviewed') <> 'accept' then 'N'
            else 'Y'
        end as valid_triage_ind,
        case
            when prpseg.name is null then 'N'
            else 'Y'
        end as valid_proposed_segment_ind,
        concat(ctt.firstname, ' ', ctt.lastname) as reviewer
    from base_ccx_triagehistory_icare as tri
    inner join base_cc_claim as clm
        on tri.claimid = clm.id
    left join cte_icd as icd
        on clm.id = icd.claimid
        and tri.createtime between icd.row_eff_dttm and icd.row_exp_dttm
    left join base_cctl_claimsegment as prpseg
        on tri.proposedsegment = prpseg.id
    left join base_cctl_claimsegment as currseg
        on tri.currentsegment = currseg.id
    left join base_cctl_triagesource_icare as src
        on tri.source = src.id
    left join base_cctl_segmentreason_icare as segrsn
        on tri.segmentreason = segrsn.id
    left join base_cctl_triagereviewoutcome_icare as revout
        on tri.outcome = revout.id
    left join base_cc_user as usr
        on usr.id = tri.userid
    left join base_cc_contact as ctt
        on ctt.id = usr.contactid
    qualify row_number() over (partition by tri.id order by tri.createtime) = 1
),

cte_join as (
    select
        subtri.claim_sk,
        subtri.src_system_cd,
        subtri.claim_nbr,
        subtri.src_claim_id,
        subtri.triage_history_sk,
        subtri.triage_history_id,
        subtri.src_triage_history_id,
        subtri.src_create_dttm,
        subtri.src_create_dt,
        subtri.src_eff_dttm,
        subtri.src_eff_dt,
        subtri.triage_dttm,
        subtri.triage_dt,
        subtri.triage_review_dttm,
        subtri.triage_review_dt,
        subtri.proposed_segment_cd,
        subtri.proposed_segment_name,
        subtri.current_segment_cd,
        subtri.current_segment_name,
        case
            when subtri.triage_review_outcome_cd = 'accept' then subtri.proposed_segment_cd
            else subtri.current_segment_cd
        end as accepted_segment_cd,
        case
            when subtri.triage_review_outcome_cd = 'accept' then subtri.proposed_segment_name
            else subtri.current_segment_name
        end as accepted_segment_name,
        subtri.triage_source_cd,
        subtri.triage_source_name,
        subtri.segment_reason_cd,
        subtri.segment_reason_name,
        subtri.triage_review_outcome_cd,
        subtri.triage_review_outcome_name,
        subtri.location_of_injury_toocs_txt,
        subtri.nature_of_injury_toocs_txt,
        subtri.risk_score,
        subtri.triage_comments_txt,
        subtri.icd_code_txt,
        subtri.inj_icd_code_txt,
        subtri.valid_triage_ind,
        case
            when subtri.valid_triage_ind = 'N' then 'N'
            when row_number() over (partition by case when subtri.valid_triage_ind = 'Y' then subtri.claim_nbr end order by triage_dttm asc) = 1
                then 'Y'
            else 'N'
        end as first_triage_ind,
        case
            when subtri.valid_triage_ind = 'N' then 'N'
            when row_number() over (partition by case when subtri.valid_triage_ind = 'Y' then subtri.claim_nbr end order by triage_dttm desc) = 1
                then 'Y'
            else 'N'
        end as latest_triage_ind,
        case
            when subtri.valid_triage_ind = 'N' then 'N'
            when row_number() over (partition by case when subtri.valid_triage_ind = 'Y' then subtri.claim_nbr end order by triage_dttm asc) = 1
                then 'N'
            else 'Y'
        end as re_triage_ind,
        case
            when subtri.valid_proposed_segment_ind = 'N' then 'N'
            when row_number() over (partition by case when subtri.valid_proposed_segment_ind = 'Y' then subtri.claim_nbr end order by triage_dttm asc) = 1
                then 'Y'
            else 'N'
        end as first_proposed_triage_ind,
        subtri.reviewer
    from cte_triage as subtri
)

select
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    triage_history_sk,
    triage_history_id,
    src_triage_history_id,
    CAST(src_create_dttm AS TIMESTAMP_NTZ) as src_create_dttm,
    src_create_dt,
    CAST(src_eff_dttm AS TIMESTAMP_NTZ) as src_eff_dttm,
    src_eff_dt,
    CAST(triage_dttm AS  TIMESTAMP_NTZ) as triage_dttm,
    triage_dt,
    CAST(triage_review_dttm AS  TIMESTAMP_NTZ) AS triage_review_dttm,
    triage_review_dt,
    proposed_segment_cd,
    proposed_segment_name,
    current_segment_cd,
    current_segment_name,
    accepted_segment_cd,
    accepted_segment_name,
    triage_source_cd,
    triage_source_name,
    segment_reason_cd,
    segment_reason_name,
    triage_review_outcome_cd,
    triage_review_outcome_name,
    location_of_injury_toocs_txt,
    nature_of_injury_toocs_txt,
    risk_score,
    triage_comments_txt,
    icd_code_txt,
    inj_icd_code_txt,
    valid_triage_ind,
    first_triage_ind,
    latest_triage_ind,
    re_triage_ind,
    first_proposed_triage_ind,
    reviewer
from cte_join
