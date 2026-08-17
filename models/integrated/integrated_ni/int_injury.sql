{{
  config(
    materialized='incremental',
    unique_key='int_loss_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 19_INJURY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A19
  TBL_NM: MSC_QLK_ASPIRE_INJURY
-#}

with cc_incident as (
    select
        id,
        publicid,
        claimid,
        description,
        multipleinjuries_icare,
        claimincident,
        subtype,
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
        updatetime,
        retired,
        file_ingestion_timestamp
    from {{ ref('v_cc_incident_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_claim as (
    select
        id,
        claimnumber,
        source_system
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cctl_dutystatus_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_dutystatus_icare_current') }}
),

cctl_severitytype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_severitytype_current') }}
),

cctl_fatalityliabdec_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_fatalityliabdec_icare_current') }}
),

cctl_incident as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_incident_current') }}
),

ccx_toocsbloiconnector_icare as (
    select
        id,
        injuryincident_icareid,
        injurycode,
        injurydescription,
        retired,
        selectedasprimary
    from {{ ref('v_ccx_toocsbloiconnector_icare_current') }}
    where retired = 0
        and selectedasprimary = 1
),

cc_injurydiagnosis as (
    select
        id,
        injuryincidentid,
        icdcode,
        retired,
        isprimary
    from {{ ref('v_cc_injurydiagnosis_current') }}
    where retired = 0
        and isprimary = 1
),

cc_icdcode as (
    select
        id,
        code,
        codedesc,
        retired
    from {{ ref('v_cc_icdcode_current') }}
    where retired = 0
),
cte_join as 
(
select distinct
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'inc.publicid'
    ]) }} as varchar(150)) as loss_sk,
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.claimnumber as claim_nbr,
    inc.id as src_incident_id,
    inc.claimid as src_claim_id,
    inc.description as injury_desc,
    case
        when inc.multipleinjuries_icare = 1 then 'Y'
        else 'N'
    end as multiple_injuries_ind,
    case
        when inc.claimincident = 1 then 'Y'
        else 'N'
    end as primary_claimable_incident_ind,
    dimincident.typecode as incident_type_cd,
    dimincident.name as incident_type_desc,
    CAST(inc.deceaseddate_icare AS TIMESTAMP_NTZ) as deceased_dt,
    CAST(inc.significantinjurydate_icare AS TIMESTAMP_NTZ) as significant_injury_dt,
    CAST(inc.contactcompletedate_icare AS TIMESTAMP_NTZ) as contact_complete_dt,
    CAST(inc.fatalitynotificationdate_icare AS TIMESTAMP_NTZ) as fatality_notification_dt,
    CAST(inc.resultofselfharm_icare as NUMBER) AS  fatality_result_of_self_harm_ind,
    CAST(inc.fatalityliabdecisiondate_icare AS TIMESTAMP_NTZ) as fatality_liability_decision_dt,
    CAST(inc.odgrtwdate_icare AS TIMESTAMP_NTZ)  as odg_rtw_dt,
    inc.odgduration_icare as odg_rtw_day_cnt,
    inc.breakdownagencycode_icare as breakdown_agency_cd,
    inc.breakdownagencydesc_icare as breakdown_agency_desc,
    inc.natureofinjurycode_icare as nature_of_injury_cd,
    inc.natureofinjurydesc_icare as nature_of_injury_desc,
    inc.mechanismofinjurycode_icare as mechanism_of_injury_cd,
    inc.mechanismofinjurydesc_icare as mechanism_of_injury_desc,
    inc.agencyofinjurycode_icare as agency_of_injury_cd,
    inc.agencyofinjurydesc_icare as agency_of_injury_desc,
    toocs.injurycode as location_of_injury_cd,
    toocs.injurydescription as location_of_injury_desc,
    dimdty.typecode as claimant_duty_status_cd,
    dimdty.name as claimant_duty_status_desc,
    dimsev.typecode as result_of_injury_cd,
    dimsev.name as result_of_injury_desc,
    dimfat.typecode as fatality_liability_decision_s1,
    dimfat.name as fatality_liability_decision_s2,
    icd.code as icd_cd,
    icd.codedesc as icd_desc,
    case
        when substring(icd.code, 1, 1) = 'F'
            and (try_cast(substring(icd.code, 2, 2) as int) between 1 and 59
                or try_cast(substring(icd.code, 2, 2) as int) = 99)
        then 'Y'
        else 'N'
    end as mental_stress_ind,
    CAST(inc.createtime as TIMESTAMP_NTZ) as src_create_dttm,
    cast(inc.createtime as date) as src_create_dt,
    CAST(inc.updatetime as TIMESTAMP_NTZ) as src_eff_dttm,
    cast(inc.updatetime as date) as src_eff_dt,
    current_date() as extract_date,
    inc.file_ingestion_timestamp

from cc_incident inc
left join cctl_dutystatus_icare dimdty
    on inc.dutystatus_icare = dimdty.id
left join cctl_severitytype dimsev
    on inc.severity = dimsev.id
left join cctl_fatalityliabdec_icare dimfat
    on inc.fatalityliabdec_icare = dimfat.id
inner join cc_claim clm
    on inc.claimid = clm.id
left join cctl_incident dimincident
    on inc.subtype = dimincident.id
left join ccx_toocsbloiconnector_icare toocs
    on inc.id = toocs.injuryincident_icareid
left join cc_injurydiagnosis injdiagnosis
    on inc.id = injdiagnosis.injuryincidentid
left join cc_icdcode icd
    on injdiagnosis.icdcode = icd.id
)
SELECT 
        cast({{ dbt_utils.generate_surrogate_key([
            'claim_nbr',
            'src_incident_id',
            'src_claim_id',
            'injury_desc',
            'primary_claimable_incident_ind',
            'multiple_injuries_ind'
        ]) }} as varchar(150)) as int_loss_sk,
        loss_sk,
        claim_sk,
        claim_nbr,
        src_incident_id,
        src_claim_id,
        injury_desc,
        multiple_injuries_ind,
        primary_claimable_incident_ind,
        incident_type_cd,
        incident_type_desc,
        deceased_dt,
        significant_injury_dt,
        contact_complete_dt,
        fatality_notification_dt,
        fatality_result_of_self_harm_ind,
        fatality_liability_decision_dt,
        odg_rtw_dt,
        odg_rtw_day_cnt,
        breakdown_agency_cd,
        breakdown_agency_desc,
        nature_of_injury_cd,
        nature_of_injury_desc,
        mechanism_of_injury_cd,
        mechanism_of_injury_desc,
        agency_of_injury_cd,
        agency_of_injury_desc,
        location_of_injury_cd,
        location_of_injury_desc,
        claimant_duty_status_cd,
        claimant_duty_status_desc,
        result_of_injury_cd,
        result_of_injury_desc,
        fatality_liability_decision_s1 as fatality_liability_decision_status_cd ,
        fatality_liability_decision_s2 as fatality_liability_decision_status_desc,
        icd_cd,
        icd_desc,
        mental_stress_ind,
        src_create_dttm,
        src_create_dt,
        src_eff_dttm,
        src_eff_dt,
        extract_date,
        file_ingestion_timestamp
FROM
         cte_join 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY int_loss_sk ORDER BY src_eff_dttm DESC) = 1 