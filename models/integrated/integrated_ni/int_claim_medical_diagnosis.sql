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
    unique_key='claim_medical_diagnosis_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 59_CLAIM_MEDICAL_DIAGNOSIS.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A59
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_MEDICAL_DIAGNOSIS
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

base_cc_injurydiagnosis as (
    select
        id,
        injuryincidentid,
        icdcode,
        datestarted,
        dateended,
        injuryseverity_icare,
        payable_icare,
        isprimary,
        createtime
    from {{ ref('v_cc_injurydiagnosis_current') }}
    where retired = 0
),

base_cc_incident as (
    select
        id,
        claimid
    from {{ ref('v_cc_incident_current') }}
),

base_cc_icdcode as (
    select
        id,
        code,
        codedesc
    from {{ ref('v_cc_icdcode_current') }}
    where retired = 0
),

base_cctl_injuryseverity_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_injuryseverity_icare_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_join as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber',
            'diag.id'
        ]) }} as varchar(150)) as claim_medical_diagnosis_sk,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.claimnumber as claim_nbr,
        inc.id as src_incident_id,
        diag.id as src_injury_diagnosis_id,
        inc.claimid as src_claim_id,
        icd.code as icd_cd,
        icd.codedesc as icd_desc,
        CAST(diag.datestarted AS TIMESTAMP_NTZ) as diagnosis_start_dttm,
        CAST(diag.dateended AS TIMESTAMP_NTZ)  as diagnosis_end_dttm,
        sev.typecode as diagnosis_severity_cd,
        sev.name as diagnosis_severity_desc,
        case when diag.payable_icare = 1 then 'Y' else 'N' end as payable_ind,
        case when diag.isprimary = 1 then 'Y' else 'N' end as primary_diagnosis_ind,
        CAST(diag.createtime AS TIMESTAMP_NTZ)  as src_create_dttm,
        cast(diag.createtime as date) as src_create_dt,
        clm.file_ingestion_timestamp
    from base_cc_injurydiagnosis as diag
    inner join base_cc_incident as inc
        on inc.id = diag.injuryincidentid
    inner join base_cc_claim as clm
        on clm.id = inc.claimid
    left join base_cc_icdcode as icd
        on icd.id = diag.icdcode
    left join base_cctl_injuryseverity_icare as sev
        on sev.id = diag.injuryseverity_icare
)

select
    claim_medical_diagnosis_sk,
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_incident_id,
    src_injury_diagnosis_id,
    src_claim_id,
    icd_cd,
    icd_desc,
    diagnosis_start_dttm,
    diagnosis_end_dttm,
    diagnosis_severity_cd,
    diagnosis_severity_desc,
    payable_ind,
    primary_diagnosis_ind,
    src_create_dttm,
    src_create_dt,
    file_ingestion_timestamp
from cte_join
