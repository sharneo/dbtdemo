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
    unique_key='disp_res_details_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: D01_DISP_RES_DETAILS.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_D01
  TBL_NM: MSC_QLK_ASPIRE_DISP_RES_DETAILS
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

base_ccx_dispute_icare as (
    select
        id,
        claimid,
        referencenumber,
        type,
        channel,
        status,
        reviewerid,
        significantlegalmatter,
        decisionissuedate
    from {{ ref('v_ccx_dispute_icare_current') }}
    where retired = 0
),

base_cctl_status_icare as (
    select
        id,
        description
    from {{ ref('v_cctl_status_icare_current') }}
    where retired = 0
),

base_cctl_disputetype_icare as (
    select
        id,
        description
    from {{ ref('v_cctl_disputetype_icare_current') }}
    where retired = 0
),

base_cctl_disputechannel_icare as (
    select
        id,
        description
    from {{ ref('v_cctl_disputechannel_icare_current') }}
    where retired = 0
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
        lastname,
        emailaddress1
    from {{ ref('v_cc_contact_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

final as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber',
            'dsp.referencenumber'
        ]) }} as varchar(150)) as disp_res_details_sk,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        dsp.referencenumber as reference_no,
        dsp.type as type_cd,
        dspt.description as type,
        dsp.channel as channel_cd,
        dspch.description as channel_desc,
        dsp.status as status_cd,
        st.description as status_desc,
        dsp.reviewerid as reviewer_id,
        concat(cct.firstname, ' ', cct.lastname) as reviewer,
        cct.emailaddress1 as reviewer_email,
        case
            when dsp.significantlegalmatter = 1 then 'Y'
            else 'N'
        end as significant_legal_ind,
        cast(dsp.decisionissuedate as date) as issue_dt,
        clm.file_ingestion_timestamp
    from base_cc_claim as clm
    inner join base_ccx_dispute_icare as dsp
        on dsp.claimid = clm.id
    left join base_cctl_status_icare as st
        on st.id = dsp.status
    left join base_cctl_disputetype_icare as dspt
        on dspt.id = dsp.type
    left join base_cctl_disputechannel_icare as dspch
        on dspch.id = dsp.channel
    left join base_cc_user as usr
        on usr.id = dsp.reviewerid
    left join base_cc_contact as cct
        on cct.id = usr.contactid
)

select
    disp_res_details_sk,
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    reference_no,
    type_cd,
    type,
    channel_cd,
    channel_desc,
    status_cd,
    status_desc,
    reviewer_id,
    reviewer,
    reviewer_email,
    significant_legal_ind,
    issue_dt,
    file_ingestion_timestamp
from final
