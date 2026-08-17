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
    unique_key='disp_res_reqreview_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: D02_DISP_RES_REQREVIEW.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_D02
  TBL_NM: MSC_QLK_ASPIRE_DISP_RES_REQREV
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
        datereviewrequested,
        furtherinfosubmitted,
        applicationlodgedby,
        ackletterdate,
        reviewduedate,
        significantlegalmatter,
        comanagedworker
    from {{ ref('v_ccx_dispute_icare_current') }}
    where retired = 0
),

base_ccx_disputerevreqwrap_icare as (
    select
        id,
        disputeid,
        decision
    from {{ ref('v_ccx_disputerevreqwrap_icare_current') }}
    where retired = 0
),

base_cctl_dispreviewreqlist_icare as (
    select
        id,
        description
    from {{ ref('v_cctl_dispreviewreqlist_icare_current') }}
    where retired = 0
),

base_cctl_disputeapplodger_icare as (
    select
        id,
        description
    from {{ ref('v_cctl_disputeapplodger_icare_current') }}
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
            'dsp.id'
        ]) }} as varchar(150)) as disp_res_reqreview_sk,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        dsp.referencenumber as reference_no,
        dsp.type as type_cd,
        cast(dsp.datereviewrequested as date) as date_request_for_review_received,
        drrl.description as review_requested,
        case
            when dsp.furtherinfosubmitted = 0 then 'No'
            when dsp.furtherinfosubmitted = 1 then 'Yes'
            else null
        end as further_information_submitted,
        dsp.applicationlodgedby as applicationlodgedby_cd,
        dspap.description as application_lodged_by,
        cast(dsp.ackletterdate as date) as acknowledgement_letter_date,
        cast(dsp.reviewduedate as date) as review_due_date,
        case
            when dsp.significantlegalmatter = 0 then 'No'
            when dsp.significantlegalmatter = 1 then 'Yes'
            else null
        end as significant_legal_matter,
        case
            when dsp.comanagedworker = 0 then 'No'
            when dsp.comanagedworker = 1 then 'Yes'
            else null
        end as co_managed_worker,
        clm.file_ingestion_timestamp
    from base_cc_claim as clm
    inner join base_ccx_dispute_icare as dsp
        on clm.id = dsp.claimid
    left join base_ccx_disputerevreqwrap_icare as drrw
        on drrw.disputeid = dsp.id
    left join base_cctl_dispreviewreqlist_icare as drrl
        on drrl.id = drrw.decision
    left join base_cctl_disputeapplodger_icare as dspap
        on dspap.id = dsp.applicationlodgedby
)

select
    disp_res_reqreview_sk,
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    reference_no,
    type_cd,
    date_request_for_review_received,
    review_requested,
    further_information_submitted,
    applicationlodgedby_cd,
    application_lodged_by,
    acknowledgement_letter_date,
    review_due_date,
    significant_legal_matter,
    co_managed_worker,
    file_ingestion_timestamp
from cte_join
