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
    unique_key='med_treatment_activity_doc_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 60_MED_TREATMENT_ACTIVITY_DOCUMENT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A60
  TBL_NM: MSC_QLK_ASPIRE_MED_TREATMENT_ACTIVITY_DOCUMENT
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

base_cc_activity as (
    select
        id,
        claimid,
        activitypatternid,
        status,
        createtime
    from {{ ref('v_cc_activity_current') }}
    where retired = 0
),

base_cc_activitypattern as (
    select
        id,
        code
    from {{ ref('v_cc_activitypattern_current') }}
    where retired = 0
      and code in ('med_treatment_approval_review_required', 'review_mail_surgery', 'review_mail_service_request')
),

base_cctl_activitystatus as (
    select
        id,
        typecode
    from {{ ref('v_cctl_activitystatus_current') }}
    where typecode <> 'skipped'
),

base_cc_activitydocument as (
    select
        id,
        activityid,
        documentid
    from {{ ref('v_cc_activitydocument_current') }}
),

base_cc_document as (
    select
        id,
        description,
        inboundpackid_icare,
        inbound,
        datesentreceived_icare,
        createtime,
        updatetime,
        type,
        documentsubsection_icare
    from {{ ref('v_cc_document_current') }}
),

base_cctl_documenttype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_documenttype_current') }}
    where retired = 0
),

base_cctl_documentsubsection_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_documentsubsection_icare_current') }}
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
            'actv.id',
            'doc.id'
        ]) }} as varchar(150)) as med_treatment_activity_doc_sk,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        actv.id as src_activity_id,
        doc.id as src_doc_id,
        doc.description as doc_description,
        doc.inboundpackid_icare as doc_inbound_pack_id,
        case
            when doc.inbound = 0 then 'N'
            when doc.inbound = 1 then 'Y'
            else null
        end as doc_inbound_ind,
        CAST(doc.datesentreceived_icare AS TIMESTAMP_NTZ) AS  doc_sent_received_dttm,
        cast(doc.datesentreceived_icare as date) as doc_sent_received_dt,
        CAST(doc.createtime as TIMESTAMP_NTZ) doc_create_dttm,
        cast(doc.createtime as date) as doc_create_dt,
        CAST(doc.updatetime as TIMESTAMP_NTZ) AS  doc_update_dttm,
        dtyp.typecode as doc_type_cd,
        dtyp.name as doc_type_desc,
        subsect.typecode as doc_subsection_cd,
        subsect.name as doc_doc_subsection_desc,
        row_number() over (partition by clm.id order by doc.datesentreceived_icare desc) as latest_date_sent_recd_rank,
        row_number() over (partition by clm.id order by doc.datesentreceived_icare asc) as earliest_date_sent_recd_rank,
        clm.file_ingestion_timestamp
    from base_cc_activity as actv
    inner join base_cc_activitypattern as patt
        on patt.id = actv.activitypatternid
    inner join base_cctl_activitystatus as asts
        on asts.id = actv.status
    inner join base_cc_claim as clm
        on clm.id = actv.claimid
    inner join base_cc_activitydocument as adoc
        on adoc.activityid = actv.id
    inner join base_cc_document as doc
        on doc.id = adoc.documentid
    left join base_cctl_documenttype as dtyp
        on dtyp.id = doc.type
    left join base_cctl_documentsubsection_icare as subsect
        on subsect.id = doc.documentsubsection_icare
    where actv.createtime > dateadd(month, -12, current_date())
)

select
    med_treatment_activity_doc_sk,
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    src_activity_id,
    src_doc_id,
    doc_description,
    doc_inbound_pack_id,
    doc_inbound_ind,
    doc_sent_received_dttm,
    doc_sent_received_dt,
    doc_create_dttm,
    doc_create_dt,
    doc_update_dttm,
    doc_type_cd,
    doc_type_desc,
    doc_subsection_cd,
    doc_doc_subsection_desc,
    latest_date_sent_recd_rank,
    earliest_date_sent_recd_rank,
    file_ingestion_timestamp
from cte_join
