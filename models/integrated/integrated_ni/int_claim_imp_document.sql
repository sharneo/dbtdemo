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
    unique_key=['src_claim_id', 'src_doc_id'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 33_CLAIM_IMP_DOCUMENT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A33
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_IMP_DOCUMENT
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        AND  file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_document as (
    select
        id,
        claimid,
        docuid,
        type,
        section,
        documentsubsection_icare,
        status,
        description,
        author,
        recipient,
        documentidentifier,
        documentpackid_icare,
        inbound,
        datesentreceived_icare,
        createtime,
        updatetime
    from {{ ref('v_cc_document_current') }}
    where retired = 0
),

cctl_documentsubsection_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_documentsubsection_icare_current') }}
),

cctl_documenttype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_documenttype_current') }}
),

cctl_documentsection as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_documentsection_current') }}
),

cctl_documentstatustype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_documentstatustype_current') }}
),
cte_join as
( 
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    doc.docuid as src_doc_id,
    dim_doctype.typecode as doc_type_cd,
    dim_doctype.name as doc_type_desc,
    dim_docsection.typecode as doc_section_cd,
    dim_docsection.name as doc_section_desc,
    dim_docsubsection.name as doc_sub_section_cd,
    dim_docsubsection.name as doc_sub_section_desc,
    dim_docstatus.typecode as doc_status_cd,
    dim_docstatus.name as doc_status_desc,
    doc.description as doc_description,
    doc.author as doc_author,
    doc.recipient as doc_recipient,
    doc.documentidentifier as doc_pack_identifier,
    doc.documentpackid_icare as doc_document_pack_id,
    case
        when doc.inbound = 0 then 'N'
        when doc.inbound = 1 then 'Y'
        else null
    end as doc_inbound_ind,
    CAST(doc.datesentreceived_icare as TIMESTAMP_NTZ) as doc_sent_received_dttm,
    cast(doc.datesentreceived_icare as date) as doc_sent_received_dt,
    CAST(doc.createtime as TIMESTAMP_NTZ) as doc_create_dttm,
    cast(doc.createtime as date) as doc_create_dt,
    CAST(doc.updatetime as TIMESTAMP_NTZ) as doc_update_dttm,
    row_number() over (partition by clm.id order by doc.datesentreceived_icare desc) as latest_date_sent_recd_rank,
    row_number() over (partition by clm.id order by doc.datesentreceived_icare asc) as earliest_date_sent_recd_rank,
    clm.file_ingestion_timestamp
from cc_document doc
inner join cctl_documentsubsection_icare dim_docsubsection
    on doc.documentsubsection_icare = dim_docsubsection.id
    and dim_docsubsection.name = 'IMP'
inner join cc_claim clm
    on clm.id = doc.claimid
left join cctl_documenttype dim_doctype
    on doc.type = dim_doctype.id
left join cctl_documentsection dim_docsection
    on doc.section = dim_docsection.id
left join cctl_documentstatustype dim_docstatus
    on doc.status = dim_docstatus.id
)
select 
        claim_sk,
        claim_nbr,
        src_claim_id,
        src_doc_id,
        doc_type_cd,
        doc_type_desc,
        doc_section_cd,
        doc_section_desc,
        doc_sub_section_cd,
        doc_sub_section_desc,
        doc_status_cd,
        doc_status_desc,
        doc_description,
        doc_author,
        doc_recipient,
        doc_pack_identifier,
        doc_document_pack_id,
        doc_inbound_ind,
        doc_sent_received_dttm,
        doc_sent_received_dt,
        doc_create_dttm,
        doc_create_dt,
        doc_update_dttm,
        latest_date_sent_recd_rank,
        earliest_date_sent_recd_rank,
        file_ingestion_timestamp
from
        cte_join