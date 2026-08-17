{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Aspire - original table materialization
2026-06-02      1.0                             Converted to incremental with merge strategy

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
  Source: 56_DOCUMENT_INVOICES.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A56
  TBL_NM: MSC_QLK_Aspire_claim_document_invoice
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        retired,
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
        status,
        datesentreceived_icare,
        name,
        description,
        createtime,
        documentchannel_icare,
        author,
        inbound,
        type,
        retired
    from {{ ref('v_cc_document_current') }}
    where retired = 0
    and coalesce(inbound, 1) = 1
),

cctl_documenttype as (
    select
        id,
        name
    from {{ ref('v_cctl_documenttype_current') }}
    where name = 'Invoice'
),

cctl_documentstatustype as (
    select
        id,
        name
    from {{ ref('v_cctl_documentstatustype_current') }}
),

cctl_documentchannel_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_documentchannel_icare_current') }}
),

cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as source_system,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    doc.docuid as doc_uid,
    dim_docstatus.name as document_status,
    CAST(doc.datesentreceived_icare AS TIMESTAMP_NTZ) as document_sent_dttm,
    doc.name as file_name,
    doc.description as description,
    CAST(doc.createtime AS TIMESTAMP_NTZ) as document_create_dttm,
    dim_docchannel.name as document_channel,
    doc.author as document_author,
    clm.file_ingestion_timestamp
from cc_claim clm
join cc_document doc
    on doc.claimid = clm.id
join cctl_documenttype dim_doctype
    on dim_doctype.id = doc.type
left join cctl_documentstatustype dim_docstatus
    on doc.status = dim_docstatus.id
left join cctl_documentchannel_icare dim_docchannel
    on dim_docchannel.id = doc.documentchannel_icare
)
SELECT 
    claim_sk,
    source_system,
    claim_nbr,
    src_claim_id,
    doc_uid,
    document_status,
    document_sent_dttm,
    file_name,
    description,
    document_create_dttm,
    document_channel,
    document_author,
    file_ingestion_timestamp,
    cast({{ dbt_utils.generate_surrogate_key([
        'claim_nbr',
        'src_claim_id',
        'doc_uid',
        'document_status',
        'document_sent_dttm',
        'file_name',
        'description',
        'document_create_dttm',
        'document_channel',
        'document_author'
    ]) }} as varchar(150)) as document_sk
FROM 
    cte_join