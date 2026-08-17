{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for claim txn document.
                                                claim_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{
  config(
    materialized='incremental',
    unique_key=['src_claim_id', 'src_txn_set_id', 'src_doc_id'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 16_CLAIM_TXN_DOCUMENT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A16
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_TXN_DOCUMENT
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
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_transactionset as (
    select
        id,
        claimid,
        documentlinkableid
    from {{ ref('v_cc_transactionset_current') }}
    where retired = 0
),

ccx_documentlinkable_icare as (
    select
        id
    from {{ ref('v_ccx_documentlinkable_icare_current') }}
    where retired = 0
),

ccx_documentlinks_icare as (
    select
        id,
        documentlinkableid,
        documentid
    from {{ ref('v_ccx_documentlinks_icare_current') }}
),

cc_document as (
    select
        id,
        docuid,
        type,
        section,
        documentsubsection_icare,
        status,
        documentchannel_icare,
        name,
        workcapacity_icareid,
        lineofbusiness_icare,
        inbound,
        schemeagent_icare,
        createtime,
        updatetime,
        datesentreceived_icare
    from {{ ref('v_cc_document_current') }}
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

cctl_documentsubsection_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_documentsubsection_icare_current') }}
),

cctl_documentstatustype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_documentstatustype_current') }}
),

cctl_documentchannel_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_documentchannel_icare_current') }}
),

cc_check as (
    select
        id,
        claimid,
        checksetid,
        ocr_invoice_icare,
        paymentsource_icare
    from {{ ref('v_cc_check_current') }}
    where retired = 0
),

ccx_ocrinvoice_icare as (
    select
        id,
        provideruniqueid,
        dateinvoicereceived,
        invoicesource,
        status,
        schemeid,
        createtime,
        updatetime
    from {{ ref('v_ccx_ocrinvoice_icare_current') }}
    where retired = 0
),

cctl_ocrprocess_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_ocrprocess_icare_current') }}
),

cctl_invoicesource_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_invoicesource_icare_current') }}
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
    trnset.id as src_txn_set_id,
    doc.docuid as src_doc_id,
    dim_doctype.typecode as doc_type_cd,
    dim_doctype.name as doc_type_desc,
    dim_docsection.typecode as doc_section_cd,
    dim_docsection.name as doc_section_desc,
    dim_docsubsection.typecode as doc_sub_section_cd,
    dim_docsubsection.name as doc_sub_section_desc,
    dim_docstatus.typecode as doc_status_cd,
    dim_docstatus.name as doc_status_desc,
    CAST(doc.datesentreceived_icare as TIMESTAMP_TZ) as  doc_sent_received_dttm,
    dim_docchannel.typecode as doc_channel_cd,
    dim_docchannel.name as doc_channel_desc,
    doc.name as doc_name,
    doc.workcapacity_icareid as doc_workcapacity_link_id,
    doc.lineofbusiness_icare as doc_lob_id,
    case
        when doc.inbound = 0 then 'N'
        when doc.inbound = 1 then 'Y'
        else null
    end as doc_inbound_ind,
    doc.schemeagent_icare as doc_schemeagent_id,
    CAST(doc.createtime AS TIMESTAMP_TZ) as doc_create_dttm,
    CAST(doc.updatetime AS TIMESTAMP_TZ) AS doc_update_dttm,
    case
        when dim_doctype.name != 'Invoice' then 'N'
        when rank() over (
            partition by clm.claimnumber, trnset.id
            order by doc.datesentreceived_icare asc, doc.createtime asc, doc.id asc
        ) = 1 then 'Y'
        else 'N'
    end as earliest_invoice_doc_ind,
    'N' as digital_pymt_invoice_ind,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_claim clm

inner join cc_transactionset trnset
    on trnset.claimid = clm.id

inner join ccx_documentlinkable_icare doclinkable
    on trnset.documentlinkableid = doclinkable.id

inner join ccx_documentlinks_icare doclink
    on doclinkable.id = doclink.documentlinkableid

inner join cc_document doc
    on doclink.documentid = doc.id

left join cctl_documenttype dim_doctype
    on doc.type = dim_doctype.id

left join cctl_documentsection dim_docsection
    on doc.section = dim_docsection.id

left join cctl_documentsubsection_icare dim_docsubsection
    on doc.documentsubsection_icare = dim_docsubsection.id

left join cctl_documentstatustype dim_docstatus
    on doc.status = dim_docstatus.id

left join cctl_documentchannel_icare dim_docchannel
    on doc.documentchannel_icare = dim_docchannel.id

union all

select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    trnset.id as src_txn_set_id,
    inv.provideruniqueid as src_doc_id,
    'invoice' as doc_type_cd,
    'Invoice' as doc_type_desc,
    'payments_icare' as doc_section_cd,
    'Payments' as doc_section_desc,
    'invoice' as doc_sub_section_cd,
    'Invoice' as doc_sub_section_desc,
    stus.typecode as doc_status_cd,
    stus.name as doc_status_desc,
    CAST(inv.dateinvoicereceived as TIMESTAMP_TZ) as doc_sent_received_dttm,
    invsrc.typecode as doc_channel_cd,
    invsrc.name as doc_channel_desc,
    null as doc_name,
    null as doc_workcapacity_link_id,
    null as doc_lob_id,
    'Y' as doc_inbound_ind,
    inv.schemeid as doc_schemeagent_id,
    CAST(inv.createtime as TIMESTAMP_TZ) as doc_create_dttm,
    CAST(inv.updatetime as TIMESTAMP_TZ) as doc_update_dttm,
    case
        when rank() over (
            partition by clm.claimnumber, trnset.id
            order by inv.dateinvoicereceived asc, inv.createtime asc, inv.id asc
        ) = 1 then 'Y'
        else 'N'
    end as earliest_invoice_doc_ind,
    'Y' as digital_pymt_invoice_ind,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_check chq

inner join ccx_ocrinvoice_icare inv
    on inv.id = chq.ocr_invoice_icare

inner join cc_claim clm
    on clm.id = chq.claimid

inner join cc_transactionset trnset
    on trnset.id = chq.checksetid

left join cctl_ocrprocess_icare stus
    on stus.id = inv.status

left join cctl_invoicesource_icare invsrc
    on invsrc.id = inv.invoicesource
where chq.paymentsource_icare = 'MP'
)
select 
        claim_sk,
        claim_nbr,
        src_claim_id,
        src_txn_set_id,
        src_doc_id,
        doc_type_cd,
        doc_type_desc,
        doc_section_cd,
        doc_section_desc,
        doc_sub_section_cd,
        doc_sub_section_desc,
        doc_status_cd,
        doc_status_desc,
        doc_sent_received_dttm,
        doc_channel_cd,
        doc_channel_desc,
        doc_name,
        doc_workcapacity_link_id,
        doc_lob_id,
        doc_inbound_ind,
        doc_schemeagent_id,
        doc_create_dttm,
        doc_update_dttm,
        earliest_invoice_doc_ind,
        digital_pymt_invoice_ind,
        extract_date,
        file_ingestion_timestamp
    from cte_join