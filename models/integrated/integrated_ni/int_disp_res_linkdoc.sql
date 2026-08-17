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
    unique_key='claim_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

with cc_claim as (
    select
          id
        , claimnumber
        , source_system
        , retired
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}

),

ccx_dispute_icare as (
    select
          id
        , claimid
        , referencenumber
        , type
        , documentlinkableid
        , retired
    from {{ ref('v_ccx_dispute_icare_current') }}
    where retired = 0
),

ccx_documentlinks_icare as (
    select
          documentlinkableid
        , documentid
    from {{ ref('v_ccx_documentlinks_icare_current') }}
),

ccx_documentlinkable_icare as (
    select
          id
    from {{ ref('v_ccx_documentlinkable_icare_current') }}
),

cc_document as (
    select
          id
        , namedenorm
        , documentsubsection_icare
        , type
        , description
        , datesentreceived_icare
        , authordenorm
        , documentpackid_icare
        , retired
    from {{ ref('v_cc_document_current') }}
    where retired = 0
),

cctl_documentsubsection_icare as (
    select
          id
        , description
        , retired
    from {{ ref('v_cctl_documentsubsection_icare_current') }}
    where retired = 0
),

cctl_documenttype as (
    select
          id
        , name
        , retired
    from {{ ref('v_cctl_documenttype_current') }}
    where retired = 0
),

final as (
    select
          cast({{ dbt_utils.generate_surrogate_key(['clm.source_system', 'clm.claimnumber']) }} as varchar(150)) as claim_sk
        , clm.source_system              as src_system_cd
        , clm.claimnumber                as claim_nbr
        , clm.id                         as src_claim_id
        , dsp.referencenumber            as reference_no
        , dsp.type                       as type_cd
        , dcmt.namedenorm                as file_name
        , dcmt.documentsubsection_icare  as document_sub_category_cd
        , dcmtsub.description            as document_sub_category
        , dcmt.type                      as document_type_cd
        , doct.name                      as document_type
        , dcmt.description               as description
        , cast(dcmt.datesentreceived_icare as datetime) as date_sent_received
        , dcmt.authordenorm              as author
        , dcmt.documentpackid_icare      as pack_id
    from cc_claim as clm
    join ccx_dispute_icare as dsp
        on clm.id = dsp.claimid
    join ccx_documentlinks_icare as doc
        on doc.documentlinkableid = dsp.documentlinkableid
    left join ccx_documentlinkable_icare as docl
        on docl.id = doc.documentid
    left join cc_document as dcmt
        on dcmt.id = doc.documentid
    left join cctl_documentsubsection_icare as dcmtsub
        on dcmt.documentsubsection_icare = dcmtsub.id
    left join cctl_documenttype as doct
        on doct.id = dcmt.type
)

select
      claim_sk
    , src_system_cd
    , claim_nbr
    , src_claim_id
    , reference_no
    , type_cd
    , file_name
    , document_sub_category_cd
    , document_sub_category
    , document_type_cd
    , document_type
    , description
    , date_sent_received
    , author
    , pack_id
from final
