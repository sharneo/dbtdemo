{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental model for managing entity reference data.
-#}   

{{
  config(
    materialized='incremental',
    unique_key='managing_entity_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 35_MANAGING_ENTITY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A35
  TBL_NM: MSC_QLK_ASPIRE_MANAGING_ENTITY
-#}

with ccx_managingentity_icare as (
    select
        id,
        publicid,
        portaldisplayname,
        retired,
        createtime,
        name,
        code,
        contactphone,
        role,
        managingentityorgid,
        website,
        contactemail,
        postaladdress,
        file_ingestion_timestamp,
        source_system
    from {{ ref('v_ccx_managingentity_icare_current') }}
    {% if is_incremental() %}
    where file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

ccx_managingentityorg_icare as (
    select
        id,
        name
    from {{ ref('v_ccx_managingentityorg_icare_current') }}
),

cctl_managingentityrole_icare as (
    select
        id,
        description
    from {{ ref('v_cctl_managingentityrole_icare_current') }}
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'ent.source_system',
        'ent.publicid'
    ]) }} as varchar(150)) as managing_entity_sk,
    ent.source_system as src_system_cd,
    ent.id as managing_entity_id,
    ent.portaldisplayname as portal_display_name,
    case
        when ent.retired > 0 then 'Y'
        else 'N'
    end as retired_ind,
    CAST(ent.createtime as TIMESTAMP_NTZ) as src_create_dttm,
    ent.name as managing_entity_name,
    ent.code as managing_entity_cd,
    ent.contactphone as contact_phone,
    rol.description as managing_entity_role,
    org.name as managing_entity_organisation,
    ent.website as managing_entity_website,
    ent.contactemail as managing_entity_email,
    ent.postaladdress as managing_entity_postal_address,
    current_date() as extract_date,
    ent.file_ingestion_timestamp

from ccx_managingentity_icare ent

left join ccx_managingentityorg_icare org
    on org.id = ent.managingentityorgid

left join cctl_managingentityrole_icare rol
    on rol.id = ent.role
)
select 
    managing_entity_sk,
    src_system_cd,
    managing_entity_id,
    portal_display_name,
    retired_ind,
    src_create_dttm,
    managing_entity_name,
    managing_entity_cd,
    contact_phone,
    managing_entity_role,
    managing_entity_organisation,
    managing_entity_website,
    managing_entity_email,
    managing_entity_postal_address,
    file_ingestion_timestamp
from
    cte_join