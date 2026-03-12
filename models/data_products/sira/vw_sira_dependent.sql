
{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Dependent View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=["sira", "business_critical"]
) }}

with cc_claimcontact as (
    select
        id,
        claimid,
        dependenttype
    from {{ ref('vw_cc_claimcontact_current') }}
    where retired = 0
),

cc_claimcontactrole as (
    select
        claimcontactid,
        role
    from {{ ref('vw_cc_claimcontactrole_current') }}
    where retired = 0
),

cctl_contactrole as (
    select
        id
    from {{ ref('vw_cctl_contactrole_current') }}
),

cctl_dependenttype as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_dependenttype_current') }}
),

base as (
    select 
        coalesce(clmctct.claimid, '') as claimid,
        case when dependenttype.typecode = 'childunder16' then 1 end as childunder16,
        case when dependenttype.typecode != 'childunder16' then 1 end as other
    from cc_claimcontact clmctct
    left join cc_claimcontactrole clmctctrole on clmctctrole.claimcontactid = clmctct.id
    left join cctl_contactrole contactrole on contactrole.id = clmctctrole.role
    left join cctl_dependenttype dependenttype on dependenttype.id = clmctct.dependenttype
)

select
    claimid,
    childunder16,
    other
from base