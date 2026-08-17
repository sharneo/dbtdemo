{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Dependent View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with cc_claimcontact as (
    select
        id,
        claimid,
        dependenttype
    from {{ ref('v_cc_claimcontact_current') }}
    where retired = 0
),

cc_claimcontactrole as (
    select
        claimcontactid,
        role
    from {{ ref('v_cc_claimcontactrole_current') }}
    where retired = 0
),

cctl_contactrole as (
    select
        id
    from {{ ref('v_cctl_contactrole_current') }}
    where typecode = 'claimantdep'
),

cctl_dependenttype as (
    select
        id,
        typecode
    from {{ ref('v_cctl_dependenttype_current') }}
),

base as (
    select
        coalesce(clmctct.claimid, '') as claimid,
        case when dependenttype.typecode = 'childunder16' then 1 end as childunder16,
        case when dependenttype.typecode != 'childunder16' then 1 end as other
    from cc_claimcontact as clmctct
    left join cc_claimcontactrole as clmctctrole on clmctctrole.claimcontactid = clmctct.id
    left join cctl_contactrole as contactrole on contactrole.id = clmctctrole.role
    left join cctl_dependenttype as dependenttype on dependenttype.id = clmctct.dependenttype
    where contactrole.id is not null
)

select
    claimid,
    childunder16,
    other
from base
