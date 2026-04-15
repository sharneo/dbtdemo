
{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Claimant View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=["sira", "business_critical"]
) }}


with cc_claim as (
    select
        id,
        claimnumber
    from {{ ref('vw_cc_claim') }}
    where retired = 0
),

cc_contact as (
    select
        id,
        retired,
        homephone,
        dateofbirth,
        abslanguagecode_icare,
        occupation,
        occupationdetails_icareid,
        primaryaddressid,
        gender
    from {{ ref('vw_cc_contact') }}
    where retired = 0
),

cc_address as (
    select
        id,
        retired,
        country,
        addressline1,
        addressline2,
        addressline3,
        city,
        postalcode
    from {{ ref('vw_cc_address') }}
    where retired = 0
),

cc_claimcontactrole as (
    select
        claimcontactid,
        retired,
        role
    from {{ ref('vw_cc_claimcontactrole') }}
    where retired = 0
),

ccx_occupationdetails_icare as (
    select
        id,
        retired,
        unitcode
    from {{ ref('vw_ccx_occupationdetails_icare') }}
    where retired = 0
),

cc_claimcontact as (
    select
        claimid,
        contactid,
        id
    from {{ ref('vw_cc_claimcontact') }}
),

cctl_contactrole as (
    select
        id
    from {{ ref('vw_cctl_contactrole') }}
    where typecode = 'claimant'
),

cctl_gendertype as (
    select
        id,
        typecode,
        name
    from {{ ref('vw_cctl_gendertype') }}
),

cctl_country as (
    select
        id,
        name
    from {{ ref('vw_cctl_country') }}
),

base as (
    select 
        coalesce(clmctct.claimid, '') as claimid,
        c.claimnumber,
        ctct.homephone,
        case 
            when coalesce(ad.country, 0) != 10015 
            then ltrim(coalesce(ad.addressline1, '') || ' ' || coalesce(ad.addressline2, '') || ' ' || coalesce(ad.city, '') || ' ' || coalesce(ad.postalcode, '') || ' ' || coalesce(cntry.name, ''))
            else ltrim(rtrim(coalesce(ad.addressline1, '') || ' ' || coalesce(ad.addressline2, '') || ' ' || coalesce(ad.addressline3, '')))
        end as addressline1,
        case 
            when coalesce(ad.country, 0) = 10015 then coalesce(ad.city, '')
            else 'OS'
        end as city,
        case 
            when coalesce(ad.country, 0) = 10015 then coalesce(ad.postalcode, '0000')
            else '0000'
        end as postalcode,
        gt.typecode as gender,
        ctct.dateofbirth,
        ctct.abslanguagecode_icare,
        ctct.occupation,
        ctct.occupationdetails_icareid,
        od.unitcode,
        cntry.name as cntryname,
        gt.name as gendername
    from cc_claimcontact clmctct
    inner join cc_claim c on c.id = clmctct.claimid
    left join cc_contact ctct on ctct.id = clmctct.contactid
    left join cc_address ad on ad.id = ctct.primaryaddressid
    left join cc_claimcontactrole clmctctrole on clmctctrole.claimcontactid = clmctct.id
    left join cctl_contactrole contactrole on contactrole.id = clmctctrole.role
    left join ccx_occupationdetails_icare od on od.id = ctct.occupationdetails_icareid
    left join cctl_gendertype gt on gt.id = ctct.gender
    left join cctl_country cntry on cntry.id = ad.country
)

select
    claimid,
    claimnumber,
    homephone,
    addressline1,
    city,
    postalcode,
    gender,
    dateofbirth,
    abslanguagecode_icare,
    occupation,
    occupationdetails_icareid,
    unitcode,
    cntryname,
    gendername
from base