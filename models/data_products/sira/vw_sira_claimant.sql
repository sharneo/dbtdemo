{{ config(
    materialized='view',
    schema='sira'
) }}

WITH cc_claim as 
(
    SELECT
        *
FROM {{ ref('vw_cc_claim_current') }}
WHERE retired=0
),
cc_contact as (
    SELECT
        *
FROM {{ ref('vw_cc_contact_current') }}
WHERE retired=0
),
cc_address as (
    select *
FROM {{ ref('vw_cc_address_current') }}
    where retired = 0
),
cc_claimcontactrole as (
    select *
FROM {{ ref('vw_cc_claimcontactrole_current') }}
      where retired = 0
),

ccx_occupationdetails_icare as (
    select *
FROM {{ ref('vw_ccx_occupationdetails_icare_current') }}
      where retired = 0
),

cc_claimcontact as (
    select * FROM {{ ref('vw_cc_claimcontact_current') }}
),

cctl_contactrole as (
    select * FROM {{ ref('vw_cctl_contactrole_current') }}
    where typecode = 'claimant'
),

cctl_gendertype as (
    select * FROM {{ ref('vw_cctl_gendertype_current') }}
),

cctl_country as (
    select * FROM {{ ref('vw_cctl_country_current') }}
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
    left join cc_contact ctct on ctct.id = clmctct.contactid and ctct.retired = 0
    left join cc_address ad on ad.id = ctct.primaryaddressid and ad.retired = 0
    left join cc_claimcontactrole clmctctrole on clmctctrole.claimcontactid = clmctct.id and clmctctrole.retired = 0
    left join cctl_contactrole contactrole on contactrole.id = clmctctrole.role
    left join ccx_occupationdetails_icare od on od.id = ctct.occupationdetails_icareid and od.retired = 0
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
