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
    unique_key='claim_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 06_CLAIM_EXTRACT_EML.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A06
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_EXTRACT_EML
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        assigneduserid,
        assignedgroupid,
        state,
        policyid,
        lossdate,
        employersize_icare,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp > (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

base_cc_claimcontact as (
    select
        id,
        claimid,
        contactid,
        claimantflag
    from {{ ref('v_cc_claimcontact_current') }}
    where retired = 0
),

base_cc_contact as (
    select
        id,
        name,
        firstname,
        middlename,
        lastname,
        dateofbirth,
        gender,
        emailaddress1,
        emailaddress2,
        primaryaddressid,
        primaryphone,
        homephone,
        workphone,
        cellphone,
        homephonecountry,
        workphonecountry,
        cellphonecountry,
        occupationdetails_icareid
    from {{ ref('v_cc_contact_current') }}
    where retired = 0
),

base_cc_claimcontactrole as (
    select
        id,
        claimcontactid,
        role
    from {{ ref('v_cc_claimcontactrole_current') }}
    where retired = 0
),

base_cctl_contactrole as (
    select
        id,
        typecode
    from {{ ref('v_cctl_contactrole_current') }}
    where typecode in ('insured', 'claimant')
),

base_cctl_gendertype as (
    select
        id,
        name
    from {{ ref('v_cctl_gendertype_current') }}
    where retired = 0
),

base_ccx_occupationdetails_icare as (
    select
        id,
        unitcode,
        unitdesc
    from {{ ref('v_ccx_occupationdetails_icare_current') }}
    where retired = 0
),

base_cc_address as (
    select
        id,
        addressline1,
        addressline2,
        addressline3,
        city,
        postalcode,
        state,
        addresstype
    from {{ ref('v_cc_address_current') }}
    where retired = 0
),

base_cctl_addresstype as (
    select
        id,
        name
    from {{ ref('v_cctl_addresstype_current') }}
    where retired = 0
),

base_cctl_state as (
    select
        id,
        name
    from {{ ref('v_cctl_state_current') }}
    where retired = 0
),

base_cctl_primaryphonetype as (
    select
        id,
        typecode
    from {{ ref('v_cctl_primaryphonetype_current') }}
    where retired = 0
),

base_cctl_phonecountrycode as (
    select
        id,
        typecode,
        description
    from {{ ref('v_cctl_phonecountrycode_current') }}
    where retired = 0
),

base_cc_user as (
    select
        id,
        contactid
    from {{ ref('v_cc_user_current') }}
    where retired = 0
),

base_cctl_claimstate as (
    select
        id,
        name
    from {{ ref('v_cctl_claimstate_current') }}
),

base_cc_group as (
    select
        id,
        name
    from {{ ref('v_cc_group_current') }}
    where retired = 0
),

base_cc_policy as (
    select
        id,
        policynumber,
        policytype_icare,
        employercategory_icare,
        totalbtp_icare,
        producername_icare,
        producercode
    from {{ ref('v_cc_policy_current') }}
    where retired = 0
),

base_cctl_policytype_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_policytype_icare_current') }}
),

base_cctl_groupemployersize_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_groupemployersize_icare_current') }}
),

base_cctl_employercategory_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_employercategory_icare_current') }}
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_claim_contact as (
    select
        clmcon.claimid as claim_id,
        ctt.dateofbirth as birth_dt,
        gender.name as gender,
        case
            when dimcon.typecode in ('insured') then ctt.name
            else concat(ctt.firstname,
                        case when ctt.middlename is null then ' ' else concat(' ', ctt.middlename, ' ') end,
                        ctt.lastname)
        end as contact_name,
        dimcon.typecode as contact_role_cd,
        clmcon.claimantflag as claimant_ind,
        occ.unitcode as occupation_cd,
        occ.unitdesc as occupation_desc,
        adrtyp.name as address_type,
        adr.addressline1 as address_line1,
        adr.addressline2 as address_line2,
        adr.addressline3 as address_line3,
        adr.city as address_suburb,
        ste.name as address_state,
        adr.postalcode as address_postcode,
        coalesce(ctt.emailaddress1, ctt.emailaddress2) as contact_email,
        phtyp.typecode as primary_phone_type,
        case when coalesce(hmctry.typecode, 'AU') = 'AU' then ctt.homephone
             else concat('+', hmctry.description, ctt.homephone)
        end as home_phone,
        case when coalesce(wkctry.typecode, 'AU') = 'AU' then ctt.workphone
             else concat('+', wkctry.description, ctt.workphone)
        end as work_phone,
        case when coalesce(mbctry.typecode, 'AU') = 'AU' then ctt.cellphone
             else concat('+', mbctry.description, ctt.cellphone)
        end as mobile_phone
    from base_cc_claimcontact as clmcon
    left join base_cc_contact as ctt
        on clmcon.contactid = ctt.id
    left join base_cc_claimcontactrole as conrol
        on clmcon.id = conrol.claimcontactid
    inner join base_cctl_contactrole as dimcon
        on conrol.role = dimcon.id
    left join base_cctl_gendertype as gender
        on ctt.gender = gender.id
    left join base_ccx_occupationdetails_icare as occ
        on occ.id = ctt.occupationdetails_icareid
    left join base_cc_address as adr
        on adr.id = ctt.primaryaddressid
    left join base_cctl_addresstype as adrtyp
        on adrtyp.id = adr.addresstype
    left join base_cctl_state as ste
        on ste.id = adr.state
    left join base_cctl_primaryphonetype as phtyp
        on phtyp.id = ctt.primaryphone
    left join base_cctl_phonecountrycode as hmctry
        on hmctry.id = ctt.homephonecountry
    left join base_cctl_phonecountrycode as wkctry
        on wkctry.id = ctt.workphonecountry
    left join base_cctl_phonecountrycode as mbctry
        on mbctry.id = ctt.cellphonecountry
),

final as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.claimnumber as claim_nbr,
        coninj.contact_name as injured_worker_name,
        CAST(coninj.birth_dt AS TIMESTAMP_NTZ) as injured_worker_birth_dt,
        coninj.gender as injured_worker_gender,
        CAST(clm.lossdate AS TIMESTAMP_NTZ) as loss_dttm,
        pol.policynumber as policy_nbr,
        conins.contact_name as policy_name,
        clmst.name as claim_state_desc,
        concat(con.firstname,
               case when con.middlename is null then ' ' else concat(' ', con.middlename, ' ') end,
               con.lastname) as case_owner_name,
        con.workphone as case_owner_phone_nbr,
        grp.name as case_owner_team_name,
        dim_policytype.name as policy_type,
        dim_empsize.name as employer_size_desc,
        dim_empcat.name as employer_category_desc,
        pol.totalbtp_icare as total_base_tariff_premium_amt,
        pol.producername_icare as agent_name,
        pol.producercode as producer_cd,
        coninj.occupation_cd as injured_worker_occupation_cd,
        coninj.occupation_desc as injured_worker_occupation_desc,
        coninj.address_type as injured_worker_addr_type,
        coninj.address_line1 as injured_worker_addr_line1,
        coninj.address_line2 as injured_worker_addr_line2,
        coninj.address_line3 as injured_worker_addr_line3,
        coninj.address_suburb as injured_worker_addr_suburb,
        coninj.address_state as injured_worker_addr_state,
        coninj.address_postcode as injured_worker_postcode,
        coninj.contact_email as injured_worker_email,
        case
            when coninj.primary_phone_type = 'home' then coninj.home_phone
            when coninj.primary_phone_type = 'work' then coninj.work_phone
            when coninj.primary_phone_type = 'mobile' then coninj.mobile_phone
            else coalesce(coninj.mobile_phone, coninj.home_phone, coninj.work_phone)
        end as injured_worker_phone,
        clm.file_ingestion_timestamp
    from base_cc_claim as clm
    left join base_cc_user as usr
        on clm.assigneduserid = usr.id
    left join base_cctl_claimstate as clmst
        on clm.state = clmst.id
    left join base_cc_contact as con
        on usr.contactid = con.id
    left join base_cc_group as grp
        on clm.assignedgroupid = grp.id
    left join base_cc_policy as pol
        on clm.policyid = pol.id
    left join cte_claim_contact as coninj
        on clm.id = coninj.claim_id
        and coninj.contact_role_cd = 'claimant'
    left join cte_claim_contact as conins
        on clm.id = conins.claim_id
        and conins.contact_role_cd = 'insured'
        and conins.contact_name is not null
    left join base_cctl_groupemployersize_icare as dim_empsize
        on clm.employersize_icare = dim_empsize.id
    left join base_cctl_employercategory_icare as dim_empcat
        on pol.employercategory_icare = dim_empcat.id
    left join base_cctl_policytype_icare as dim_policytype
        on pol.policytype_icare = dim_policytype.id
)

select
    claim_sk,
    src_system_cd,
    claim_nbr,
    injured_worker_name,
    injured_worker_birth_dt,
    injured_worker_gender,
    loss_dttm,
    policy_nbr,
    policy_name,
    claim_state_desc,
    case_owner_name,
    case_owner_phone_nbr,
    case_owner_team_name,
    policy_type,
    employer_size_desc,
    employer_category_desc,
    total_base_tariff_premium_amt,
    agent_name,
    producer_cd,
    injured_worker_occupation_cd,
    injured_worker_occupation_desc,
    injured_worker_addr_type,
    injured_worker_addr_line1,
    injured_worker_addr_line2,
    injured_worker_addr_line3,
    injured_worker_addr_suburb,
    injured_worker_addr_state,
    injured_worker_postcode,
    injured_worker_email,
    injured_worker_phone,
    file_ingestion_timestamp
from final
