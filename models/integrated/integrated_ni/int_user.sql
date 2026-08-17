{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental model for user dimension.
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_user_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 28_USER.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A28
  TBL_NM: MSC_QLK_ASPIRE_USER
-#}

with cc_user as (
    select
        id,
        publicid,
        contactid,
        credentialid,
        externaluser,
        oktaid_icare,
        jobtitle,
        vacationstatus,
        language,
        defaultphonecountry,
        systemusertype,
        defaultcountry,
        managingentity_icareid,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_user_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_credential as (
    select
        id,
        active
    from {{ ref('v_cc_credential_current') }}
    where retired = 0
),

cc_contact as (
    select
        id,
        firstname,
        middlename,
        lastname,
        workphone,
        emailaddress1
    from {{ ref('v_cc_contact_current') }}
    where retired = 0
),

cctl_vacationstatustype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_vacationstatustype_current') }}
),

cctl_languagetype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_languagetype_current') }}
),

cctl_phonecountrycode as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_phonecountrycode_current') }}
),

cctl_systemusertype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_systemusertype_current') }}
),

cctl_country as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_country_current') }}
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'usr.source_system',
        'usr.publicid'
    ]) }} as varchar(150)) as user_sk,
    usr.source_system as src_system_cd,
    usr.publicid as src_ext_user_id,
    usr.id as src_user_id,
    con.firstname as user_first_name,
    con.middlename as user_middle_name,
    con.lastname as user_last_name,
    con.workphone as user_phone_nbr,
    con.emailaddress1 as user_email_address,
    case
        when usr.externaluser = 1 then 'Y'
        else 'N'
    end as external_user_ind,
    usr.oktaid_icare as okta_id,
    usr.jobtitle as user_job_title_txt,
    vacst.typecode as vacation_status_type_cd,
    vacst.name as vacation_status_type_name,
    lang.typecode as user_language_cd,
    lang.name as user_language_name,
    defphctry.typecode as user_default_phone_ctry_cd,
    defphctry.name as user_default_phone_ctry_name,
    sysusr.typecode as system_user_type_cd,
    sysusr.name as system_user_type_name,
    defctry.typecode as user_default_country_cd,
    defctry.name as user_default_country_name,
    usr.managingentity_icareid as managing_entity_id,
    case
        when cred.active = 1 then 'Y'
        else 'N'
    end as user_active_ind,
    usr.file_ingestion_timestamp

from cc_user usr

left join cc_credential cred
    on cred.id = usr.credentialid

left join cc_contact con
    on usr.contactid = con.id

left join cctl_vacationstatustype vacst
    on usr.vacationstatus = vacst.id

left join cctl_languagetype lang
    on usr.language = lang.id

left join cctl_phonecountrycode defphctry
    on usr.defaultphonecountry = defphctry.id

left join cctl_systemusertype sysusr
    on usr.systemusertype = sysusr.id

left join cctl_country defctry
    on usr.defaultcountry = defctry.id
)
select 
    user_sk,
    src_system_cd,
    src_ext_user_id,
    src_user_id,
    user_first_name,
    user_middle_name,
    user_last_name,
    user_phone_nbr,
    user_email_address,
    external_user_ind,
    okta_id,
    user_job_title_txt,
    vacation_status_type_cd,
    vacation_status_type_name,
    user_language_cd,
    user_language_name,
    user_default_phone_ctry_cd,
    user_default_phone_ctry_name,
    system_user_type_cd,
    system_user_type_name,
    user_default_country_cd,
    user_default_country_name,
    managing_entity_id,
    user_active_ind,
    file_ingestion_timestamp
from
    cte_join
