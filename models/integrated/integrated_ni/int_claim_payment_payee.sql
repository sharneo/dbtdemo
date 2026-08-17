{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for claim payment payee.
                                                claim_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{
  config(
    materialized='incremental',
    unique_key=['src_claim_payment_id', 'src_payee_contact_id'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 11_CLAIM_PAYMENT_PAYEE.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A11
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_PAYMENT_PAYEE
-#}

with cc_check as (
    select
        id,
        claimid,
        publicid
    from {{ ref('v_cc_check_current') }}
    where retired = 0
),

cc_claim as (
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

cc_checkpayee as (
    select
        id,
        checkid,
        payeedenormid,
        payeetype
    from {{ ref('v_cc_checkpayee_current') }}
),

cc_contact as (
    select
        id,
        publicid,
        name,
        firstname,
        middlename,
        lastname,
        taxid,
        tfndeclarationdate_icare,
        dateofbirth
    from {{ ref('v_cc_contact_current') }}
    where retired = 0
),

cctl_contactrole as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_contactrole_current') }}
),

cc_claimcontact as (
    select
        id,
        claimid,
        contactid,
        contactprohibited
    from {{ ref('v_cc_claimcontact_current') }}
    where retired = 0
),

cc_eftdata as (
    select
        id,
        contactid,
        bankaccountnumber,
        bankroutingnumber,
        isprimary,
        createtime
    from {{ ref('v_cc_eftdata_current') }}
    where retired = 0
        and isprimary = 1
        and bankroutingnumber is not null
        and bankaccountnumber is not null
),

cte_eft_ranked as (
    select
        contactid,
        bankaccountnumber,
        row_number() over (partition by contactid order by createtime desc, id desc) as eft_rank
    from cc_eftdata
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
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'chq.publicid'
    ]) }} as varchar(150)) as claim_payment_sk,
    chq.publicid as claim_payment_id,
    chq.id as src_claim_payment_id,
    con.publicid as src_payee_contact_id,
    dim_conrole.typecode as payee_type_cd,
    dim_conrole.name as payee_type_desc,
    case
        when con.name is not null then con.name
        when con.firstname is not null then concat(con.firstname, ' ', con.middlename, ' ', con.lastname)
        else 'No Name'
    end as payee_name,
    case
        when con.taxid is null then null
        when con.taxid in ('000 000 000000 000 000', '111 111 111111 111 111', '333 333 333333 333 333',
                           '444 444 444444 444 444', '123 456 789987 654 321') then right(con.taxid, 11)
        else md5(upper(trim(con.taxid)))
    end as payee_tfn,
    CAST(con.tfndeclarationdate_icare AS TIMESTAMP_NTZ) as tfn_declaration_dt,
    case
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) is null then null
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 14 then '14 and below'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 19 then '15-19'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 24 then '20-24'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 29 then '25-29'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 34 then '30-34'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 39 then '35-39'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 44 then '40-44'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 49 then '45-49'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 54 then '50-54'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 59 then '55-59'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 64 then '60-64'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 69 then '65-69'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 74 then '70-74'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 79 then '75-79'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) <= 84 then '80-84'
        when floor(datediff(day, con.dateofbirth, current_date()) / 365.25) >= 85 then '85 and over'
    end as age_band,
    case
        when con.dateofbirth is null then null
        when cast(
            case
                when month(con.dateofbirth) = 2 and day(con.dateofbirth) = 29
                then dateadd(day, 1, dateadd(year, 18, con.dateofbirth))
                else dateadd(year, 18, con.dateofbirth)
            end as date
        ) <= current_date() then 'Y'
        else 'N'
    end as age_18_and_over_ind,
    case
        when clmctt.contactprohibited = 1 then 'Y'
        else 'N'
    end as contact_prohibited_ind,
    case
        when bnk.bankaccountnumber is not null then 'Y'
        else 'N'
    end as bank_details_present_ind,
    current_date() as extract_date,
    clm.file_ingestion_timestamp
from cc_check chq
inner join cc_claim clm
    on chq.claimid = clm.id
inner join cc_checkpayee chq_payee
    on chq_payee.checkid = chq.id
inner join cc_contact con
    on chq_payee.payeedenormid = con.id
inner join cctl_contactrole dim_conrole
    on chq_payee.payeetype = dim_conrole.id
left join cc_claimcontact clmctt
    on clmctt.claimid = clm.id
    and clmctt.contactid = con.id
left join cte_eft_ranked bnk
    on bnk.contactid = con.id
    and bnk.eft_rank = 1
)

select  
    claim_sk,
    claim_nbr,
    src_claim_id,
    claim_payment_sk,
    claim_payment_id,
    src_claim_payment_id,
    src_payee_contact_id,
    payee_type_cd,
    payee_type_desc,
    payee_name,
    payee_tfn,
    tfn_declaration_dt,
    age_band,
    age_18_and_over_ind,
    contact_prohibited_ind,
    bank_details_present_ind,
    file_ingestion_timestamp
from    
    cte_join