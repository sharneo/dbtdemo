{{
  config(
    materialized='incremental',
    unique_key='claim_payment_sk, src_payee_contact_id',
    incremental_strategy='merge'
  )
}}

{#
  Source: 11_CLAIM_PAYMENT_PAYEE.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A11
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_PAYMENT_PAYEE

  Version History:
  0.1  2019-06-28  V.Lau  Initial Draft
  0.2  2020-05-14  Yu Yain  EST10-495: Added Payee_TFN, TFN_Declaration_Date and Age indicator
  0.3  2020-05-28  Yu Yain  EST10-517: Added Contact_Prohibited_IND and Bank_Details_Present_IND
  0.4  2020-08-25  V.Lau  EST10-946: Added hashed TFN value
  0.5  2024-06-07  E.McGregor  TDAREP-4517 and TDAREP-4518: Added EFT_CREATE_DT
  0.6  2024-10-24  Theo V.  TDAREP-4119 Added payee Age Banding, removed EFT_CREATE_DT
#}

with cc_check as (
    select
        id,
        publicid,
        claimid,
        retired,
        file_ingestion_timestamp
    from {{ ref('v_cc_check_current') }}
),
cc_claim as (
    select
        id,
        claimnumber,
        retired
    from {{ ref('v_cc_claim_current') }}
),
cc_checkpayee as (
    select
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
        dateofbirth,
        retired
    from {{ ref('v_cc_contact_current') }}
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
        claimid,
        contactid,
        contactprohibited,
        retired
    from {{ ref('v_cc_claimcontact_current') }}
),
cc_eftdata as (
    select
        contactid,
        bankaccountnumber,
        bankroutingnumber,
        isprimary,
        createtime,
        id,
        retired
    from {{ ref('v_cc_eftdata_current') }}
)

select
    md5('GWCC' || clm.claimnumber) as claim_sk,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    md5('GWCC' || chq.publicid) as claim_payment_sk,
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
        when con.taxid in ('000 000 000000 000 000', '111 111 111111 111 111', '333 333 333333 333 333', '444 444 444444 444 444', '123 456 789987 654 321') then right(con.taxid, 11)
        else md5(upper(rtrim(ltrim(con.taxid))))
    end as payee_tfn,
    con.tfndeclarationdate_icare as tfn_declaration_dt,
    case
        when con.dateofbirth is null then null
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 14 then '14 and below'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 19 then '15-19'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 24 then '20-24'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 29 then '25-29'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 34 then '30-34'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 39 then '35-39'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 44 then '40-44'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 49 then '45-49'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 54 then '50-54'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 59 then '55-59'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 64 then '60-64'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 69 then '65-69'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 74 then '70-74'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 79 then '75-79'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) <= 84 then '80-84'
        when floor(datediff(day, con.dateofbirth, current_timestamp()) / 365.25) >= 85 then '85 and over'
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
    chq.file_ingestion_timestamp

from cc_check chq

inner join cc_claim clm
    on chq.claimid = clm.id
    and clm.retired = 0

inner join cc_checkpayee chq_payee
    on chq_payee.checkid = chq.id

inner join cc_contact con
    on chq_payee.payeedenormid = con.id
    and con.retired = 0

inner join cctl_contactrole dim_conrole
    on chq_payee.payeetype = dim_conrole.id

left join cc_claimcontact clmctt
    on clmctt.claimid = clm.id
    and clmctt.contactid = con.id
    and clmctt.retired = 0

left join (
    select eft.contactid, eft.bankaccountnumber
    from (
        select
            contactid,
            bankaccountnumber,
            row_number() over (
                partition by contactid
                order by createtime desc, id desc
            ) as eftrank
        from cc_eftdata
        where retired = 0
        and isprimary = 1
        and bankroutingnumber is not null
        and bankaccountnumber is not null
    ) eft
    where eft.eftrank = 1
) bnk
    on bnk.contactid = con.id

where chq.retired = 0

{% if is_incremental() %}
and chq.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}
