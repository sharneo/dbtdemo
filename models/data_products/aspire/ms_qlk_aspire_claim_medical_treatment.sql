{{
  config(
    materialized='incremental',
    unique_key='treatment_public_id',
    incremental_strategy='merge'
  )
}}

{#
  Source: 08_CLAIM_MEDICAL_TREATMENT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A08
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_MEDICAL_TREATMENT

  Version History:
  0.1  19/06/19  V.Lau  Initial Draft
  0.2  21/06/19  V.Lau  Added TREATMENT_PUBLIC_ID
  0.3  27/06/19  V.Lau  Added TREATMENT_REQUEST_UPDATE_DTTM and DT
#}

with cc_claim as (
    select
        id,
        claimnumber,
        retired
    from {{ ref('v_cc_claim_current') }}
),
ccx_medpersontreatment_icare as (
    select
        id,
        publicid,
        claimid,
        subtype,
        category,
        paycodeid,
        requestdate,
        startdate,
        enddate,
        treatmentquantityrequested,
        description,
        icd1id,
        odgflag,
        approvalstatus,
        frequency,
        treatmentquantityapproved,
        dateapproved,
        cost,
        hourlycost,
        hours,
        totalcost,
        impinclude,
        createtime,
        updatetime,
        contactid,
        retired,
        treatmenttype,
        file_ingestion_timestamp
    from {{ ref('v_ccx_medpersontreatment_icare_current') }}
),
cctl_medentitytreatment_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_medentitytreatment_icare_current') }}
),
cctl_medtreatsubserv_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_medtreatsubserv_icare_current') }}
),
ccx_paycode_icare as (
    select
        id,
        paycode,
        paymentsubtype,
        retired
    from {{ ref('v_ccx_paycode_icare_current') }}
),
cc_contact as (
    select
        id,
        firstname,
        middlename,
        lastname,
        name,
        retired
    from {{ ref('v_cc_contact_current') }}
),
cc_icdcode as (
    select
        id,
        code,
        retired
    from {{ ref('v_cc_icdcode_current') }}
),
cctl_odgflag_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_odgflag_icare_current') }}
),
cctl_approvalstatus_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_approvalstatus_icare_current') }}
),
cctl_frequency_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_frequency_icare_current') }}
)

select
    md5('GWCC' || clm.claimnumber) as claim_sk,
    'GWCC' as source_system,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    med.publicid as treatment_public_id,
    dim_med.typecode as treatment_group_cd,
    dim_med.name as treatment_group_desc,
    med.treatmenttype as treatment_type,
    dim_medcat.typecode as treatment_category_cd,
    dim_medcat.name as treatment_category_desc,
    pay.paycode as treatment_pay_cd,
    pay.paymentsubtype as treatment_pay_desc,
    cast(med.requestdate as date) as treatment_request_dt,
    case
        when con.name is null then concat(con.firstname, ' ', con.middlename, ' ', con.lastname)
        else con.name
    end as treatment_provider_name,
    cast(med.startdate as date) as treatment_start_dt,
    cast(med.enddate as date) as treatment_end_dt,
    med.treatmentquantityrequested as treatment_sessions_requested,
    med.description as treatment_description,
    icd.code as treatment_icd,
    odgflag.typecode as odg_flag_cd,
    odgflag.name as odg_flag_desc,
    med.odgmax as odg_max,
    dim_approval.name as treatment_approval_status,
    med.treatmentquantityapproved as treatment_sessions_approved,
    med.dateapproved as treatment_approved_dttm,
    cast(med.dateapproved as date) as treatment_approved_dt,
    dim_freq.name as domestic_personal_care_frequenc1,
    med.frequencycount as domestic_personal_care_frequenc2,
    med.cost as treatment_unit_cost,
    med.hourlycost as treatment_hourly_cost,
    med.hours as treatment_hours,
    med.totalcost as treatment_total_cost,
    case
        when med.impinclude = 1 then 'Y'
        when med.impinclude = 0 then 'N'
        else null
    end as include_on_imp_ind,
    med.createtime as treatment_request_create_dttm,
    cast(med.createtime as date) as treatment_request_create_dt,
    med.updatetime as treatment_request_update_dttm,
    cast(med.updatetime as date) as treatment_request_update_dt,
    med.file_ingestion_timestamp

from cc_claim clm

left join ccx_medpersontreatment_icare med
    on clm.id = med.claimid
    and med.retired = 0

left join cctl_medentitytreatment_icare dim_med
    on med.subtype = dim_med.id

left join cctl_medtreatsubserv_icare dim_medcat
    on med.category = dim_medcat.id

left join ccx_paycode_icare pay
    on med.paycodeid = pay.id
    and pay.retired = 0

left join cc_contact con
    on med.contactid = con.id
    and con.retired = 0

left join cc_icdcode icd
    on med.icd1id = icd.id
    and icd.retired = 0

left join cctl_odgflag_icare odgflag
    on med.odgflag = odgflag.id

left join cctl_approvalstatus_icare dim_approval
    on med.approvalstatus = dim_approval.id

left join cctl_frequency_icare dim_freq
    on med.frequency = dim_freq.id

order by clm.claimnumber, dim_med.typecode, med.requestdate

{% if is_incremental() %}
and med.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}
