{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Aspire - original table materialization
2026-04-20      1.0                             Converted to incremental with merge strategy

-#}

{{
  config(
    materialized='incremental',
    unique_key=['src_claim_id', 'treatment_public_id'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 08_CLAIM_MEDICAL_TREATMENT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A08
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_MEDICAL_TREATMENT
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    {% if is_incremental() %}
        WHERE file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

ccx_medpersontreatment_icare as (
    select
        id,
        publicid,
        claimid,
        subtype,
        category,
        paycodeid,
        contactid,
        icd1id,
        odgflag,
        approvalstatus,
        frequency,
        treatmenttype,
        requestdate,
        startdate,
        enddate,
        treatmentquantityrequested,
        description,
        odgmax,
        treatmentquantityapproved,
        dateapproved,
        frequencycount,
        cost,
        hourlycost,
        hours,
        totalcost,
        impinclude,
        createtime,
        updatetime
    from {{ ref('v_ccx_medpersontreatment_icare_current') }}
    where retired = 0
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
        paymentsubtype
    from {{ ref('v_ccx_paycode_icare_current') }}
    where retired = 0
),

cc_contact as (
    select
        id,
        name,
        firstname,
        middlename,
        lastname
    from {{ ref('v_cc_contact_current') }}
    where retired = 0
),

cc_icdcode as (
    select
        id,
        code
    from {{ ref('v_cc_icdcode_current') }}
    where retired = 0
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
),

cte_join AS 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as source_system,
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
    CAST(med.dateapproved AS TIMESTAMP_NTZ) as treatment_approved_dttm,
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
    CAST(med.createtime AS TIMESTAMP_NTZ) as treatment_request_create_dttm,
    cast(med.createtime AS date) as treatment_request_create_dt,
    CAST(med.updatetime AS TIMESTAMP_NTZ) as treatment_request_update_dttm,
    cast(med.updatetime AS date) as treatment_request_update_dt,
    clm.file_ingestion_timestamp
from cc_claim clm
left join ccx_medpersontreatment_icare med
    on clm.id = med.claimid
left join cctl_medentitytreatment_icare dim_med
    on med.subtype = dim_med.id
left join cctl_medtreatsubserv_icare dim_medcat
    on med.category = dim_medcat.id
left join ccx_paycode_icare pay
    on med.paycodeid = pay.id
left join cc_contact con
    on med.contactid = con.id
left join cc_icdcode icd
    on med.icd1id = icd.id
left join cctl_odgflag_icare odgflag
    on med.odgflag = odgflag.id
left join cctl_approvalstatus_icare dim_approval
    on med.approvalstatus = dim_approval.id
left join cctl_frequency_icare dim_freq
    on med.frequency = dim_freq.id
)
select 
    claim_sk,
    source_system,
    claim_nbr,
    src_claim_id,
    treatment_public_id,
    treatment_group_cd,
    treatment_group_desc,
    treatment_type,
    treatment_category_cd,
    treatment_category_desc,
    treatment_pay_cd,
    treatment_pay_desc,
    treatment_request_dt,
    treatment_provider_name,
    treatment_start_dt,
    treatment_end_dt,
    treatment_sessions_requested,
    treatment_description,
    treatment_icd,
    odg_flag_cd,
    odg_flag_desc,
    odg_max,
    treatment_approval_status,
    treatment_sessions_approved,
    treatment_approved_dttm,
    treatment_approved_dt,
    domestic_personal_care_frequenc1,
    domestic_personal_care_frequenc2,
    treatment_unit_cost,
    treatment_hourly_cost,
    treatment_hours,
    treatment_total_cost,
    include_on_imp_ind,
    treatment_request_create_dttm,
    treatment_request_create_dt,
    treatment_request_update_dttm,
    treatment_request_update_dt,
    file_ingestion_timestamp
from
    cte_join