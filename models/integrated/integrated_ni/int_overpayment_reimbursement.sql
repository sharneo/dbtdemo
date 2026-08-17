{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for overpayment reimbursement.

-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_reimbursement_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 41_OVERPAYMENT_REIMBURSEMENT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A41
  TBL_NM: MSC_QLK_ASPIRE_OVERPAYMENT_REIMBURSEMENT
-#}

with cc_claim as (
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

ccx_checkreversal_icare as (
    select
        id,
        publicid,
        claim,
        invoicenumber,
        status,
        invoicedate,
        duedate,
        createtime,
        updatetime,
        paymenttype,
        waivedflag,
        totalreimbursementamount,
        paymentamountreceivedtodate,
        paymentamountunallocated,
        writeoffamountreceivedtodate,
        writeoffamountunallocated,
        payeeinstructionsdetails
    from {{ ref('v_ccx_checkreversal_icare_current') }}
    where retired = 0
        and invoicedate is not null
),

cctl_overpmtrbmntstatus_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_overpmtrbmntstatus_icare_current') }}
    where retired = 0
),

cctl_paymenttype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_paymenttype_icare_current') }}
    where retired = 0
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'chqrev.publicid'
    ]) }} as varchar(150)) as claim_reimbursement_sk,
    chqrev.publicid as reimbursement_id,
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.id as src_claim_id,
    clm.claimnumber as claim_nbr,
    chqrev.id as src_reimbursement_id,
    chqrev.invoicenumber as reimbursement_invoice_nbr,
    chqrevsta.typecode as reimbursement_status_cd,
    chqrevsta.name as reimbursement_status_desc,
    cast(chqrev.invoicedate as date) as invoice_dt,
    CAST(chqrev.duedate as TIMESTAMP_NTZ) reimbursement_due_dttm,
    CAST(chqrev.createtime  as TIMESTAMP_NTZ) as src_create_dttm,
    CAST(chqrev.updatetime as TIMESTAMP_NTZ) as src_update_dttm,
    pytype.typecode as payment_type_cd,
    pytype.name as payment_type_desc,
    case when chqrev.waivedflag = 1 then 'Y' else 'N' end as waived_ind,
    chqrev.totalreimbursementamount as total_reimbursement_amt,
    chqrev.paymentamountreceivedtodate as payment_amt_received_to_date,
    chqrev.paymentamountunallocated as payment_amt_unallocated,
    chqrev.writeoffamountreceivedtodate as writeoff_amt_received_to_date,
    chqrev.writeoffamountunallocated as writeoff_amt_unallocated,
    chqrev.payeeinstructionsdetails as payee_instructions,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from ccx_checkreversal_icare chqrev

inner join cc_claim clm
    on clm.id = chqrev.claim

left join cctl_overpmtrbmntstatus_icare chqrevsta
    on chqrevsta.id = chqrev.status

left join cctl_paymenttype_icare pytype
    on pytype.id = chqrev.paymenttype
)
select
    claim_reimbursement_sk,
    reimbursement_id,
    claim_sk,
    src_claim_id,
    claim_nbr,
    src_reimbursement_id,
    reimbursement_invoice_nbr,
    reimbursement_status_cd,
    reimbursement_status_desc,
    invoice_dt,
    reimbursement_due_dttm,
    src_create_dttm,
    src_update_dttm,
    payment_type_cd,
    payment_type_desc,
    waived_ind,
    total_reimbursement_amt,
    payment_amt_received_to_date,
    payment_amt_unallocated,
    writeoff_amt_received_to_date,
    writeoff_amt_unallocated,
    payee_instructions,
    file_ingestion_timestamp
from
    cte_join