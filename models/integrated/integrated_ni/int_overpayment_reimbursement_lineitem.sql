{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for overpayment reimbursement lineitem.
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_reimb_lineitem_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 38_OVERPAYMENT_REIMBURSEMENT_LINEITEM.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A38
  TBL_NM: MSC_QLK_ASPIRE_OVERPAYMENT_REIMBURSEMENT_LINEITEM
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

ccx_reimbtotranslineitems as (
    select
        id,
        publicid,
        overpaymentreimbursement_icare,
        transactionlineitem,
        reimbursementamount,
        paymentamountallocate,
        writeoffamountallocate,
        creditamount,
        waived
    from {{ ref('v_ccx_reimbtotranslineitems_current') }}
    where retired = 0
),

cc_transactionlineitem as (
    select
        id,
        transactionid
    from {{ ref('v_cc_transactionlineitem_current') }}
    where retired = 0
),

cc_transaction as (
    select
        id,
        claimid,
        checkid
    from {{ ref('v_cc_transaction_current') }}
    where retired = 0
),

cc_check as (
    select
        id,
        invoicenumber,
        checknumber
    from {{ ref('v_cc_check_current') }}
    where retired = 0
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'reimbln.publicid'
    ]) }} as varchar(150)) as claim_reimb_lineitem_sk,
    reimbln.publicid as reimb_lineitem_id,
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    reimbln.id as src_reimb_lineitem_id,
    reimbln.overpaymentreimbursement_icare as src_reimbursement_id,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    trnln.id as txn_lineitem_id,
    reimbln.reimbursementamount as recovery_amt,
    reimbln.paymentamountallocate as received_amt,
    reimbln.writeoffamountallocate as write_off_amt,
    reimbln.creditamount as credit_amt,
    case when reimbln.waived = 1 then 'Y' else 'N' end as waived_ind,
    chq.invoicenumber as lineitem_invoice_nbr,
    case
        when reimbln.waived = 1 then 'Waived'
        when reimbln.creditamount > 0 then 'Credit'
        else ''
    end as credit_type,
    chq.checknumber as payment_nbr,
    clm.file_ingestion_timestamp
from ccx_reimbtotranslineitems reimbln
inner join cc_transactionlineitem trnln
    on trnln.id = reimbln.transactionlineitem
inner join cc_transaction trn
    on trn.id = trnln.transactionid
inner join cc_claim clm
    on clm.id = trn.claimid
left join cc_check chq
    on chq.id = trn.checkid
)
select 
    claim_reimb_lineitem_sk,
    reimb_lineitem_id,
    claim_sk,
    src_reimb_lineitem_id,
    src_reimbursement_id,
    claim_nbr,
    src_claim_id,
    txn_lineitem_id,
    recovery_amt,
    received_amt,
    write_off_amt,
    credit_amt,
    waived_ind,
    lineitem_invoice_nbr,
    credit_type,
    payment_nbr,
    file_ingestion_timestamp
from
    cte_join