{{
  config(
    materialized='incremental',
    unique_key='src_claim_payment_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 09_CLAIM_PAYMENT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A09
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_PAYMENT
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cc_check as (
    select
        id,
        claimid,
        transactionsetid,
        checksetid,
        publicid,
        reportableamt,
        claimcontactid,
        paymentmethod,
        scheduledsenddateext_icare,
        issuedate,
        createtime,
        paymentsource_icare
    from {{ ref('v_cc_check_current') }}
    where retired = 0
),

cc_transactionset as (
    select
        id,
        claimid,
        approvaldate,
        approvalstatus
    from {{ ref('v_cc_transactionset_current') }}
    where retired = 0
),

cctl_paymentmethod as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_paymentmethod_current') }}
),

cctl_approvalstatus as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_approvalstatus_current') }}
)

select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as source_system,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    chq.id as src_claim_payment_id,
    chq.publicid as claim_payment_public_id,
    chq.reportableamt as payment_amount,
    chq.scheduledsenddateext_icare as scheduled_send_dttm,
    cast(chq.scheduledsenddateext_icare as date) as scheduled_send_dt,
    chq.issuedate as payment_issue_dttm,
    cast(chq.issuedate as date) as payment_issue_dt,
    chq.createtime as src_create_dttm,
    cast(chq.createtime as date) as src_create_dt,
    pymtmethod.typecode as payment_method_cd,
    pymtmethod.name as payment_method_desc,
    tset.approvaldate as txn_approval_dttm,
    cast(tset.approvaldate as date) as txn_approval_dt,
    apprstus.typecode as approval_status_cd,
    apprstus.name as approval_status_desc,
    chq.paymentsource_icare as payment_source,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_claim clm

inner join cc_check chq
    on clm.id = chq.claimid

inner join cc_transactionset tset
    on chq.checksetid = tset.id

left join cctl_paymentmethod pymtmethod
    on chq.paymentmethod = pymtmethod.id

left join cctl_approvalstatus apprstus
    on tset.approvalstatus = apprstus.id

{% if is_incremental() %}
where clm.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}
