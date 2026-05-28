{{
  config(
    materialized='incremental',
    unique_key='claim_txn_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 14_CLAIM_TXN.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A14
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_TXN
-#}

with cc_transaction as (
    select
        id,
        publicid,
        claimid,
        checkid,
        transactionsetid,
        subtype,
        status,
        lifecyclestate,
        costtype,
        costcategory,
        paymenttype,
        submitdate,
        loadcommandid,
        file_ingestion_timestamp
    from {{ ref('v_cc_transaction_current') }}
    where retired = 0
),

cc_claim as (
    select
        id,
        claimnumber,
        source_system
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cctl_transaction as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_transaction_current') }}
),

cc_transactionset as (
    select
        id,
        approvaldate,
        approvalstatus,
        paymenttype_icare,
        adjustmentpayment_icare,
        requestinguserid
    from {{ ref('v_cc_transactionset_current') }}
    where retired = 0
),

cctl_transactionstatus as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_transactionstatus_current') }}
),

cctl_transactionlifecyclestate as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_transactionlifecyclestate_current') }}
),

cctl_approvalstatus as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_approvalstatus_current') }}
),

cctl_costtype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_costtype_current') }}
),

cctl_costcategory as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_costcategory_current') }}
),

cctl_paymenttype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_paymenttype_current') }}
),

cctl_paymenttype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_paymenttype_icare_current') }}
),

cc_user as (
    select
        id,
        publicid
    from {{ ref('v_cc_user_current') }}
)

select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'trn.publicid'
    ]) }} as varchar(150)) as claim_txn_sk,
    trn.publicid as claim_txn_id,
    trn.id as src_claim_txn_id,
    tset.id as src_claim_txn_set_id,
    trn.checkid as src_claim_payment_id,
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    trn.submitdate as txn_submitted_dt,
    tset.approvaldate as txn_approval_dttm,
    cast(tset.approvaldate as date) as txn_approval_dt,
    apprstus.typecode as txn_approval_status_cd,
    apprstus.name as txn_approval_status_desc,
    trnstus.typecode as txn_status_cd,
    trnstus.name as txn_status_desc,
    trnlife.typecode as txn_lifecycle_state_cd,
    trnlife.name as txn_lifecycle_state_desc,
    dimtrn.typecode as txn_subtype_cd,
    dimtrn.name as txn_subtype_desc,
    cstcat.typecode as txn_cost_cat_cd,
    cstcat.name as txn_cost_cat_desc,
    dimcst.typecode as txn_cost_type_cd,
    dimcst.name as txn_cost_type_desc,
    dimpay.typecode as payment_type_cd,
    dimpay.name as payment_type_desc,
    dimpycat.typecode as payment_category_cd,
    dimpycat.name as payment_category_desc,
    case
        when tset.adjustmentpayment_icare = 1 then 'Y'
        else 'N'
    end as adj_payment_ind,
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'usr.publicid'
    ]) }} as varchar(150)) as payment_requester_user_sk,
    case
        when trn.loadcommandid is null then 'N'
        else 'Y'
    end as migrated_txn_ind,
    current_date() as extract_date,
    trn.file_ingestion_timestamp

from cc_transaction trn

inner join cc_claim clm
    on trn.claimid = clm.id

inner join cctl_transaction dimtrn
    on dimtrn.id = trn.subtype
    and dimtrn.typecode = 'payment'

inner join cc_transactionset tset
    on tset.id = trn.transactionsetid

left join cctl_transactionstatus trnstus
    on trnstus.id = trn.status

left join cctl_transactionlifecyclestate trnlife
    on trnlife.id = trn.lifecyclestate

left join cctl_approvalstatus apprstus
    on apprstus.id = tset.approvalstatus

left join cctl_costtype dimcst
    on dimcst.id = trn.costtype

left join cctl_costcategory cstcat
    on cstcat.id = trn.costcategory

left join cctl_paymenttype dimpay
    on dimpay.id = trn.paymenttype

left join cctl_paymenttype_icare dimpycat
    on dimpycat.id = tset.paymenttype_icare

left join cc_user usr
    on tset.requestinguserid = usr.id

{% if is_incremental() %}
where trn.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}
