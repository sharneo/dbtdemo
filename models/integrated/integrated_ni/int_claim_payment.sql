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

with cc_check as (
    select
        id,
        publicid,
        claimid,
        checknumber,
        invoicenumber,
        checksetid,
        payto,
        issuedate,
        lastpostingdate_icare,
        datepresented_icare,
        scheduledsenddate,
        datepaymenttransacted_icare,
        dateofservice,
        servicepdstart,
        servicepdend,
        updatetime,
        createtime,
        createuserid,
        registeredforgst_icare,
        paymentmethod,
        gstmethodpel_ext,
        checktype,
        status,
        weeklybenefitpayeetype_icare,
        portionid,
        dbnetamount_icare,
        memo,
        loadcommandid,
        paymentsource_icare,
        ocr_invoice_icare,
        camtvoiddate_icare,
        westpacid_icare,
        voidedpaymentnumber_icare,
        rejectionreason_ext,
        file_ingestion_timestamp
    from {{ ref('v_cc_check_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_claim as (
    select
        id,
        claimnumber,
        source_system
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cc_transaction as (
    select
        id,
        checkid,
        costcategory,
        transactionsetid
    from {{ ref('v_cc_transaction_current') }}
    where retired = 0
),

cctl_costcategory_wb as (
    select
        id
    from {{ ref('v_cctl_costcategory_current') }}
    where typecode = '50'
      and retired = 0
),

cc_transactionlineitem as (
    select
        transactionid,
        dateto_icare,
        createtime
    from {{ ref('v_cc_transactionlineitem_current') }}
    where retired = 0
),

cc_transactionset as (
    select
        id,
        transfertoclaimnumber_icare,
        voidtransactionreason_icare
    from {{ ref('v_cc_transactionset_current') }}
    where retired = 0
),

cc_checkportion as (
    select
        id,
        fixedclaimamount
    from {{ ref('v_cc_checkportion_current') }}
    where retired = 0
),

cc_deduction as (
    select
        checkid,
        transactionamount
    from {{ ref('v_cc_deduction_current') }}
    where retired = 0
),

cctl_paymentmethod as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_paymentmethod_current') }}
),

cctl_gstmethodpel_ext as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_gstmethodpel_ext_current') }}
),

cctl_checktype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_checktype_current') }}
),

cctl_transactionstatus as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_transactionstatus_current') }}
),

cctl_weekbenpayeetype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_weekbenpayeetype_icare_current') }}
),

ccx_checkreissuanceinfo_icare as (
    select
        rootcheckid,
        reissuedtocheckid
    from {{ ref('v_ccx_checkreissuanceinfo_icare_current') }}
    where retired = 0
),

cc_check_reissued as (
    select
        id,
        checknumber
    from {{ ref('v_cc_check_current') }}
),

cc_activity as (
    select
        transactionsetid,
        claimid,
        activitypatternid
    from {{ ref('v_cc_activity_current') }}
    where retired = 0
),

cc_activitypattern as (
    select
        id
    from {{ ref('v_cc_activitypattern_current') }}
    where code = 'approve_payment'
      and retired = 0
),

cctl_transactionreason_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_transactionreason_icare_current') }}
),

cte_wb as (
    select
        chq_wb.claimid,
        chq_wb.id as checkid,
        row_number() over (
            partition by chq_wb.claimid
            order by ln.dateto_icare desc, ln.createtime desc
        ) as latest_wb
    from cc_check chq_wb
    inner join cc_transaction trn_wb
        on trn_wb.checkid = chq_wb.id
    inner join cctl_costcategory_wb cstcatg
        on cstcatg.id = trn_wb.costcategory
    inner join cc_transactionlineitem ln
        on ln.transactionid = trn_wb.id
    where chq_wb.status in (2, 5)
),

approve_payment_exists as (
    select distinct
        act.transactionsetid,
        act.claimid
    from cc_activity act
    inner join cc_activitypattern actpattern
        on act.activitypatternid = actpattern.id
),

final as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'chq.publicid'
        ]) }} as varchar(150)) as claim_payment_sk,
        chq.publicid as claim_payment_id,
        chq.id as src_claim_payment_id,
        chq.checknumber as payment_nbr,
        chq.invoicenumber as invoice_nbr,
        chq.checksetid as claim_txn_set_id,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        case
            when ape.transactionsetid is not null then 'N'
            else 'Y'
        end as auto_payment_ind,
        chq.payto as payee,
        CAST(chq.issuedate as TIMESTAMP_NTZ)  as payment_issue_dttm,
        cast(chq.issuedate as date) as payment_issue_dt,
        CAST(chq.lastpostingdate_icare AS TIMESTAMP_NTZ) as last_posting_dttm,
        cast(chq.lastpostingdate_icare as date) as last_posting_dt,
        cast(chq.datepresented_icare as date) as payment_presented_dt,
        CAST(chq.scheduledsenddate AS TIMESTAMP_NTZ) as  payment_scheduled_send_dt,
        CAST(chq.datepaymenttransacted_icare AS TIMESTAMP_NTZ) as payment_transacted_dt,
        CAST(chq.dateofservice AS TIMESTAMP_NTZ) as service_provision_dt,
        CAST(chq.servicepdstart AS TIMESTAMP_NTZ) as service_period_start_dt,
        CAST(chq.servicepdend AS TIMESTAMP_NTZ) as service_period_end_dt,
        CAST(chq.lastpostingdate_icare AS TIMESTAMP_NTZ) as payment_dt,
        CAST(chq.updatetime AS TIMESTAMP_NTZ) as src_eff_dttm,
        cast(chq.updatetime as date) as src_eff_dt,
        CAST(chq.createtime AS TIMESTAMP_NTZ) AS  src_create_dttm,
        cast(chq.createtime as date) as src_create_dt,
        chq.createuserid as create_src_user_id,
        case
            when chq.registeredforgst_icare = 1 then 'Y'
            else 'N'
        end as payee_registered_for_gst_ind,
        chqtyp.typecode as payment_ordinality_cd,
        chqtyp.name as payment_ordinality_desc,
        paymth.typecode as payment_method_cd,
        paymth.name as payment_method_desc,
        gstmth.typecode as payment_gst_method_cd,
        gstmth.name as payment_gst_method_desc,
        paystus.typecode as payment_status_cd,
        paystus.name as payment_status_desc,
        payeetyp.typecode as weekly_benefit_payee_type_cd,
        payeetyp.name as weekly_benefit_payee_type_desc,
        coalesce(chqptn.fixedclaimamount, 0) as payment_portion_amt,
        coalesce(ded.transactionamount, 0) as payg_amt,
        coalesce(chq.dbnetamount_icare, 0) as payment_amt,
        chq.memo as solv_unique_payment_id,
        case
            when wb.latest_wb is not null then 'Y'
            else null
        end as latest_weekly_benefit_ind,
        case
            when chq.loadcommandid is null then 'N'
            else 'Y'
        end as migrated_pymt_ind,
        case
            when chq.paymentsource_icare = 'MP' then 'Y'
            else 'N'
        end as digital_payment_ind,
        chq.ocr_invoice_icare as src_invoice_id,
        cast(chq.camtvoiddate_icare as date) as camt_void_dt,
        chq.westpacid_icare as westpac_id,
        reichq.checknumber as reissued_payment_nbr,
        chq.voidedpaymentnumber_icare as voided_payment_nbr,
        chq.rejectionreason_ext as payment_rejection_reason,
        rsn.typecode as void_reason_cd,
        rsn.name as void_reason_desc,
        tset.transfertoclaimnumber_icare as pymt_transferred_to_claim,
        chq.file_ingestion_timestamp
    from cc_check chq

    inner join cc_claim clm
        on chq.claimid = clm.id

    left join cc_transactionset tset
        on tset.id = chq.checksetid

    left join cc_checkportion chqptn
        on chq.portionid = chqptn.id

    left join cc_deduction ded
        on chq.id = ded.checkid

    left join cctl_paymentmethod paymth
        on chq.paymentmethod = paymth.id

    left join cctl_gstmethodpel_ext gstmth
        on chq.gstmethodpel_ext = gstmth.id

    left join cctl_checktype chqtyp
        on chq.checktype = chqtyp.id

    left join cctl_transactionstatus paystus
        on chq.status = paystus.id

    left join cctl_weekbenpayeetype_icare payeetyp
        on chq.weeklybenefitpayeetype_icare = payeetyp.id

    left join ccx_checkreissuanceinfo_icare rei
        on rei.rootcheckid = chq.id

    left join cc_check_reissued reichq
        on reichq.id = rei.reissuedtocheckid

    left join cte_wb wb
        on wb.checkid = chq.id
        and wb.latest_wb = 1

    left join cctl_transactionreason_icare rsn
        on rsn.id = tset.voidtransactionreason_icare

    left join approve_payment_exists ape
        on chq.checksetid = ape.transactionsetid
        and clm.id = ape.claimid
)

select 
    claim_payment_sk,
    claim_payment_id,
    src_claim_payment_id,
    payment_nbr,
    invoice_nbr,
    claim_txn_set_id,
    claim_sk,
    claim_nbr,
    src_claim_id,
    auto_payment_ind,
    payee,
    payment_issue_dttm,
    payment_issue_dt,
    last_posting_dttm,
    last_posting_dt,
    payment_presented_dt,
    payment_scheduled_send_dt,
    payment_transacted_dt,
    service_provision_dt,
    service_period_start_dt,
    service_period_end_dt,
    payment_dt,
    src_eff_dttm,
    src_eff_dt,
    src_create_dttm,
    src_create_dt,
    create_src_user_id,
    payee_registered_for_gst_ind,
    payment_ordinality_cd,
    payment_ordinality_desc,
    payment_method_cd,
    payment_method_desc,
    payment_gst_method_cd,
    payment_gst_method_desc,
    payment_status_cd,
    payment_status_desc,
    weekly_benefit_payee_type_cd,
    weekly_benefit_payee_type_desc,
    payment_portion_amt,
    payg_amt,
    payment_amt,
    solv_unique_payment_id,
    latest_weekly_benefit_ind,
    migrated_pymt_ind,
    digital_payment_ind,
    src_invoice_id,
    camt_void_dt,
    westpac_id,
    reissued_payment_nbr,
    voided_payment_nbr,
    payment_rejection_reason,
    void_reason_cd,
    void_reason_desc,
    pymt_transferred_to_claim,
    file_ingestion_timestamp
from    
    final