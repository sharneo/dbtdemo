{{
  config(
    materialized='incremental',
    unique_key='claim_payment_sk',
    incremental_strategy='merge',
    tags=['business_critical', 'aspire']
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
        claimid,
        publicid,
        checknumber,
        invoicenumber,
        checksetid,
        retired,
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
),

cc_claim as (
    select
        id,
        claimnumber
    from {{ ref('v_cc_claim_current') }}
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
        id,
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

cc_check_reissue as (
    select
        id,
        checknumber
    from {{ ref('v_cc_check_current') }}
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
        chq.claimid,
        chq.id as checkid,
        row_number() over (
            partition by chq.claimid
            order by ln.dateto_icare desc, ln.createtime desc
        ) as latest_wb
    from {{ ref('v_cc_check_current') }} chq
    inner join {{ ref('v_cc_transaction_current') }} trn
        on trn.checkid = chq.id
        and trn.retired = 0
    inner join {{ ref('v_cctl_costcategory_current') }} cstcatg
        on cstcatg.id = trn.costcategory
        and cstcatg.typecode = '50'
        and cstcatg.retired = 0
    inner join {{ ref('v_cc_transactionlineitem_current') }} ln
        on ln.transactionid = trn.id
        and ln.retired = 0
    where chq.status in (2, 5)
),

cc_activity as (
    select
        id,
        claimid,
        activitypatternid,
        transactionsetid
    from {{ ref('v_cc_activity_current') }}
    where retired = 0
),

cc_activitypattern as (
    select
        id,
        code
    from {{ ref('v_cc_activitypattern_current') }}
    where retired = 0
)

select
    md5(concat('GWCC', chq.publicid)) as claim_payment_sk,
    'GWCC' as source_system,
    chq.publicid as claim_payment_id,
    chq.id as src_claim_payment_id,
    chq.checknumber as payment_nbr,
    chq.invoicenumber as invoice_nbr,
    chq.checksetid as claim_txn_set_id,
    md5(concat('GWCC', clm.claimnumber)) as claim_sk,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    case
        when exists (
            select 1
            from cc_activity act
            inner join cc_activitypattern actpattern
                on act.activitypatternid = actpattern.id
                and actpattern.code = 'approve_payment'
            where chq.checksetid = act.transactionsetid
                and act.claimid = clm.id
        ) then 'N'
        else 'Y'
    end as auto_payment_ind,
    chq.payto as payee,
    chq.issuedate as payment_issue_dttm,
    cast(chq.issuedate as date) as payment_issue_dt,
    chq.lastpostingdate_icare as last_posting_dttm,
    cast(chq.lastpostingdate_icare as date) as last_posting_dt,
    cast(chq.datepresented_icare as date) as payment_presented_dt,
    chq.scheduledsenddate as payment_scheduled_send_dt,
    chq.datepaymenttransacted_icare as payment_transacted_dt,
    chq.dateofservice as service_provision_dt,
    chq.servicepdstart as service_period_start_dt,
    chq.servicepdend as service_period_end_dt,
    chq.lastpostingdate_icare as payment_dt,
    chq.updatetime as src_eff_dttm,
    cast(chq.updatetime as date) as src_eff_dt,
    chq.createtime as src_create_dttm,
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
    current_date() as extract_date,
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

left join (
    ccx_checkreissuanceinfo_icare rei
    inner join cc_check_reissue reichq
        on reichq.id = rei.reissuedtocheckid
)
    on rei.rootcheckid = chq.id

left join cte_wb wb
    on wb.checkid = chq.id
    and wb.latest_wb = 1

left join cctl_transactionreason_icare rsn
    on rsn.id = tset.voidtransactionreason_icare

{% if is_incremental() %}
where chq.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% else %}
where 1=1
{% endif %}
