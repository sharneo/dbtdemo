{{
  config(
    materialized='incremental',
    unique_key='claim_txn_lineitem_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 17_CLAIM_TXN_LINEITEM.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A17
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_TXN_LINEITEM
-#}

with cc_transactionlineitem as (
    select
        publicid,
        id,
        transactionid,
        paycode_icareid,
        gstdecision,
        gstmethodpel_ext,
        datefrom_icare,
        dateto_icare,
        dateofservice_icare,
        updatetime,
        createtime,
        serviceproviderid_icare,
        hourspaid_icare,
        hourslost_icare,
        transactionamount,
        itcaaamountpel,
        reportingamount,
        reservingamount,
        weeklybenefitrate_icare,
        amountwithoutgst,
        gstcalcrate_icare,
        gstamountpel,
        deemedearningsperweek_icare,
        grossweeklywagerate_icare,
        nonpecuniarybenefitd_icare,
        earningse_icare,
        gstapplicable_icare,
        itcapplicable_icare,
        draftamount_ext,
        rehabservice_extid,
        retired,
        file_ingestion_timestamp
    from {{ ref('v_cc_transactionlineitem_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_transaction as (
    select
        id,
        transactionsetid,
        checkid,
        exposureid,
        claimid,
        costcategory,
        subtype,
        createuserid,
        recoverycategory,
        status,
        payerdenormid
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

cc_check as (
    select
        id,
        status
    from {{ ref('v_cc_check_current') }}
    where retired = 0
),

ccx_paycode_icare as (
    select
        id,
        paycode,
        paymentcategory,
        paymenttype,
        paymentsubtype
    from {{ ref('v_ccx_paycode_icare_current') }}
),

cctl_paycodegstdecision_icare as (
    select
        id
    from {{ ref('v_cctl_paycodegstdecision_icare_current') }}
),

cctl_costcategory as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_costcategory_current') }}
),

cc_transactionset as (
    select
        id,
        approvaldate,
        approvalstatus,
        requestinguserid,
        adjustmentpayment_icare,
        paymenttype_icare
    from {{ ref('v_cc_transactionset_current') }}
    where retired = 0
),

cctl_paymenttype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_paymenttype_icare_current') }}
),

cctl_approvalstatus as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_approvalstatus_current') }}
),

cctl_gstmethodpel_ext as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_gstmethodpel_ext_current') }}
),

cctl_transaction as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_transaction_current') }}
),

cc_exposure as (
    select
        id,
        claimorder,
        exposuretype,
        coverageid
    from {{ ref('v_cc_exposure_current') }}
    where retired = 0
),

cctl_exposuretype as (
    select
        id,
        l_en_au
    from {{ ref('v_cctl_exposuretype_current') }}
),

cc_coverage as (
    select
        id,
        type
    from {{ ref('v_cc_coverage_current') }}
    where retired = 0
),

cctl_coveragetype as (
    select
        id,
        l_en_au
    from {{ ref('v_cctl_coveragetype_current') }}
),

ccx_paycodegazettedrate_icare as (
    select
        paycode,
        rate,
        effectivedate,
        expirationdate
    from {{ ref('v_ccx_paycodegazettedrate_icare_current') }}
    where retired = 0
),

cctl_recoverycategory as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_recoverycategory_current') }}
),

cctl_transactionstatus_txn as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_transactionstatus_current') }}
),

cctl_transactionstatus_chq as (
    select
        id,
        typecode,
        description
    from {{ ref('v_cctl_transactionstatus_current') }}
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'trnln.publicid'
    ]) }} as varchar(150)) as claim_txn_lineitem_sk,
    trn.transactionsetid as src_txn_set_id,
    trnln.publicid as txn_lineitem_id,
    trnln.id as src_txn_lineitem_id,
    trnln.transactionid as src_txn_id,
    trn.checkid as src_claim_payment_id,
    trn.exposureid as src_claim_exposure_id,

    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,

    dim_txntype.typecode as txn_type_cd,
    dim_txntype.name as txn_type_desc,

    dim_paytype.typecode as payment_type_cd,
    dim_paytype.name as payment_type_desc,
    CAST(trnln.datefrom_icare AS TIMESTAMP_NTZ) as payment_from_dt,
    CAST(trnln.dateto_icare AS TIMESTAMP_NTZ) as payment_to_dt,
    CAST(trnln.dateofservice_icare AS TIMESTAMP_NTZ) as service_provision_dt,
    CAST(trnln.updatetime AS TIMESTAMP_NTZ) as src_eff_dttm,
    cast(trnln.updatetime as date) as src_eff_dt,
    CAST(trnln.createtime AS TIMESTAMP_NTZ) as src_create_dttm,
    cast(trnln.createtime as date) as src_create_dt,

    dimcostcat.typecode as cost_category_cd,
    dimcostcat.name as cost_category_desc,

    pycd.paycode as paycode_cd,
    pycd.paymentcategory as paycode_payment_cat,
    pycd.paymenttype as paycode_payment_type,
    pycd.paymentsubtype as paycode_payment_sub_type,
    trnln.serviceproviderid_icare as service_provider_id,

    trnln.hourspaid_icare as count_hours_lost,
    sum(trnln.hourspaid_icare) over (partition by clm.claimnumber) as total_hours_lost,
    trnln.hourslost_icare as count_hours_paid,

    case
        when dim_txntype.typecode <> 'Payment' or dimcostcat.typecode <> '50' then null
        when trnln.transactionamount = 0 then null
        when dim_chqsts.typecode in ('issued', 'cleared', 'awaitingsubmission', 'requested')
        then dense_rank()
            over(partition by trn.claimid
                , dim_txntype.typecode
                , dimcostcat.typecode
                , (case when dim_chqsts.typecode in ('issued', 'cleared', 'awaitingsubmission', 'requested') and trnln.transactionamount <> 0 then 1 else 0 end)
            order by trnln.datefrom_icare)
        else null
    end as payment_count_of_weeks,

    trnln.itcaaamountpel as itc_aa_amt,
    trnln.reportingamount as reporting_amt,
    trnln.reservingamount as reserving_amt,
    trnln.weeklybenefitrate_icare as weekly_benefit_rate,
    trnln.amountwithoutgst as txn_amt_without_gst,
    trnln.gstcalcrate_icare as gst_percent_rate,
    trnln.gstamountpel as gst_amt,
    dim_gstmethod.typecode as txn_gst_method_cd,
    dim_gstmethod.name as txn_gst_method_desc,
    trnln.transactionamount as txn_amt,
    dim_gazette.rate as txn_gazetted_amt,
    trnln.deemedearningsperweek_icare as payee_deemed_earnings_per_week,
    trnln.grossweeklywagerate_icare as piawe,
    trnln.nonpecuniarybenefitd_icare as payee_non_pecuniary_benefit_d,
    trnln.earningse_icare as payee_earnings_e,
    CAST(trnset.approvaldate AS TIMESTAMP_NTZ)  as approval_dttm,
    dim_approvalstat.typecode as approval_type_cd,
    dim_approvalstat.name as approval_type_desc,
    trnset.requestinguserid as approved_src_user_id,

    case
        when trnset.adjustmentpayment_icare = 0 then 'N'
        when trnset.adjustmentpayment_icare = 1 then 'Y'
        else null
    end as txn_adjusted_ind,

    case
        when dim_txntype.name = 'Payment' and trn.exposureid is null then trnln.reportingamount * -1
        when dim_txntype.name = 'Reserve' and trn.exposureid is null then trnln.reportingamount
        else null
    end as claim_txn_lineitem_reserve_amt,

    case
        when dim_txntype.name = 'Payment' and trn.exposureid is null then trnln.reportingamount
        else null
    end as claim_txn_lineitem_paid_amt,

    trn.createuserid as create_src_user_id,
    case
        when trnln.gstapplicable_icare = 1 then 'Y'
        when trnln.gstapplicable_icare = 2 then 'N'
        when trnln.gstapplicable_icare = 3 then 'N/A'
    end as gst_applicable_ind,
    case
        when trnln.itcapplicable_icare = 1 then 'Y'
        when trnln.itcapplicable_icare = 2 then 'N'
        when trnln.itcapplicable_icare = 3 then 'N/A'
    end as itc_applicable_ind,

    coalesce(cast(exposure.claimorder as varchar), 'Claim-level') as exposure_cd,
    dim_exptype.l_en_au as exposure_desc,
    dim_covtype.l_en_au as coverage_desc,

    dim_rcvcatg.typecode as recovery_category_cd,
    dim_rcvcatg.name as recovery_category_desc,
    dim_txnsts.typecode as txn_status_cd,
    dim_txnsts.name as txn_status_desc,
    trn.payerdenormid as payer_id,
    dim_chqsts.typecode as payment_status_cd,
    dim_chqsts.description as payment_status_desc,
    trnln.draftamount_ext as draft_txn_amt,
    trnln.rehabservice_extid as rehab_service_ext_id,
    row_number() over (partition by clm.claimnumber order by trnln.createtime) as txn_line_rank,
    trnln.file_ingestion_timestamp
from cc_transactionlineitem trnln
inner join cc_transaction trn
    on trn.id = trnln.transactionid
inner join cc_claim clm
    on trn.claimid = clm.id
left join cc_check chq
    on chq.id = trn.checkid
left join ccx_paycode_icare pycd
    on pycd.id = trnln.paycode_icareid
left join cctl_paycodegstdecision_icare pycddec
    on pycddec.id = trnln.gstdecision
left join cctl_costcategory dimcostcat
    on trn.costcategory = dimcostcat.id
left join cc_transactionset trnset
    on trn.transactionsetid = trnset.id
left join cctl_paymenttype_icare dim_paytype
    on trnset.paymenttype_icare = dim_paytype.id

left join cctl_approvalstatus dim_approvalstat
    on trnset.approvalstatus = dim_approvalstat.id

left join cctl_gstmethodpel_ext dim_gstmethod
    on trnln.gstmethodpel_ext = dim_gstmethod.id

left join cctl_transaction dim_txntype
    on trn.subtype = dim_txntype.id

left join cc_exposure exposure
    on trn.exposureid = exposure.id

left join cctl_exposuretype dim_exptype
    on exposure.exposuretype = dim_exptype.id

left join cc_coverage coverage
    on exposure.coverageid = coverage.id

left join cctl_coveragetype dim_covtype
    on coverage.type = dim_covtype.id

left join ccx_paycodegazettedrate_icare dim_gazette
    on pycd.id = dim_gazette.paycode
    and trnln.dateofservice_icare between dim_gazette.effectivedate and coalesce(dim_gazette.expirationdate, current_timestamp())

left join cctl_recoverycategory dim_rcvcatg
    on dim_rcvcatg.id = trn.recoverycategory

left join cctl_transactionstatus_txn dim_txnsts
    on dim_txnsts.id = trn.status

left join cctl_transactionstatus_chq dim_chqsts
    on dim_chqsts.id = chq.status
)
select  
    claim_txn_lineitem_sk,
    src_txn_set_id,
    txn_lineitem_id,
    src_txn_lineitem_id,
    src_txn_id,
    src_claim_payment_id,
    src_claim_exposure_id,
    claim_sk,
    claim_nbr,
    src_claim_id,
    txn_type_cd,
    txn_type_desc,
    payment_type_cd,
    payment_type_desc,
    payment_from_dt,
    payment_to_dt,
    service_provision_dt,
    src_eff_dttm,
    src_eff_dt,
    src_create_dttm,
    src_create_dt,
    cost_category_cd,
    cost_category_desc,
    paycode_cd,
    paycode_payment_cat,
    paycode_payment_type,
    paycode_payment_sub_type,
    service_provider_id,
    count_hours_lost,
    total_hours_lost,
    count_hours_paid,
    payment_count_of_weeks,
    itc_aa_amt,
    reporting_amt,
    reserving_amt,
    weekly_benefit_rate,
    txn_amt_without_gst,
    gst_percent_rate,
    gst_amt,
    txn_gst_method_cd,
    txn_gst_method_desc,
    txn_amt,
    txn_gazetted_amt,
    payee_deemed_earnings_per_week,
    piawe,
    payee_non_pecuniary_benefit_d,
    payee_earnings_e,
    approval_dttm,
    approval_type_cd,
    approval_type_desc,
    approved_src_user_id,
    txn_adjusted_ind,
    claim_txn_lineitem_reserve_amt,
    claim_txn_lineitem_paid_amt,
    create_src_user_id,
    gst_applicable_ind,
    itc_applicable_ind,
    exposure_cd,
    exposure_desc,
    coverage_desc,
    recovery_category_cd,
    recovery_category_desc,
    txn_status_cd,
    txn_status_desc,
    payer_id,
    payment_status_cd,
    payment_status_desc,
    draft_txn_amt,
    rehab_service_ext_id,
    txn_line_rank,
    file_ingestion_timestamp
from
    cte_join