{% snapshot int_claim_payment_ni_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-04-02      0.0                             This Builds the Integrated Layer for claim payment for NI
-#}

{{
    config(
        target_schema='integrated_ni',
        unique_key='src_claim_payment_id',
        alias='int_claim_payment_ni',
        strategy='check',
        check_cols='all',
        tags=['claim', 'integrated', 'NI', 'snapshot_ni']
    )
}}

with cte_integrated_claim as (
    select
        claim_sk,
        src_claim_id
    from
        {{ ref('int_claim_ni_snapshot') }}
    where
        dbt_valid_to is null
),

-- Get all records from cc_check
cte_check as (
    select
        hash_key,
        publicid,
        bankaccount,
        accountname,
        bankaccountnumber,
        bankaccounttype,
        bankbranchname_icare,
        bankname,
        payto,
        emailaddress_icare,
        abn_icare,
        registeredforgst_icare,
        mailto,
        checkinstructions,
        checktype,
        issuedate,
        scheduledsenddate,
        datepaymenttransacted_icare,
        checknumber,
        memo,
        paymentmethod,
        dateofservice,
        servicepdstart,
        servicepdend,
        servicesprovidedoverseas_icare,
        gstmethodpel_ext,
        isinvoiceissuedto_icare,
        invoicenumber,
        prepostpayg_icare,
        recurrence_icare,
        status,
        voidedpaymentnumber_icare,
        westpacid_icare,
        retired,
        id,
        claimid,
        createtime,
        updatetime,
        source_system,
        check_sk
    from
        {{ ref('v_cc_check_current') }}
    where
        retired = 0
),

-- Get latest record from cctl_paymentmethod
cte_cctl_paymentmethod as (
    select
        id,
        typecode
    from
        {{ ref('v_cctl_paymentmethod_current') }}
),

-- Get latest record from cctl_checktype
cte_cctl_checktype as (
    select
        id,
        typecode
    from
        {{ ref('v_cctl_checktype_current') }}
),

-- Get latest record from cctl_prepostpayg_icare
cte_cctl_prepostpayg_icare as (
    select
        id,
        typecode
    from
        {{ ref('v_cctl_prepostpayg_icare_current') }}
),

-- Get latest record from cctl_recurrenceday
cte_cctl_recurrenceday as (
    select
        id,
        typecode
    from
        {{ ref('v_cctl_recurrenceday_current') }}
),

-- Get latest record from cctl_transactionstatus
cte_cctl_transactionstatus as (
    select
        id,
        typecode
    from
        {{ ref('v_cctl_transactionstatus_current') }}
),

-- Get latest record from cctl_gstmethodpel_ext
cte_cctl_gstmethodpel_ext as (
    select
        id,
        typecode
    from
        {{ ref('v_cctl_gstmethodpel_ext_current') }}
),

-- Get latest record from cctl_yesno
cte_cctl_yesno as (
    select
        id,
        typecode
    from
        {{ ref('v_cctl_yesno_current') }}
),

-- Get latest record from cctl_checkhandlinginstructions
cte_cctl_checkhandlinginstructions as (
    select
        id,
        typecode
    from
        {{ ref('v_cctl_checkhandlinginstructions_current') }}
),

-- Get latest record from cctl_bankaccounttype
cte_cctl_bankaccounttype as (
    select
        id,
        typecode
    from
        {{ ref('v_cctl_bankaccounttype_current') }}
),

cte_join as (
    select
        cc_check.check_sk                               as claim_payment_sk,
        source_system,
        ni_claim.claim_sk                               as claim_sk,
        cc_check.publicid                               as claim_payment_id,
        cc_check.bankaccount                            as payer_bank_account,
        cc_check.accountname                            as payee_bank_account_name,
        cc_check.bankaccountnumber                      as payee_bank_account_nbr,
        cc_check.bankaccounttype                        as payee_bank_account_type_ref_id,
        cctl_bankaccounttype.typecode                   as payee_bank_account_type_code,
        cc_check.bankbranchname_icare                   as payee_bank_branch_name,
        cc_check.bankname                               as payee_bank_name,
        cc_check.payto                                  as payee_name,
        cc_check.emailaddress_icare                     as payee_email_addr,
        cc_check.abn_icare                              as payee_abn,
        case
            when cc_check.registeredforgst_icare = false
                or cc_check.registeredforgst_icare is null then 'N'
            when cc_check.registeredforgst_icare = true then 'Y'
        end                                             as payee_registered_for_gst_ind,
        cc_check.mailto                                 as cheque_mailing_name,
        cc_check.checkinstructions                      as cheque_handling_instruction_ref_id,
        cctl_checkhandlinginstructions.typecode          as cheque_handling_instructions_code,
        cc_check.checktype                              as payment_ordinality_ref_id,
        cctl_checktype.typecode                         as payment_ordinality_code,
        cc_check.issuedate                              as payment_issue_date,
        cc_check.scheduledsenddate                      as payment_scheduled_send_date,
        cc_check.datepaymenttransacted_icare            as payment_txn_date,
        cc_check.checknumber                            as payment_nbr,
        cc_check.memo                                   as payment_memo,
        cc_check.paymentmethod                          as payment_method_ref_id,
        cctl_paymentmethod.typecode                     as payment_method_code,
        cc_check.dateofservice                          as service_provision_date,
        cc_check.servicepdstart                         as service_period_start_date,
        cc_check.servicepdend                           as service_period_end_date,
        case
            when cc_check.servicesprovidedoverseas_icare = false
                or cc_check.servicesprovidedoverseas_icare is null then 'N'
            when cc_check.servicesprovidedoverseas_icare = true then 'Y'
        end                                             as services_provided_overseas_ind,
        cc_check.gstmethodpel_ext                       as gst_method_ref_id,
        cctl_gstmethodpel_ext.typecode                  as gst_method_code,
        cc_check.isinvoiceissuedto_icare                as invoice_issued_to_icare_ref_id,
        cctl_yesno.typecode                             as invoice_issued_to_icare_code,
        cc_check.invoicenumber                          as invoice_nbr,
        cc_check.prepostpayg_icare                      as pre_post_payg_ref_id,
        cctl_prepostpayg_icare.typecode                 as pre_post_payg_code,
        cc_check.recurrence_icare                       as recurring_payment_day_ref_id,
        cctl_recurrenceday.typecode                     as recurring_payment_day_code,
        cc_check.status                                 as payment_status_ref_id,
        cctl_transactionstatus.typecode                 as payment_status_code,
        cc_check.voidedpaymentnumber_icare              as voided_payment_nbr,
        cc_check.westpacid_icare                        as westpac_id,
        case
            when cc_check.retired = 0
                or cc_check.retired is null then 'N'
            when cc_check.retired >= 1 then 'Y'
        end                                             as retired_ind,
        cc_check.id                                     as src_claim_payment_id,
        cc_check.createtime                             as src_create_ts,
        cc_check.updatetime                             as src_eff_ts
    from cte_check as cc_check
    inner join cte_integrated_claim as ni_claim
        on cc_check.claimid = ni_claim.src_claim_id
    inner join cte_cctl_paymentmethod as cctl_paymentmethod
        on cc_check.paymentmethod = cctl_paymentmethod.id
    inner join cte_cctl_gstmethodpel_ext as cctl_gstmethodpel_ext
        on cc_check.gstmethodpel_ext = cctl_gstmethodpel_ext.id
    inner join cte_cctl_checktype as cctl_checktype
        on cc_check.checktype = cctl_checktype.id
    inner join cte_cctl_transactionstatus as cctl_transactionstatus
        on cc_check.status = cctl_transactionstatus.id
    left join cte_cctl_recurrenceday as cctl_recurrenceday
        on cc_check.recurrence_icare = cctl_recurrenceday.id
    left join cte_cctl_prepostpayg_icare as cctl_prepostpayg_icare
        on cc_check.prepostpayg_icare = cctl_prepostpayg_icare.id
    left join cte_cctl_yesno as cctl_yesno
        on cc_check.isinvoiceissuedto_icare = cctl_yesno.id
    left join cte_cctl_checkhandlinginstructions as cctl_checkhandlinginstructions
        on cc_check.checkinstructions = cctl_checkhandlinginstructions.id
    left join cte_cctl_bankaccounttype as cctl_bankaccounttype
        on cc_check.bankaccounttype = cctl_bankaccounttype.id
)

-- Deduplicate to latest record per src_claim_payment_id
select *
from cte_join
qualify row_number() over (
    partition by src_claim_payment_id
    order by src_eff_ts desc
) = 1

{% endsnapshot %}