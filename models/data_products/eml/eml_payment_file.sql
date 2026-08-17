{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             EML Payment File extract - final assembly model.
                                                Replaces monolithic E02_EML_PAYMENT_FILE.sas by
                                                referencing modular int_* dbt models.

-#}   

{{
  config(
    materialized='table',
    tags=['extract', 'eml', 'legacy', 'business_critical']
  )
}}

{#
  Source: E02_EML_PAYMENT_FILE.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_E02
  TBL_NM: MSC_QLK_ASPIRE_EML_PAYMENT_FILE
-#}

{# ============================================================
   BASE CTEs - Each ref model with predicates applied early
   ============================================================ #}

with base_claim as (
    select
        claim_sk,
        claim_nbr,
        policy_nbr,
        managing_entity_cd,
        claim_segment_desc,
        case_owner_user_sk,
        case_owner_team_sk
    from {{ ref('int_claim') }}
),

base_txn_lineitem as (
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
        txn_line_rank
    from {{ ref('int_claim_txn_lineitem') }}
),

base_payment as (
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
        camt_void_dt,
        westpac_id
    from {{ ref('int_claim_payment') }}
),

base_txn_document as (
    select
        claim_nbr,
        src_txn_set_id,
        src_doc_id,
        doc_type_desc,
        doc_section_desc,
        doc_sub_section_desc,
        doc_status_desc,
        doc_sent_received_dttm,
        doc_channel_desc,
        doc_name,
        doc_workcapacity_link_id,
        doc_lob_id,
        doc_inbound_ind,
        doc_schemeagent_id,
        doc_create_dttm,
        doc_update_dttm,
        earliest_invoice_doc_ind
    from {{ ref('int_claim_txn_document') }}
),

base_triage as (
    select
        claim_nbr,
        first_proposed_triage_ind
    from {{ ref('int_claim_triage') }}
    where latest_triage_ind = 'Y'
),

base_user as (
    select
        user_sk,
        src_user_id,
        user_first_name,
        user_last_name
    from {{ ref('int_user') }}
),

base_team as (
    select
        level_leaf_team_sk,
        level_leaf_team_name
    from {{ ref('int_team') }}
),

base_payee as (
    select
        claim_nbr,
        claim_payment_sk,
        src_payee_contact_id,
        payee_type_cd,
        payee_type_desc,
        payee_name
    from {{ ref('int_claim_payment_payee') }}
),

{# ============================================================
   INTERMEDIATE CTEs - Derived logic before final assembly
   ============================================================ #}

txn_document_ranked as (
    select
        *,
        row_number() over (
            partition by claim_nbr, src_txn_set_id
            order by doc_sent_received_dttm desc
        ) as doc_rn
    from base_txn_document
),

txn_document_invoice as (
    select *
    from txn_document_ranked
    where doc_type_desc = 'Invoice'
      and earliest_invoice_doc_ind = 'Y'
      and doc_rn = 1
),

cte_documents as (
    select
        src_txn_set_id,
        count(src_doc_id) as count_document,
        min(doc_sent_received_dttm) as payment_earliest_document_received_date
    from base_txn_document
    group by src_txn_set_id
),

{# ============================================================
   CLAIM_PAYMENT_TXN_LINEITEM_DOC equivalent
   Combines txn_lineitem + payment + invoice document
   with early filter: Recovery OR (Payment with matching payment)
   ============================================================ #}

payment_txn_lineitem_doc as (
    select
        pmt.claim_payment_sk,
        pmt.claim_payment_id,
        pmt.src_claim_payment_id,
        case when tli.txn_type_cd = 'Payment' then pmt.payment_nbr else null end as payment_nbr,
        case when tli.txn_type_cd = 'Payment' then pmt.invoice_nbr else null end as invoice_nbr,
        tli.src_txn_set_id,
        tli.claim_sk,
        tli.claim_nbr,
        tli.src_claim_id,
        pmt.payee,
        case when tli.txn_type_cd = 'Payment' then pmt.payment_issue_dttm else null end as payment_issue_dttm,
        case when tli.txn_type_cd = 'Payment' then pmt.payment_issue_dt else null end as payment_issue_dt,
        case when tli.txn_type_cd = 'Payment' then pmt.last_posting_dt else null end as last_posting_dt,
        pmt.payment_presented_dt,
        pmt.payment_scheduled_send_dt,
        pmt.payment_transacted_dt,
        pmt.service_provision_dt as payment_service_provision_dt,
        cast(tli.payment_from_dt as date) as service_period_start_dt,
        cast(tli.payment_to_dt as date) as service_period_end_dt,
        pmt.payment_dt,
        pmt.src_create_dttm as payment_src_create_dttm,
        pmt.src_create_dt as payment_src_create_dt,
        case
            when tli.txn_type_cd = 'Payment' then pmt.create_src_user_id
            else tli.create_src_user_id
        end as create_src_user_id,
        pmt.payee_registered_for_gst_ind,
        pmt.payment_ordinality_cd,
        pmt.payment_ordinality_desc,
        pmt.payment_method_cd,
        pmt.payment_method_desc,
        pmt.payment_gst_method_cd,
        pmt.payment_gst_method_desc,
        case
            when tli.txn_type_cd = 'Payment' then pmt.payment_status_cd
            else tli.txn_status_cd
        end as payment_status_cd,
        case
            when tli.txn_type_cd = 'Payment' then pmt.payment_status_desc
            else tli.txn_status_desc
        end as payment_status_desc,
        case
            when tli.txn_type_cd = 'Payment' and pmt.payment_status_desc = 'Voided' then pmt.camt_void_dt
            else null
        end as camt_void_date,
        pmt.weekly_benefit_payee_type_cd,
        pmt.weekly_benefit_payee_type_desc,
        pmt.payment_portion_amt,
        pmt.payg_amt,
        pmt.payment_amt,
        case
            when tli.txn_type_cd = 'Payment' then pmt.src_eff_dt
            else tli.src_eff_dt
        end as src_eff_dt,
        case
            when tli.txn_type_cd = 'Payment' then pmt.src_eff_dttm
            else tli.src_eff_dttm
        end as src_eff_dttm,
        tli.txn_type_cd,
        tli.txn_type_desc,
        tli.claim_txn_lineitem_sk,
        tli.txn_lineitem_id,
        tli.src_txn_lineitem_id,
        tli.src_txn_id,
        case when tli.txn_type_cd = 'Payment' then tli.payment_type_cd else null end as payment_type_cd,
        case when tli.txn_type_cd = 'Payment' then tli.payment_type_desc else null end as payment_type_desc,
        tli.payment_from_dt,
        tli.payment_to_dt,
        tli.service_provision_dt,
        tli.src_create_dttm as txn_src_create_dttm,
        tli.src_create_dt as txn_src_create_dt,
        tli.approval_dttm,
        tli.approval_type_cd,
        tli.approval_type_desc,
        tli.approved_src_user_id,
        tli.cost_category_cd,
        tli.cost_category_desc,
        tli.paycode_cd,
        tli.paycode_payment_cat,
        tli.paycode_payment_type,
        tli.paycode_payment_sub_type,
        tli.service_provider_id,
        tli.count_hours_lost,
        tli.count_hours_paid,
        tli.payment_count_of_weeks,
        tli.itc_aa_amt,
        tli.reporting_amt,
        tli.reserving_amt,
        tli.weekly_benefit_rate,
        tli.txn_amt_without_gst,
        tli.gst_percent_rate,
        tli.gst_amt,
        tli.txn_gst_method_desc,
        tli.txn_amt,
        tli.txn_gazetted_amt,
        tli.payee_deemed_earnings_per_week,
        tli.piawe,
        tli.payee_non_pecuniary_benefit_d,
        tli.payee_earnings_e,
        tli.txn_adjusted_ind,
        tli.gst_applicable_ind,
        tli.itc_applicable_ind,
        tli.exposure_cd,
        tli.exposure_desc,
        tli.coverage_desc,
        tli.recovery_category_cd,
        tli.recovery_category_desc,
        tli.txn_status_cd,
        tli.txn_status_desc,
        cast(tli.payer_id as varchar) as payer_id,
        case
            when tli.txn_type_cd = 'Recovery' then 'N'
            when tli.cost_category_desc = 'Weekly' and pmt.payment_status_cd not in ('issued', 'cleared') then 'N'
            when row_number() over (
                partition by case
                    when tli.cost_category_desc = 'Weekly'
                     and pmt.payment_status_cd in ('issued', 'cleared')
                     and tli.txn_type_cd = 'Payment'
                    then c.claim_nbr
                end
                order by tli.payment_from_dt desc, pmt.payment_issue_dt desc
            ) = 1 then 'Y'
            else 'N'
        end as latest_paid_weekly_benefit_rank,
        case
            when tli.txn_type_cd = 'Recovery' then 'N'
            when tli.cost_category_desc <> 'Medical' then 'N'
            when pmt.payment_status_cd not in ('issued', 'cleared') then 'N'
            when rank() over (
                partition by c.claim_nbr
                order by tli.txn_type_cd, pmt.payment_dt desc, pmt.payment_issue_dttm desc, pmt.src_create_dttm desc
            ) = 1 then 'Y'
            else 'N'
        end as latest_paid_medical_rank,
        row_number() over (
            partition by c.claim_nbr, pmt.claim_txn_set_id
            order by tli.txn_amt desc, tli.service_provision_dt desc
        ) as txn_lineitem_value_rank,
        doc.src_doc_id,
        doc.doc_type_desc,
        doc.doc_section_desc,
        doc.doc_sub_section_desc,
        doc.doc_status_desc,
        case when tli.txn_type_cd = 'Payment' then doc.doc_sent_received_dttm else null end as doc_sent_received_dttm,
        doc.doc_channel_desc,
        doc.doc_name,
        doc.doc_workcapacity_link_id,
        doc.doc_lob_id,
        doc.doc_inbound_ind,
        doc.doc_schemeagent_id,
        doc.doc_create_dttm,
        doc.doc_update_dttm,
        case
            when tli.txn_type_cd = 'Payment' then pmt.westpac_id
            else null
        end as westpac_id,
        pmt.src_eff_dt as src_eff_dt_pymnt,
        pmt.src_eff_dttm as src_eff_dttm_pymnt,
        tli.src_eff_dt as src_eff_dt_txnlineitem,
        tli.src_eff_dttm as src_eff_dttm_txnlineitem
    from base_claim c
    inner join base_txn_lineitem tli
        on c.claim_nbr = tli.claim_nbr
    left join base_payment pmt
        on c.claim_nbr = pmt.claim_nbr
        and pmt.src_claim_payment_id = tli.src_claim_payment_id
    left join txn_document_invoice doc
        on c.claim_nbr = doc.claim_nbr
        and tli.src_txn_set_id = doc.src_txn_set_id
    where tli.txn_type_cd = 'Recovery'
       or (tli.txn_type_cd = 'Payment' and pmt.src_claim_payment_id is not null)
),

{# ============================================================
   FINAL ASSEMBLY - Mirrors E02 final SELECT
   ============================================================ #}

final as (
    select distinct
        current_timestamp() as extract_dttm,
        c.claim_nbr,
        c.policy_nbr,
        ptli.invoice_nbr,
        ptli.payment_nbr,
        ptli.src_txn_lineitem_id as internal_payment_line_item_id,
        ptli.src_txn_set_id,
        c.claim_segment_desc as segment,
        t.level_leaf_team_name as team,
        concat(u_owner.user_first_name, ' ', u_owner.user_last_name) as claims_officer,
        concat(u_enter.user_first_name, ' ', u_enter.user_last_name) as txn_entered_by,
        ptli.approval_type_desc as txn_approval_outcome,
        concat(u_approve.user_first_name, ' ', u_approve.user_last_name) as txn_approved_by,
        ptli.payment_type_desc,
        ptli.paycode_cd as paycode,
        ptli.paycode_payment_cat as paycode_category,
        ptli.paycode_payment_type as paycode_type,
        ptli.paycode_payment_sub_type as paycode_subtype,
        ptli.payment_status_desc,
        ptli.gst_applicable_ind,
        ptli.itc_applicable_ind,
        ptli.payment_amt as txn_amount,
        ptli.txn_amt as txn_lineitem_gross_amount,
        ptli.gst_amt as txn_lineitem_tax_amount,
        ptli.txn_amt - ptli.gst_amt as txn_lineitem_net_amount,
        ptli.itc_aa_amt as txn_lineitem_itc_amount,
        ptli.txn_gst_method_desc as txn_lineitem_gst_method,
        ptli.txn_adjusted_ind as txn_adjusted_flag,
        ptli.service_provider_id,
        case
            when ptli.txn_type_cd = 'Payment' then payee.src_payee_contact_id
            else ptli.payer_id
        end as payee_id,
        case
            when ptli.txn_type_cd = 'Payment' then payee.payee_name
            else null
        end as payee_name,
        case
            when ptli.txn_type_cd = 'Payment' then payee.payee_type_desc
            else null
        end as payee_type,
        case
            when ptli.txn_type_cd = 'Payment' then cast(ptli.payment_scheduled_send_dt as date)
            else null
        end as payment_scheduled_sent_dt,
        ptli.payment_issue_dt,
        to_char(ptli.payment_issue_dttm, 'HH24:MI:SS') as payment_issue_time,
        ptli.src_eff_dt as payment_update_dt,
        to_char(ptli.src_eff_dttm, 'HH24:MI:SS') as payment_update_time,
        cast(ptli.approval_dttm as date) as approval_dt,
        to_char(ptli.approval_dttm, 'HH24:MI:SS') as approval_time,
        ptli.last_posting_dt,
        ptli.txn_src_create_dt as txn_lineitem_create_dt,
        to_char(ptli.txn_src_create_dttm, 'HH24:MI:SS') as txn_lineitem_create_time,
        ptli.service_period_start_dt,
        ptli.service_period_end_dt,
        ptli.service_provision_dt,
        cast(ptli.doc_sent_received_dttm as date) as invoice_dt,
        to_char(ptli.doc_sent_received_dttm, 'HH24:MI:SS') as invoice_time,
        c.managing_entity_cd,
        to_char(current_date(), 'YYYYMMDD') as extract_date,
        ptli.txn_type_desc,
        ptli.cost_category_desc,
        ptli.recovery_category_desc,
        case
            when ptli.payment_method_cd = 'check' then 'CHEQUE'
            else upper(ptli.payment_method_cd)
        end as payment_method_cd,
        ptli.westpac_id,
        {# Note: PAYEE_ABN requires raw ABN_icare - not available in int_claim_payment_payee (hashed).
           Enhance int_claim_payment_payee if raw ABN is required for this extract. #}
        ptli.txn_gazetted_amt,
        cte_doc.count_document as doc_count_linked_to_payment,
        to_char(cte_doc.payment_earliest_document_received_date, 'DD/MM/YYYY HH24:MI') as payment_earliest_document_received_date,
        ptli.camt_void_date,
        ptli.weekly_benefit_rate,
        ptli.payee_earnings_e,
        ptli.piawe,
        ptli.count_hours_lost as hours_worked,
        {# Note: BSB and ACCOUNT_NUMBER require raw bank details - not available in
           int_claim_payment_payee (only bank_details_present_ind). Enhance if needed. #}
        ptli.src_eff_dt_pymnt,
        ptli.src_eff_dttm_pymnt,
        ptli.src_eff_dt_txnlineitem,
        ptli.src_eff_dttm_txnlineitem

    from base_claim c

    inner join payment_txn_lineitem_doc ptli
        on c.claim_nbr = ptli.claim_nbr

    left join base_triage tri
        on c.claim_nbr = tri.claim_nbr

    left join base_user u_owner
        on c.case_owner_user_sk = u_owner.user_sk

    left join base_team t
        on c.case_owner_team_sk = t.level_leaf_team_sk

    left join base_user u_enter
        on ptli.create_src_user_id = u_enter.src_user_id

    left join base_user u_approve
        on ptli.approved_src_user_id = u_approve.src_user_id

    left join base_payee payee
        on payee.claim_nbr = ptli.claim_nbr
        and payee.claim_payment_sk = ptli.claim_payment_sk

    left join cte_documents cte_doc
        on ptli.src_txn_set_id = cte_doc.src_txn_set_id
)

select * from final
order by claim_nbr, payment_nbr
