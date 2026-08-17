{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental Model for claim deductible.
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_deductible_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 42_CLAIM_DEDUCTIBLE.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A42
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_DEDUCTIBLE
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
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_deductible as (
    select
        id,
        publicid,
        claimid,
        amount,
        waived,
        paid,
        overridden,
        editreason,
        invoicenumber_icare,
        accountnumber_icare,
        invoiceamountsenttobc,
        createtime
    from {{ ref('v_cc_deductible_current') }}
    where retired = 0
),
cte_join AS 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'ded.publicid'
    ]) }} as varchar(150)) as claim_deductible_sk,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    ded.id as src_deductible_id,
    ded.amount as excess_amount,
    CASE
        WHEN ded.waived = 1 THEN 'Y'
        ELSE 'N'
    END                       AS excess_waived_ind,
    CASE
        WHEN ded.paid = 1 THEN 'Y'
        ELSE 'N'
    END                       AS excess_paid_ind,
    CASE
        WHEN ded.overridden = 1 THEN 'Y'
        ELSE 'N'
    END                       AS excess_modified_ind,
    ded.editreason as excess_edit_reason,
    ded.invoicenumber_icare as excess_invoice_nbr,
    ded.accountnumber_icare as account_nbr,
    ded.invoiceamountsenttobc as invoice_amt_sent_to_bc,
    CAST(ded.createtime as TIMESTAMP_NTZ) as src_create_dttm,
    current_date() as extract_date,
    clm.file_ingestion_timestamp
from cc_deductible ded
inner join cc_claim clm
    on clm.id = ded.claimid
)
select  
        claim_sk,
        claim_deductible_sk,
        claim_nbr,
        src_claim_id,
        src_deductible_id,
        excess_amount,
        excess_waived_ind,
        excess_paid_ind,
        excess_modified_ind,
        excess_edit_reason,
        excess_invoice_nbr,
        account_nbr,
        invoice_amt_sent_to_bc,
        src_create_dttm,
        file_ingestion_timestamp
FROM    
    cte_join