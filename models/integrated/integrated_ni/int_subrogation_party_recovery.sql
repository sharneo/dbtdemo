{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental model for subrogation party recovery.
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_recovery_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 45_SUBROGATION_PARTY_RECOVERY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A45
  TBL_NM: MSC_QLK_ASPIRE_SUBROGATION_PARTY_RECOVERY
-#}

with ccx_subadvpartyrecovery_icare as (
    select
        id,
        subroadverseparty,
        recoverytype_icare,
        invoicenumber,
        invoicetype,
        strategy,
        recoveryperiodstart,
        recoveryperiodend,
        invoicecreated,
        writeoff,
        writtenoffamount,
        waived,
        invoiceamount,
        amountreceived,
        invoicedate,
        duedate,
        createtime,
        updatetime,
        file_ingestion_timestamp
    from {{ ref('v_ccx_subadvpartyrecovery_icare_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cctl_recoverytype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_recoverytype_icare_current') }}
    where retired = 0
),

cctl_subrostrategy as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_subrostrategy_current') }}
    where retired = 0
),

cctl_recoveryinvoicetype_icare as (
    select
        id,
        typecode,
        l_en_au
    from {{ ref('v_cctl_recoveryinvoicetype_icare_current') }}
    where retired = 0
),

cte_join as 
(
select
    rcv.id as src_recovery_id,
    rcv.subroadverseparty as src_adverse_party_id,
    rcvtyp.typecode as recovery_type_cd,
    rcvtyp.name as recovery_type_desc,
    rcv.invoicenumber as invoice_nbr,
    invtyp.typecode as invoice_type_cd,
    invtyp.l_en_au as invoice_type_desc,
    strat.typecode as strategy_cd,
    strat.name as strategy_desc,
    CAST(rcv.recoveryperiodstart AS TIMESTAMP_NTZ) as recovery_period_start_dttm,
    CAST(rcv.recoveryperiodend AS TIMESTAMP_NTZ) as recovery_period_end_dttm,
    case
        when rcv.invoicecreated is null then null
        when rcv.invoicecreated = 1 then 'Y'
        else 'N'
    end as invoice_created_ind,
    case
        when rcv.writeoff is null then null
        when rcv.writeoff = 1 then 'Y'
        else 'N'
    end as write_off_ind,
    rcv.writtenoffamount as write_off_amt,
    case
        when rcv.waived is null then null
        when rcv.waived = 1 then 'Y'
        else 'N'
    end as waived_ind,
    rcv.invoiceamount as invoice_amt,
    rcv.amountreceived as received_amt,
    cast(rcv.invoicedate as date) as invoice_dt,
    cast(rcv.duedate as date) as due_dt,
    CAST(rcv.createtime as TIMESTAMP_NTZ) as src_create_dttm,
    CAST(rcv.updatetime as TIMESTAMP_NTZ) AS  src_update_dttm,
    current_date() as extract_date,
    rcv.file_ingestion_timestamp

from ccx_subadvpartyrecovery_icare rcv

left join cctl_recoverytype_icare rcvtyp
    on rcvtyp.id = rcv.recoverytype_icare

left join cctl_subrostrategy strat
    on strat.id = rcv.strategy

left join cctl_recoveryinvoicetype_icare invtyp
    on invtyp.id = rcv.invoicetype
)
select 
    src_recovery_id,
    src_adverse_party_id,
    recovery_type_cd,
    recovery_type_desc,
    invoice_nbr,
    invoice_type_cd,
    invoice_type_desc,
    strategy_cd,
    strategy_desc,
    recovery_period_start_dttm,
    recovery_period_end_dttm,
    invoice_created_ind,
    write_off_ind,
    write_off_amt,
    waived_ind,
    invoice_amt,
    received_amt,
    invoice_dt,
    due_dt,
    src_create_dttm,
    src_update_dttm,
    file_ingestion_timestamp
from
    cte_join