{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates an Overpayment Recovery View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with cc_transaction as (
    select
        id,
        claimid,
        subtype,
        recoverycategory,
        recoverytype_icare
    from {{ ref('v_cc_transaction_current') }}
    where retired = 0
),

cctl_transaction as (
    select
        id
    from {{ ref('v_cctl_transaction_current') }}
    where retired = 0 and typecode = 'RECOVERY'
),

cctl_recoverycategory as (
    select
        id,
        typecode
    from {{ ref('v_cctl_recoverycategory_current') }}
    where retired = 0 and typecode = 'OVERPAYMENT_ICARE'
),

cc_transactionlineitem as (
    select
        id,
        transactionid,
        transactionamount
    from {{ ref('v_cc_transactionlineitem_current') }}
    where retired = 0 and transactionamount > 0
),

base as (
    select
        t.id as cc_transaction_id,
        tli.id as cc_transactionlineitem_id,
        t.claimid,
        cast(tli.transactionamount as decimal(30, 6)) as transactionamount,
        t.recoverytype_icare,
        rcat.typecode
    from cc_transaction as t
    inner join cctl_transaction as ttl on ttl.id = t.subtype
    inner join cctl_recoverycategory as rcat on rcat.id = t.recoverycategory
    left join cc_transactionlineitem as tli on tli.transactionid = t.id
)

select
    cc_transaction_id,
    cc_transactionlineitem_id,
    claimid,
    transactionamount,
    recoverytype_icare,
    typecode
from base
