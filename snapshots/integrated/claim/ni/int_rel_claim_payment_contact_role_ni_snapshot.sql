{%snapshot int_rel_claim_payment_contact_role_ni_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-03-30      0.0                             This Builds the Integrated Layer for int_rel_claim_contact_role for NI
2026-03-30      0.0                             AF Changes

-#}

{{
    config(
        target_schema='integrated_ni',
        unique_key='claim_payment_contact_role_sk',
        alias='int_rel_claim_payment_contact_role_ni',
        strategy='check',
        check_cols='all',
        tags=['claim', 'integrated', 'NI', 'snapshot_ni']
    )
}}

-- Get latest cte_checkpayee records
with 
cte_checkpayee as (
    select
        id,
        payeedenormid,
        checkid,
        payeetype,
        hash_key,
        source_system
    from
        {{ ref('v_cc_checkpayee_current') }}
),
-- get latest cte_contact records
cte_contact as (
    select
        id,
        subtype
    from
        {{ ref('v_cc_contact_current') }}
    where
        retired = 0
),
--get latest record from int_claim_ni
cte_int_payment_ni as (
    select
        claim_payment_sk,
        src_claim_payment_id
    from
        {{ ref('int_claim_payment_ni_snapshot') }}
    where
        dbt_valid_to is null
),
--get latest record from int_claim_contact_ni
cte_int_claim_contact_role_ni as (
    select
        src_claim_id,
        src_contact_id,
        src_claim_contact_role_id,
        contact_role_code,
        contact_role_ref_id
    from
        dev_curated_db.integrated_db.int_rel_claim_policy_contact_role
    where
        dbt_valid_to is null
),
cte_join as (
    select
        {{ dbt_utils.generate_surrogate_key(['contactrole.src_contact_id','contactrole.src_claim_contact_role_id','payment.src_claim_payment_id') }} as claim_payment_contact_role_sk,
        --hash_key as claim_payment_contact_role_sk,
        source_system,
        claim_payment_sk,
        contact_sk,
        contact_role_code,
        contact_role_ref_id
    from
        cte_checkpayee  as  cc_checkpayee 
        inner join cte_int_claim_contact_role_ni as contactrole  on contactrole.src_contact_id = cc_checkpayee.payeedenormid
        inner join cte_int_payment_ni as payment on cc_checkpayee.checkid = payment.src_claim_payment_id
       
) 
select
    *
from
    cte_join
    
{% endsnapshot %}		