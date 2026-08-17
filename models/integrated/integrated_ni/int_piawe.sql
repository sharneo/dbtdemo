{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for overpayment reimbursement.

-#}   

{{
  config(
    materialized='incremental',
    unique_key=['src_claim_id', 'piawe_effective_dt', 'latest_piawe_rank'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 21_PIAWE.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A21
  TBL_NM: MSC_QLK_ASPIRE_PIAWE
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    {% if is_incremental() %}
    WHERE file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_exposure as (
    select
        id,
        claimid
    from {{ ref('v_cc_exposure_current') }}
    where retired = 0
),

ccx_piawe_icare as (
    select
        id,
        exposureid,
        effectivedate_icare,
        piawefirst52_icare,
        piawelater52_icare,
        piawetype_icare,
        createtime,
        retired,
        draft,
        piawedeactivated_icare
    from {{ ref('v_ccx_piawe_icare_current') }}
    where retired = 0
        and draft = 0
        and piawedeactivated_icare = 0
),

cctl_piawetype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_piawetype_icare_current') }}
),

ccx_benefitsaccrual_icare as (
    select
        exposureid,
        totalweekspaid
    from {{ ref('v_ccx_benefitsaccrual_icare_current') }}
    where retired = 0
),

piawe_deduped as (
    select
        piawe.id,
        piawe.exposureid,
        piawe.effectivedate_icare,
        piawe.piawefirst52_icare,
        piawe.piawelater52_icare,
        piawe.createtime,
        dimpiawe.typecode as piawe_type,
        dimpiawe.name as piawe_type_desc,
        row_number() over (
            partition by piawe.exposureid, piawe.effectivedate_icare
            order by piawe.createtime desc
        ) as row_num
    from ccx_piawe_icare piawe
    inner join cctl_piawetype_icare dimpiawe
        on piawe.piawetype_icare = dimpiawe.id
),

piawe_with_expiry as (
    select
        p.*,
        lead(p.effectivedate_icare) over (
            partition by p.exposureid
            order by p.createtime asc
        ) as expirydate
    from piawe_deduped p
    where p.row_num = 1
),

cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as source_system,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    CAST(piawe.effectivedate_icare AS TIMESTAMP_NTZ) AS  piawe_effective_dt,
    CAST(coalesce(piawe.expirydate, current_timestamp()) as TIMESTAMP_NTZ) as piawe_expiry_dt,
    piawe.piawe_type,
    piawe.piawe_type_desc,
    CAST(piawe.createtime as TIMESTAMP_NTZ) as src_create_dttm,
    case
        when piawe.piawe_type = 'manual_icare' and coalesce(bacc.totalweekspaid, 0) > 52
        then piawe.piawelater52_icare
        else piawe.piawefirst52_icare
    end as piawe_amount,
    row_number() over (
        partition by clm.claimnumber
        order by piawe.effectivedate_icare desc, piawe.createtime desc
    ) as latest_piawe_rank,
    current_date() as extract_date,
    clm.file_ingestion_timestamp
from cc_claim clm
inner join cc_exposure exp
    on clm.id = exp.claimid
inner join piawe_with_expiry piawe
    on exp.id = piawe.exposureid
left join ccx_benefitsaccrual_icare bacc
    on bacc.exposureid = exp.id
)
select 
    claim_sk,
    source_system,
    claim_nbr,
    src_claim_id,
    piawe_effective_dt,
    piawe_expiry_dt,
    piawe_type,
    piawe_type_desc,
    src_create_dttm,
    piawe_amount,
    latest_piawe_rank,
    file_ingestion_timestamp
from
    cte_join
