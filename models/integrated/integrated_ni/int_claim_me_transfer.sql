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
    unique_key=['src_claim_id', 'transfer_dttm'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 37_CLAIM_ME_TRANSFER.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A37
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_ME_TRANSFER
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

cc_history as (
    select
        id,
        claimid,
        customtype,
        eventtimestamp,
        description
    from {{ ref('v_cc_history_current') }}
),

cctl_customhistorytype as (
    select
        id,
        typecode
    from {{ ref('v_cctl_customhistorytype_current') }}
    where retired = 0
),

ccx_managingentity_icare as (
    select
        id,
        name,
        code
    from {{ ref('v_ccx_managingentity_icare_current') }}
    where retired = 0
),

ccx_claimtransferhistory_ext as (
    select
        claimid,
        eventtimestamp,
        transferstatus,
        transferringcsp,
        receivingcsp,
        transfertype,
        retired
    from {{ ref('v_ccx_claimtransferhistory_ext_current') }}
    where retired = 0
),

cte_bct as (
    select
        hst.claimid,
        hst.eventtimestamp
    from cc_history hst
    inner join cctl_customhistorytype ctyp
        on ctyp.id = hst.customtype
        and ctyp.typecode = 'transferred_icare'
    where cast(hst.eventtimestamp as date) = '2023-06-04'
        and hst.description = 'Claims Service Provider is changed from NI_EML to NI_ICT'

    union

    select
        bct.claimid,
        bct.eventtimestamp
    from ccx_claimtransferhistory_ext bct
    where cast(bct.eventtimestamp as date) = '2023-06-04'
        and bct.transferstatus = 'Transferred'
        and bct.transferringcsp = 'NI_EML'
        and bct.receivingcsp = 'NI_ICT'
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as src_system_cd,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    case
        when mgefrom.code is null then
            case
                when bct.claimid is not null then replace(substring(hst.description, 33, charindex(' to ', hst.description) - 33), 'NI_DP_EML', 'NI_ICT')
                else substring(hst.description, 33, charindex(' to ', hst.description) - 33)
            end
        else
            case
                when bct.claimid is not null and hst.eventtimestamp < bct.eventtimestamp then replace(mgefrom.code, 'NI_DP_EML', 'NI_ICT')
                else mgefrom.code
            end
    end as from_managing_entity,
    case
        when mgeto.code is null then
            case
                when bct.claimid is not null then replace(right(hst.description, len(hst.description) - charindex(' to ', hst.description) - 3), 'NI_DP_EML', 'NI_ICT')
                else right(hst.description, len(hst.description) - charindex(' to ', hst.description) - 3)
            end
        else
            case
                when bct.claimid is not null and hst.eventtimestamp < bct.eventtimestamp then replace(mgeto.code, 'NI_DP_EML', 'NI_ICT')
                else mgeto.code
            end
    end as to_managing_entity,
    CAST(hst.eventtimestamp AS TIMESTAMP_NTZ) as  transfer_dttm,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_history hst

inner join cctl_customhistorytype ctyp
    on ctyp.id = hst.customtype
    and ctyp.typecode = 'transferred_icare'

inner join cc_claim clm
    on clm.id = hst.claimid

left join cte_bct bct
    on bct.claimid = hst.claimid

left join ccx_managingentity_icare mgefrom
    on mgefrom.name =
        case
            when left(hst.description, 7) = 'Claims '
                then substring(hst.description, 41, charindex(' to ', hst.description) - 41)
            when cast(hst.eventtimestamp as date) >= '2023-03-18'
                then substring(hst.description, 33, charindex(' to ', hst.description) - 33)
            else null
        end

left join ccx_managingentity_icare mgeto
    on mgeto.name =
        case
            when cast(hst.eventtimestamp as date) >= '2023-03-18'
                then right(hst.description, len(hst.description) - charindex(' to ', hst.description) - 3)
            else null
        end

where bct.claimid is null
    or bct.eventtimestamp != hst.eventtimestamp

union all

select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as src_system_cd,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    frmcsp.code as from_managing_entity,
    tocsp.code as to_managing_entity,
    CAST(hst.eventtimestamp AS TIMESTAMP_NTZ) as transfer_dttm,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from ccx_claimtransferhistory_ext hst

inner join cc_claim clm
    on clm.id = hst.claimid

left join cte_bct bct
    on bct.claimid = hst.claimid
    and bct.eventtimestamp = hst.eventtimestamp

left join ccx_managingentity_icare frmcsp
    on frmcsp.name = hst.transferringcsp

left join ccx_managingentity_icare tocsp
    on tocsp.name = hst.receivingcsp

where hst.transfertype = 'Claims Transfer'
    and hst.transferstatus = 'Transferred'
    and bct.claimid is null
)
select  
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    CAST(from_managing_entity AS VARCHAR(150)) AS from_managing_entity,
    CAST(to_managing_entity AS VARCHAR(150)) AS to_managing_entity,
    transfer_dttm,
    file_ingestion_timestamp
from
    cte_join 