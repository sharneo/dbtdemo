{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_activitydocument.
                                                Source: ref('stg_raw_cc_activitydocument')
                                                unique_key: activitydocument_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    tags=['aspire']
) }}

with

cc_claim as (
    select
          id
        , claimnumber
        , locationcodeid
        , claimworkcompid
        , retired
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cc_incident as (
    select
          claimid
        , claimincident
        , retired
        , mechanismofinjurydesc_icare
        , subtype
    from {{ ref('v_cc_incident_current') }}
    where claimincident = 1
      and retired = 0
),

cctl_incident as (
    select
          id
        , typecode
    from {{ ref('v_cctl_incident_current') }}
    where lower(typecode) = 'injuryincident'
),

cc_policylocation as (
    select
          id
        , retired
        , addressid
    from {{ ref('v_cc_policylocation_current') }}
    where retired = 0
),

cc_address as (
    select
          id
        , retired
        , addressline1
        , addressline2
        , addressline3
        , city
        , postalcode
    from {{ ref('v_cc_address_current') }}
    where retired = 0
),

cc_workcomp as (
    select
          id
        , retired
        , accidentlocationtype_icare
    from {{ ref('v_cc_workcomp_current') }}
    where retired = 0
),

cctl_accidentloctype_icare as (
    select
          id
        , typecode
        , name
    from {{ ref('v_cctl_accidentloctype_icare_current') }}
),

cc_subrogationsummary as (
    select
          id
        , claimid
        , retired
    from {{ ref('v_cc_subrogationsummary_current') }}
    where retired = 0
),

cc_subrogation as (
    select
          subrogationsummaryid
        , retired
    from {{ ref('v_cc_subrogation_current') }}
    where retired = 0
),

subro as (
    select distinct 
        subrosumm.claimid
    from cc_subrogationsummary subrosumm 
    inner join cc_subrogation subro
        on subro.subrogationsummaryid = subrosumm.id
),

final as (
    select distinct
          convert(varchar(32), hashbytes('md5', concat('GWCC', clm.claimnumber)), 2) as claim_sk
        , 'GWCC' as src_system_cd
        , clm.id as src_claim_id
        , clm.claimnumber as claim_nbr
        , case 
            when subro.claimid is not null then 'Y' 
            else 'N' 
          end as recovery_investigation_ind
        , concat(
              rtrim(
                  concat(pollocaddr.addressline1, ' '
                       , pollocaddr.addressline2, ' '
                       , pollocaddr.addressline3
                  )
              )
            , ' ', pollocaddr.city, ' '
            , pollocaddr.postalcode
          ) as policy_location_addr
        , inc.mechanismofinjurydesc_icare as toocs_mechanism_if_injury_desc
        , dimacc.typecode as accident_location_type_cd
        , dimacc.name as accident_location_type_desc

    from cc_claim clm

    join cc_incident inc
        on clm.id = inc.claimid

    join cctl_incident cctl_incident
        on cctl_incident.id = inc.subtype

    left join cc_policylocation polloc
        on clm.locationcodeid = polloc.id

    join cc_address pollocaddr
        on polloc.addressid = pollocaddr.id

    left join cc_workcomp wrkcomp
        on clm.claimworkcompid = wrkcomp.id

    join cctl_accidentloctype_icare dimacc
        on wrkcomp.accidentlocationtype_icare = dimacc.id

    left join subro
        on subro.claimid = clm.id
)

select * from final
