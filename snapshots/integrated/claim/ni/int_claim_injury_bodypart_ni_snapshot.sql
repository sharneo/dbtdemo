{% snapshot int_claim_injury_bodypart_ni_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-03-30      0.0                             This Builds the Integrated Layer for claim Injury for NI
2026-03-30      0.0                             AF Changes

-#}

{{
    config(
        target_schema='integrated_ni',
        unique_key='injury_body_part_sk',
        alias='int_claim_injury_bodypart_ni',
        strategy='check',
        check_cols='all',
        tags=['claim', 'integrated', 'NI', 'snapshot_ni']
    )
}}
-- Get latest cc_incident record per hash_key
WITH cte_incident AS (
    SELECT
        hash_key,
        id,
        claimid,
        claimincident,
        subtype,
        sourc_system
    from
        {{ ref('v_cc_incident_current') }}
    WHERE claimincident=1
),
-- Get latest ccx_toocsbloiconnector_icare record
cte_ccx_toocsbloiconnector_icare AS (
    SELECT
        injurycode,
        injurydescription,
        selectedasprimary,
        id,
        retired,
        createtime,
        updatetime,
        injuryincident_icareid
    FROM
        {{ ref('v_ccx_toocsbloiconnector_icare_current') }}
),
--get latest record from cctl_incident
cte_cctl_incident AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_incident_current') }}
    WHERE typecode = 'InjuryIncident'
),
--get latest record from int_claim_injury_ni
cte_int_claim_injury_ni AS (
    SELECT
        loss_sk,
        src_incident_id
    FROM
        { { ref('int_claim_injury_ni_snapshot') } }
    WHERE
        dbt_valid_to is null
),
cte_join AS (
    SELECT
      {{ dbt_utils.generate_surrogate_key(['i.id','toocs.id']) }} AS injury_body_part_sk,
        loss_sk,
        injurycode as injury_code,
        sourc_system,
        injurydescription as body_part_desc,
        case
            when selectedasprimary = 0
            or selectedasprimary is null then 'N'
            when selectedasprimary >= 1 then 'Y'
        end as is_body_part_primary_ind,
        toocs.id as src_body_location_of_injury_id,
        case
            when retired = 0
            or retired is null then 'N'
            when retired >= 1 then 'Y'
        end as retired_ind,
        createtime as src_create_ts,
        updatetime as src_eff_ts,
   from
        cte_incident i 
        inner join cte_ccx_toocsbloiconnector_icare toocs on toocs.injuryincident_icareid = i.id
        left join cte_cctl_incident ci on ci.id = i.subtype
        inner join cte_int_claim_injury_ni inj on inj.src_incident_id=i.id
)
-- Deduplicate to latest record per injury_body_part_sk
SELECT
    *
FROM
    cte_join QUALIFY ROW_NUMBER() OVER (
        PARTITION BY injury_body_part_sk
        ORDER BY
            src_eff_ts DESC
    ) = 1
	
{% endsnapshot %}