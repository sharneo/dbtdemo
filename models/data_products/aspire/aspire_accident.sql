{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Aspire

-#}   

{{ config(
    materialized='table',
    tags=["aspire","daily","sas","legacy"]
) }}
{# ============================================================
   BASE TABLE CTEs — all filtering pushed down
   ============================================================ #}
WITH cc_claim AS (

    SELECT
        id,
        claimnumber,
        locationcodeid,
        claimworkcompid,
        retired
    FROM {{ ref('cc_claim_current') }}

),

cc_incident AS (

    SELECT
        claimid,
        mechanismofinjurydesc_icare,
        subtype,
        claimincident,
        retired
    FROM {{ ref('cc_incident_current') }}

),

cctl_incident AS (

    SELECT
        id,
        typecode
    FROM {{ ref('cctl_incident_current') }}

),

cc_policylocation AS (

    SELECT
        id,
        addressid,
        retired
    FROM {{ ref('cc_policylocation_current') }}

),

cc_address AS (

    SELECT
        id,
        addressline1,
        addressline2,
        addressline3,
        city,
        postalcode,
        retired
    FROM {{ ref('cc_address_current') }}

),

cc_workcomp AS (

    SELECT
        id,
        accidentlocationtype_icare,
        retired
    FROM {{ ref('cc_workcomp_current') }}

),

cctl_accidentloctype_icare AS (

    SELECT
        id,
        typecode,
        name
    FROM {{ ref('cctl_accidentloctype_icare_current') }}

),

cc_subrogationsummary AS (

    SELECT
        id,
        claimid,
        retired
    FROM {{ ref('cc_subrogationsummary_current') }}

),

cc_subrogation AS (

    SELECT
        subrogationsummaryid,
        retired
    FROM {{ ref('cc_subrogation_current') }}

),

subro AS (

    SELECT DISTINCT
        subro_summ.claimid
    FROM cc_subrogationsummary AS subro_summ
    INNER JOIN cc_subrogation AS subro_detail
        ON subro_detail.subrogationsummaryid = subro_summ.id
        AND subro_detail.retired = 0
    WHERE subro_summ.retired = 0

),

final AS (

    SELECT DISTINCT
        MD5(CONCAT('GWCC', clm.claimnumber)) AS claim_sk,
        'GWCC' AS src_system_cd,
        clm.id AS src_claim_id,
        clm.claimnumber AS claim_nbr,
        CASE
            WHEN subro.claimid IS NOT NULL THEN 'Y'
            ELSE 'N'
        END AS recovery_investigation_ind,
        CONCAT(
            RTRIM(
                CONCAT(
                    polloc_addr.addressline1, ' ',
                    polloc_addr.addressline2, ' ',
                    polloc_addr.addressline3
                )
            ),
            ' ', polloc_addr.city, ' ',
            polloc_addr.postalcode
        ) AS policy_location_addr,
        inc.mechanismofinjurydesc_icare AS toocs_mechanism_if_injury_desc,
        dim_acc.typecode AS accident_location_type_cd,
        dim_acc.name AS accident_location_type_desc
    FROM cc_claim AS clm

    INNER JOIN cc_incident AS inc
        ON clm.id = inc.claimid
        AND inc.claimincident = 1
        AND inc.retired = 0

    INNER JOIN cctl_incident AS cctl_inc
        ON cctl_inc.id = inc.subtype
        AND LOWER(cctl_inc.typecode) = 'injuryincident'

    -- NOTE: Original SAS has LEFT JOIN here, but the subsequent INNER JOIN
    -- to cc_address on polloc.addressid effectively makes this an INNER JOIN.
    -- Preserving original join type for fidelity to the SAS logic.
    LEFT JOIN cc_policylocation AS polloc
        ON clm.locationcodeid = polloc.id
        AND polloc.retired = 0

    INNER JOIN cc_address AS polloc_addr
        ON polloc.addressid = polloc_addr.id
        AND polloc_addr.retired = 0

    -- NOTE: Original SAS has LEFT JOIN here, but the subsequent INNER JOIN
    -- to cctl_accidentloctype_icare effectively makes this an INNER JOIN.
    -- Preserving original join type for fidelity to the SAS logic.
    LEFT JOIN cc_workcomp AS wrk_comp
        ON clm.claimworkcompid = wrk_comp.id
        AND wrk_comp.retired = 0

    INNER JOIN cctl_accidentloctype_icare AS dim_acc
        ON wrk_comp.accidentlocationtype_icare = dim_acc.id

    LEFT JOIN subro
        ON subro.claimid = clm.id

    WHERE clm.retired = 0

)

SELECT * FROM final