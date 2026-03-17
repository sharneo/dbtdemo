{% snapshot snap_acdoca %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for ACDOCA
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='acdoca_sk',
    alias ='acdoca',
    strategy='check',
    check_cols=['ryear', 'rmvct', 'vorgn', 'vrgng', 'bttype', 'awtyp', 'aworg', 'awref', 'awitem', 'awitgrp', 'subta', 'xreversing', 'xreversed', 'xtruerev', 'awtyp_rev', 'aworg_rev', 'awref_rev', 'rtcur', 'rwcur', 'rhcur', 'rkcur', 'rco_ocur', 'runit', 'racct', 'rcntr', 'prctr', 'kokrs', 'scntr', 'pprctr', 'rassc', 'tsl', 'wsl', 'hsl', 'ksl', 'co_osl', 'msl', 'drcrk', 'poper', 'periv', 'fiscyearper', 'budat', 'bldat', 'blart', 'buzei', 'zuonr', 'bschl', 'bstat', 'linetype', 'ktosl', 'slalittype', 'xsplitmod', 'usnam', 'timestamp', 'eprctr', 'rhoart', 'glaccount_type', 'ktopl', 'ebeln', 'ebelp', 'sgtxt', 'werks', 'lifnr', 'kunnr', 'koart', 'mwskz', 'hbkid', 'hktid', 'xopvw', 'augdt', 'augbl', 'auggj', 'afabe', 'anln1', 'anln2', 'bzdat', 'anbwa', 'movcat', 'depr_period', 'anlgr', 'anlgr2', 'settlement_rule', 'bwkey', 'objnr', 'hrkft', 'hkgrp', 'parob1', 'parobsrc', 'uspob', 'co_belkz', 'co_beknz', 'beltp', 'muvflg', 'gkont', 'gkoar', 'scope', 'pbukrs', 'pscope', 'aufnr_org', 'ukostl', 'accas', 'accasty', 'aufnr', 'paccas', 'paccasty', 'co_belnr', 'co_buzei', 'co_refbz', 'zzchannel', 'zzproduct', 'zzuwyear', 'zzaccyear', 'zzclaimno', 'zzpolicyno', 'zzdebtorname', 'zzcounterp', 'zzunitprc', 'zzaccount', 'zzprimeno', 'zzinvno', 'zzriskdt', 'zzpolenddt', 'zzgwid', 'zztranno', 'mig_source', 'mig_docln', 'accyear_icare'],
    tags=['sap','snapshot','snp_acdoca']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'rldnr',
            'rbukrs',
            'gjahr',
            'belnr',
            'docln'
    ]) }} AS acdoca_sk,
        *
    FROM {{ source('sap', 'acdoca') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}