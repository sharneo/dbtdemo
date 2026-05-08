{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_dashboardstats.
                                                dashboardstats_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    transient=True,
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "cc_dashboardstats"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:NewLitigation::NUMBER AS newlitigation,
                CAST(data_payload:ExpensesInPeriod AS NUMBER(18,2)) AS expensesinperiod,
                data_payload:OpenClaims::NUMBER AS openclaims,
                CAST(data_payload:OpenReserves AS NUMBER(18,2)) AS openreserves,
                data_payload:OpenExposures::NUMBER AS openexposures,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:NoticeOnly::NUMBER AS noticeonly,
                data_payload:CloseTime::NUMBER AS closetime,
                data_payload:Litigated::NUMBER AS litigated,
                data_payload:StatType::NUMBER AS stattype,
                data_payload:ReopenedClaims::NUMBER AS reopenedclaims,
                CAST(data_payload:ClaimCostsOnClosed AS NUMBER(18,2)) AS claimcostsonclosed,
                CAST(data_payload:ClaimCostsPaid AS NUMBER(18,2)) AS claimcostspaid,
                data_payload:NewClaims::NUMBER AS newclaims,
                data_payload:Flagged::NUMBER AS flagged,
                data_payload:GroupID::NUMBER AS groupid,
                data_payload:ID::NUMBER AS id,
                data_payload:LossType::NUMBER AS losstype,
                data_payload:ClosedExposures::NUMBER AS closedexposures,
                data_payload:CoverageType::NUMBER AS coveragetype,
                CAST(data_payload:TotalIncurredNet AS NUMBER(18,2)) AS totalincurrednet,
                data_payload:ClosedClaims::NUMBER AS closedclaims,
                data_payload:NewNoticeOnly::NUMBER AS newnoticeonly,
                CAST(data_payload:ExpensesOnClosed AS NUMBER(18,2)) AS expensesonclosed,
                CAST(data_payload:ExpensesPaid AS NUMBER(18,2)) AS expensespaid,
                CAST(data_payload:RecoveredInPeriod AS NUMBER(18,2)) AS recoveredinperiod,
                data_payload:Handlers::NUMBER AS handlers,
                data_payload:LOBCode::NUMBER AS lobcode,
                data_payload:NewExposures::NUMBER AS newexposures,
                CAST(data_payload:ClaimCostsInPeriod AS NUMBER(18,2)) AS claimcostsinperiod,
                data_payload:OverIncurredLimit::NUMBER AS overincurredlimit,
                CAST(data_payload:TtlIncNetMinusOpenRecReserves AS NUMBER(18,2)) AS ttlincnetminusopenrecreserves,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS STRING) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'AVRO' file_type
            FROM {{ source('gwcc', 'cc_dashboardstats') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:newlitigation::NUMBER AS newlitigation,
                CAST($1:expensesinperiod AS NUMBER(18,2)) AS expensesinperiod,
                $1:openclaims::NUMBER AS openclaims,
                CAST($1:openreserves AS NUMBER(18,2)) AS openreserves,
                $1:openexposures::NUMBER AS openexposures,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:noticeonly::NUMBER AS noticeonly,
                $1:closetime::NUMBER AS closetime,
                $1:litigated::NUMBER AS litigated,
                $1:stattype::NUMBER AS stattype,
                $1:reopenedclaims::NUMBER AS reopenedclaims,
                CAST($1:claimcostsonclosed AS NUMBER(18,2)) AS claimcostsonclosed,
                CAST($1:claimcostspaid AS NUMBER(18,2)) AS claimcostspaid,
                $1:newclaims::NUMBER AS newclaims,
                $1:flagged::NUMBER AS flagged,
                $1:groupid::NUMBER AS groupid,
                $1:id::NUMBER AS id,
                $1:losstype::NUMBER AS losstype,
                $1:closedexposures::NUMBER AS closedexposures,
                $1:coveragetype::NUMBER AS coveragetype,
                CAST($1:totalincurrednet AS NUMBER(18,2)) AS totalincurrednet,
                $1:closedclaims::NUMBER AS closedclaims,
                $1:newnoticeonly::NUMBER AS newnoticeonly,
                CAST($1:expensesonclosed AS NUMBER(18,2)) AS expensesonclosed,
                CAST($1:expensespaid AS NUMBER(18,2)) AS expensespaid,
                CAST($1:recoveredinperiod AS NUMBER(18,2)) AS recoveredinperiod,
                $1:handlers::NUMBER AS handlers,
                $1:lobcode::NUMBER AS lobcode,
                $1:newexposures::NUMBER AS newexposures,
                CAST($1:claimcostsinperiod AS NUMBER(18,2)) AS claimcostsinperiod,
                $1:overincurredlimit::NUMBER AS overincurredlimit,
                CAST($1:ttlincnetminusopenrecreserves AS NUMBER(18,2)) AS ttlincnetminusopenrecreserves,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::STRING as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'PARQUET' file_type
            FROM {{ source('gwcc', 'cc_dashboardstats') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS dashboardstats_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'newlitigation',
                        'expensesinperiod',
                        'openclaims',
                        'openreserves',
                        'openexposures',
                        'publicid',
                        'noticeonly',
                        'closetime',
                        'litigated',
                        'stattype',
                        'reopenedclaims',
                        'claimcostsonclosed',
                        'claimcostspaid',
                        'newclaims',
                        'flagged',
                        'groupid',
                        'losstype',
                        'closedexposures',
                        'coveragetype',
                        'totalincurrednet',
                        'closedclaims',
                        'newnoticeonly',
                        'expensesonclosed',
                        'expensespaid',
                        'recoveredinperiod',
                        'handlers',
                        'lobcode',
                        'newexposures',
                        'claimcostsinperiod',
                        'overincurredlimit',
                        'ttlincnetminusopenrecreserves'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}