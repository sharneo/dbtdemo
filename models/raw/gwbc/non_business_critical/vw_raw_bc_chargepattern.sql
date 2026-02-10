
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This Converts Parquet or AVRO Data Loaded in the Variant Column in the RAW DB into Flattend Views
                                                This also creates a HASH_KEY for Incremental Tables for the Curated Layer 
                                                Additional CDA Files are Null in the AVRO but not in CDA .
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    tags=["raw_gwbc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:ChargeCode::TEXT AS VARCHAR(255)) AS chargecode,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:InUse::BOOLEAN AS inuse,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Priority::NUMBER AS priority,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:InvoiceTreatment::NUMBER AS invoicetreatment,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:InternalCharge::BOOLEAN AS internalcharge,
                data_payload:IncludedInEquityDating::BOOLEAN AS includedinequitydating,
                data_payload:TAccountOwnerPatternID::NUMBER AS taccountownerpatternid,
                data_payload:Periodicity::NUMBER AS periodicity,
                data_payload:TAccountsLazyLoaded::BOOLEAN AS taccountslazyloaded,
                data_payload:Reversible::BOOLEAN AS reversible,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                data_payload:Category::NUMBER AS category,
                CAST(data_payload:ChargeName::TEXT AS VARCHAR(255)) AS chargename,
                data_payload:IncludedInDelinquencyPlan_Ext::BOOLEAN AS includedindelinquencyplan_ext,
                data_payload:ReferToRNSW_Ext::BOOLEAN AS refertornsw_ext,
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
            FROM {{ source('gwbc', 'bc_chargepattern') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:chargecode::TEXT AS VARCHAR(255)) AS chargecode,
                $1:createuserid::NUMBER AS createuserid,
                $1:inuse::BOOLEAN AS inuse,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:priority::NUMBER AS priority,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:invoicetreatment::NUMBER AS invoicetreatment,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:internalcharge::BOOLEAN AS internalcharge,
                $1:includedinequitydating::BOOLEAN AS includedinequitydating,
                $1:taccountownerpatternid::NUMBER AS taccountownerpatternid,
                $1:periodicity::NUMBER AS periodicity,
                $1:taccountslazyloaded::BOOLEAN AS taccountslazyloaded,
                $1:reversible::BOOLEAN AS reversible,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                $1:category::NUMBER AS category,
                CAST($1:chargename::TEXT AS VARCHAR(255)) AS chargename,
                $1:includedindelinquencyplan_ext::BOOLEAN AS includedindelinquencyplan_ext,
                $1:refertornsw_ext::BOOLEAN AS refertornsw_ext,
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
            FROM {{ source('gwbc', 'bc_chargepattern') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),
{#-
    Driving CTE Over 
    Transformed CTE is To Create the HASH_KEY Based on the Right Combination
-#}   
cte_transformed AS (
    SELECT
        *,
        CASE
             WHEN file_type = 'AVRO' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'chargecode',
                        'createuserid',
                        'inuse',
                        'publicid',
                        'priority',
                        'beanversion',
                        'createtime',
                        'retired',
                        'invoicetreatment',
                        'updateuserid',
                        'internalcharge',
                        'includedinequitydating',
                        'taccountownerpatternid',
                        'periodicity',
                        'taccountslazyloaded',
                        'reversible',
                        'updatetime',
                        'subtype',
                        'id',
                        'category',
                        'chargename',
                        'includedindelinquencyplan_ext',
                        'refertornsw_ext'
                        ]) }}
            WHEN file_type = 'PARQUET' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'id',
                        'gwcbi_seqval'
                        ]) }}
        END AS hash_key    
    FROM cte_source_data
)
SELECT * FROM cte_transformed
        