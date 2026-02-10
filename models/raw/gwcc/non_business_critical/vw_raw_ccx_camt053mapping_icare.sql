
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
    tags=["raw_gwcc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:Reverse::BOOLEAN AS reverse,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:Mid::TEXT AS VARCHAR(255)) AS mid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:GwccInstructionCode::NUMBER AS gwccinstructioncode,
                data_payload:CreditDebitInd::NUMBER AS creditdebitind,
                CAST(data_payload:RecallId::TEXT AS VARCHAR(255)) AS recallid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Skipped::BOOLEAN AS skipped,
                CAST(data_payload:DomainCode::TEXT AS VARCHAR(10)) AS domaincode,
                CAST(data_payload:FamilyCode::TEXT AS VARCHAR(10)) AS familycode,
                CAST(data_payload:SubFamilyCode::TEXT AS VARCHAR(10)) AS subfamilycode,
                CAST(data_payload:PaymentMethodName::TEXT AS VARCHAR(100)) AS paymentmethodname,
                data_payload:Debulked::BOOLEAN AS debulked,
                CAST(data_payload:PaymentMethodCode::TEXT AS VARCHAR(50)) AS paymentmethodcode,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:TranCode::NUMBER AS trancode,
                data_payload:ID::NUMBER AS id,
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
            FROM {{ source('gwcc', 'ccx_camt053mapping_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:reverse::BOOLEAN AS reverse,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:mid::TEXT AS VARCHAR(255)) AS mid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:gwccinstructioncode::NUMBER AS gwccinstructioncode,
                $1:creditdebitind::NUMBER AS creditdebitind,
                CAST($1:recallid::TEXT AS VARCHAR(255)) AS recallid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:skipped::BOOLEAN AS skipped,
                CAST($1:domaincode::TEXT AS VARCHAR(10)) AS domaincode,
                CAST($1:familycode::TEXT AS VARCHAR(10)) AS familycode,
                CAST($1:subfamilycode::TEXT AS VARCHAR(10)) AS subfamilycode,
                CAST($1:paymentmethodname::TEXT AS VARCHAR(100)) AS paymentmethodname,
                $1:debulked::BOOLEAN AS debulked,
                CAST($1:paymentmethodcode::TEXT AS VARCHAR(50)) AS paymentmethodcode,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:trancode::NUMBER AS trancode,
                $1:id::NUMBER AS id,
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
            FROM {{ source('gwcc', 'ccx_camt053mapping_icare') }}
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
                                'loadcommandid',
                        'createuserid',
                        'reverse',
                        'publicid',
                        'beanversion',
                        'mid',
                        'createtime',
                        'retired',
                        'gwccinstructioncode',
                        'creditdebitind',
                        'recallid',
                        'updateuserid',
                        'skipped',
                        'domaincode',
                        'familycode',
                        'subfamilycode',
                        'paymentmethodname',
                        'debulked',
                        'paymentmethodcode',
                        'updatetime',
                        'trancode',
                        'id'
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
        