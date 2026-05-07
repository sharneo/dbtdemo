{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_commutation_icare.
                                                commutation_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_commutation_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CommutationDecision_icare::NUMBER AS commutationdecision_icare,
                CAST(data_payload:Calculation_icare::TEXT AS VARCHAR(16777216)) AS calculation_icare,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:DecisionDate_icare::NUMBER/1000) AS decisiondate_icare,
                data_payload:Eligibility_icare::BOOLEAN AS eligibility_icare,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:MaximumOfferAmount_icare AS NUMBER(18,2)) AS maximumofferamount_icare,
                data_payload:Claim_icareID::NUMBER AS claim_icareid,
                CAST(data_payload:InitialOffer_icare AS NUMBER(18,2)) AS initialoffer_icare,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:SiraCertificationNumber_icare::TEXT AS VARCHAR(16777216)) AS siracertificationnumber_icare,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:WccMatterNumber_icare::TEXT AS VARCHAR(16777216)) AS wccmatternumber_icare,
                TO_TIMESTAMP_TZ(data_payload:ActionDate_icare::NUMBER/1000) AS actiondate_icare,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:Rationale_icare::TEXT AS VARCHAR(16777216)) AS rationale_icare,
                data_payload:Attorney_icareID::NUMBER AS attorney_icareid,
                TO_TIMESTAMP_TZ(data_payload:WccRegistrationDate_icare::NUMBER/1000) AS wccregistrationdate_icare,
                TO_TIMESTAMP_TZ(data_payload:SiraCertificationDate_icare::NUMBER/1000) AS siracertificationdate_icare,
                data_payload:CommutationType_icare::NUMBER AS commutationtype_icare,
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
            FROM {{ source('gwcc', 'ccx_commutation_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:commutationdecision_icare::NUMBER AS commutationdecision_icare,
                CAST($1:calculation_icare::TEXT AS VARCHAR(16777216)) AS calculation_icare,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:decisiondate_icare::TIMESTAMP_TZ AS decisiondate_icare,
                $1:eligibility_icare::BOOLEAN AS eligibility_icare,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:maximumofferamount_icare AS NUMBER(18,2)) AS maximumofferamount_icare,
                $1:claim_icareid::NUMBER AS claim_icareid,
                CAST($1:initialoffer_icare AS NUMBER(18,2)) AS initialoffer_icare,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:siracertificationnumber_icare::TEXT AS VARCHAR(16777216)) AS siracertificationnumber_icare,
                $1:id::NUMBER AS id,
                CAST($1:wccmatternumber_icare::TEXT AS VARCHAR(16777216)) AS wccmatternumber_icare,
                $1:actiondate_icare::TIMESTAMP_TZ AS actiondate_icare,
                $1:createuserid::NUMBER AS createuserid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:rationale_icare::TEXT AS VARCHAR(16777216)) AS rationale_icare,
                $1:attorney_icareid::NUMBER AS attorney_icareid,
                $1:wccregistrationdate_icare::TIMESTAMP_TZ AS wccregistrationdate_icare,
                $1:siracertificationdate_icare::TIMESTAMP_TZ AS siracertificationdate_icare,
                $1:commutationtype_icare::NUMBER AS commutationtype_icare,
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
            FROM {{ source('gwcc', 'ccx_commutation_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS commutation_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'commutationdecision_icare',
                        'calculation_icare',
                        'publicid',
                        'decisiondate_icare',
                        'eligibility_icare',
                        'createtime',
                        'maximumofferamount_icare',
                        'claim_icareid',
                        'initialoffer_icare',
                        'updatetime',
                        'siracertificationnumber_icare',
                        'wccmatternumber_icare',
                        'actiondate_icare',
                        'createuserid',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'updateuserid',
                        'rationale_icare',
                        'attorney_icareid',
                        'wccregistrationdate_icare',
                        'siracertificationdate_icare',
                        'commutationtype_icare'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}