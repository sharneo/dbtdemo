
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
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Active::BOOLEAN AS active,
                data_payload:WitnessStatementInd::NUMBER AS witnessstatementind,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:CoveredPartyType::NUMBER AS coveredpartytype,
                data_payload:PolicyID::NUMBER AS policyid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:EvaluationID::NUMBER AS evaluationid,
                data_payload:ID::NUMBER AS id,
                data_payload:MatterID::NUMBER AS matterid,
                data_payload:ExposureID::NUMBER AS exposureid,
                data_payload:WitnessPosition::NUMBER AS witnessposition,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:WitnessPerspective::TEXT AS VARCHAR(100)) AS witnessperspective,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:IncidentID::NUMBER AS incidentid,
                data_payload:PartyNumber::NUMBER AS partynumber,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:Comments::TEXT AS VARCHAR(255)) AS comments,
                data_payload:Role::NUMBER AS role,
                data_payload:NegotiationID::NUMBER AS negotiationid,
                data_payload:ClaimContactID::NUMBER AS claimcontactid,
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
            FROM {{ source('gwcc', 'cc_claimcontactrole') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:active::BOOLEAN AS active,
                $1:witnessstatementind::NUMBER AS witnessstatementind,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:coveredpartytype::NUMBER AS coveredpartytype,
                $1:policyid::NUMBER AS policyid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:evaluationid::NUMBER AS evaluationid,
                $1:id::NUMBER AS id,
                $1:matterid::NUMBER AS matterid,
                $1:exposureid::NUMBER AS exposureid,
                $1:witnessposition::NUMBER AS witnessposition,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:witnessperspective::TEXT AS VARCHAR(100)) AS witnessperspective,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:incidentid::NUMBER AS incidentid,
                $1:partynumber::NUMBER AS partynumber,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:comments::TEXT AS VARCHAR(255)) AS comments,
                $1:role::NUMBER AS role,
                $1:negotiationid::NUMBER AS negotiationid,
                $1:claimcontactid::NUMBER AS claimcontactid,
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
            FROM {{ source('gwcc', 'cc_claimcontactrole') }}
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
                        'publicid',
                        'active',
                        'witnessstatementind',
                        'createtime',
                        'coveredpartytype',
                        'policyid',
                        'updatetime',
                        'evaluationid',
                        'id',
                        'matterid',
                        'exposureid',
                        'witnessposition',
                        'createuserid',
                        'witnessperspective',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'incidentid',
                        'partynumber',
                        'updateuserid',
                        'comments',
                        'role',
                        'negotiationid',
                        'claimcontactid'
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
        