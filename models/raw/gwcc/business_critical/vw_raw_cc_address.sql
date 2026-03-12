
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
                data_payload:ObfuscatedInternal::BOOLEAN AS obfuscatedinternal,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BatchGeocode::BOOLEAN AS batchgeocode,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:AddressLine1::TEXT AS VARCHAR(60)) AS addressline1,
                CAST(data_payload:AddressLine2::TEXT AS VARCHAR(60)) AS addressline2,
                CAST(data_payload:County::TEXT AS VARCHAR(500)) AS county,
                CAST(data_payload:AddressLine3::TEXT AS VARCHAR(60)) AS addressline3,
                CAST(data_payload:CityKanji::TEXT AS VARCHAR(60)) AS citykanji,
                CAST(data_payload:SpatialPoint::TEXT AS VARCHAR(16777216)) AS spatialpoint,
                CAST(data_payload:AddressLine2Kanji::TEXT AS VARCHAR(60)) AS addressline2kanji,
                data_payload:State::NUMBER AS state,
                CAST(data_payload:AddressBookUID::TEXT AS VARCHAR(64)) AS addressbookuid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Country::NUMBER AS country,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:StandardizedAddressID_icare::TEXT AS VARCHAR(510)) AS standardizedaddressid_icare,
                data_payload:IsValidated_icare::BOOLEAN AS isvalidated_icare,
                CAST(data_payload:ExternalLinkID::TEXT AS VARCHAR(64)) AS externallinkid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:IsAddressFromPC_icare::BOOLEAN AS isaddressfrompc_icare,
                TO_TIMESTAMP_TZ(data_payload:ValidUntil::NUMBER/1000) AS validuntil,
                CAST(data_payload:DPID_icare::TEXT AS VARCHAR(8)) AS dpid_icare,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:CityDenorm::TEXT AS VARCHAR(40)) AS citydenorm,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:City::TEXT AS VARCHAR(40)) AS city,
                data_payload:AddressType::NUMBER AS addresstype,
                CAST(data_payload:AddressLine1Kanji::TEXT AS VARCHAR(60)) AS addressline1kanji,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:CEDEXBureau::TEXT AS VARCHAR(2)) AS cedexbureau,
                data_payload:GeocodeStatus::NUMBER AS geocodestatus,
                data_payload:CEDEX::BOOLEAN AS cedex,
                CAST(data_payload:PostalCodeDenorm::TEXT AS VARCHAR(20)) AS postalcodedenorm,
                CAST(data_payload:PostalCode::TEXT AS VARCHAR(20)) AS postalcode,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
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
            FROM {{ source('gwcc', 'cc_address') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:obfuscatedinternal::BOOLEAN AS obfuscatedinternal,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:batchgeocode::BOOLEAN AS batchgeocode,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:addressline1::TEXT AS VARCHAR(60)) AS addressline1,
                CAST($1:addressline2::TEXT AS VARCHAR(60)) AS addressline2,
                CAST($1:county::TEXT AS VARCHAR(500)) AS county,
                CAST($1:addressline3::TEXT AS VARCHAR(60)) AS addressline3,
                CAST($1:citykanji::TEXT AS VARCHAR(60)) AS citykanji,
                CAST($1:spatialpoint::TEXT AS VARCHAR(16777216)) AS spatialpoint,
                CAST($1:addressline2kanji::TEXT AS VARCHAR(60)) AS addressline2kanji,
                $1:state::NUMBER AS state,
                CAST($1:addressbookuid::TEXT AS VARCHAR(64)) AS addressbookuid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:country::NUMBER AS country,
                $1:id::NUMBER AS id,
                CAST($1:standardizedaddressid_icare::TEXT AS VARCHAR(510)) AS standardizedaddressid_icare,
                $1:isvalidated_icare::BOOLEAN AS isvalidated_icare,
                CAST($1:externallinkid::TEXT AS VARCHAR(64)) AS externallinkid,
                $1:createuserid::NUMBER AS createuserid,
                $1:isaddressfrompc_icare::BOOLEAN AS isaddressfrompc_icare,
                $1:validuntil::TIMESTAMP_TZ AS validuntil,
                CAST($1:dpid_icare::TEXT AS VARCHAR(8)) AS dpid_icare,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:citydenorm::TEXT AS VARCHAR(40)) AS citydenorm,
                $1:retired::NUMBER AS retired,
                CAST($1:city::TEXT AS VARCHAR(40)) AS city,
                $1:addresstype::NUMBER AS addresstype,
                CAST($1:addressline1kanji::TEXT AS VARCHAR(60)) AS addressline1kanji,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:cedexbureau::TEXT AS VARCHAR(2)) AS cedexbureau,
                $1:geocodestatus::NUMBER AS geocodestatus,
                $1:cedex::BOOLEAN AS cedex,
                CAST($1:postalcodedenorm::TEXT AS VARCHAR(20)) AS postalcodedenorm,
                CAST($1:postalcode::TEXT AS VARCHAR(20)) AS postalcode,
                $1:subtype::NUMBER AS subtype,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
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
            FROM {{ source('gwcc', 'cc_address') }}
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
                        'obfuscatedinternal',
                        'publicid',
                        'batchgeocode',
                        'createtime',
                        'addressline1',
                        'addressline2',
                        'county',
                        'addressline3',
                        'citykanji',
                        'spatialpoint',
                        'addressline2kanji',
                        'state',
                        'addressbookuid',
                        'updatetime',
                        'country',
                        'id',
                        'standardizedaddressid_icare',
                        'isvalidated_icare',
                        'externallinkid',
                        'createuserid',
                        'isaddressfrompc_icare',
                        'validuntil',
                        'dpid_icare',
                        'archivepartition',
                        'beanversion',
                        'citydenorm',
                        'retired',
                        'city',
                        'addresstype',
                        'addressline1kanji',
                        'updateuserid',
                        'cedexbureau',
                        'geocodestatus',
                        'cedex',
                        'postalcodedenorm',
                        'postalcode',
                        'subtype',
                        'description'
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
        