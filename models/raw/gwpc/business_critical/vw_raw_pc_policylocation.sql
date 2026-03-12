
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
    tags=["raw_gwpc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:CityKanjiInternalDenorm::TEXT AS VARCHAR(60)) AS citykanjiinternaldenorm,
                CAST(data_payload:AddressLine1Internal::TEXT AS VARCHAR(60)) AS addressline1internal,
                CAST(data_payload:CountyInternal::TEXT AS VARCHAR(250)) AS countyinternal,
                CAST(data_payload:AddressLine2Internal::TEXT AS VARCHAR(60)) AS addressline2internal,
                CAST(data_payload:AddressLine3Internal::TEXT AS VARCHAR(60)) AS addressline3internal,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:CityKanjiInternal::TEXT AS VARCHAR(60)) AS citykanjiinternal,
                data_payload:IsRatedLocation_icare::BOOLEAN AS isratedlocation_icare,
                CAST(data_payload:AddressLine2KanjiInternal::TEXT AS VARCHAR(60)) AS addressline2kanjiinternal,
                data_payload:StateInternal::NUMBER AS stateinternal,
                data_payload:FixedID::NUMBER AS fixedid,
                data_payload:CountryInternal::NUMBER AS countryinternal,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:EmployeeCountInternal::NUMBER AS employeecountinternal,
                data_payload:IsValidated_icare::BOOLEAN AS isvalidated_icare,
                TO_TIMESTAMP_TZ(data_payload:ValidUntilInternal::NUMBER/1000) AS validuntilinternal,
                data_payload:TaxLocation::NUMBER AS taxlocation,
                data_payload:LocationContactID_icare::NUMBER AS locationcontactid_icare,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:AccountLocation::NUMBER AS accountlocation,
                CAST(data_payload:CityInternalDenorm::TEXT AS VARCHAR(60)) AS cityinternaldenorm,
                data_payload:IndustryCodeID::NUMBER AS industrycodeid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:CityInternal::TEXT AS VARCHAR(60)) AS cityinternal,
                data_payload:ChangeType::NUMBER AS changetype,
                data_payload:AddressTypeInternal::NUMBER AS addresstypeinternal,
                CAST(data_payload:AddressLine1KanjiInternal::TEXT AS VARCHAR(60)) AS addressline1kanjiinternal,
                CAST(data_payload:CEDEXBureauInternal::TEXT AS VARCHAR(2)) AS cedexbureauinternal,
                data_payload:BasedOnID::NUMBER AS basedonid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:LocationNum::NUMBER AS locationnum,
                CAST(data_payload:PostalCodeInternalDenorm::TEXT AS VARCHAR(60)) AS postalcodeinternaldenorm,
                data_payload:CEDEXInternal::BOOLEAN AS cedexinternal,
                data_payload:BuildingAutoNumberSeq::NUMBER AS buildingautonumberseq,
                CAST(data_payload:PostalCodeInternal::TEXT AS VARCHAR(60)) AS postalcodeinternal,
                CAST(data_payload:DescriptionInternal::TEXT AS VARCHAR(255)) AS descriptioninternal,
                data_payload:BranchID::NUMBER AS branchid,
                data_payload:FireProtectClass::NUMBER AS fireprotectclass,
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
            FROM {{ source('gwpc', 'pc_policylocation') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:citykanjiinternaldenorm::TEXT AS VARCHAR(60)) AS citykanjiinternaldenorm,
                CAST($1:addressline1internal::TEXT AS VARCHAR(60)) AS addressline1internal,
                CAST($1:countyinternal::TEXT AS VARCHAR(250)) AS countyinternal,
                CAST($1:addressline2internal::TEXT AS VARCHAR(60)) AS addressline2internal,
                CAST($1:addressline3internal::TEXT AS VARCHAR(60)) AS addressline3internal,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:citykanjiinternal::TEXT AS VARCHAR(60)) AS citykanjiinternal,
                $1:isratedlocation_icare::BOOLEAN AS isratedlocation_icare,
                CAST($1:addressline2kanjiinternal::TEXT AS VARCHAR(60)) AS addressline2kanjiinternal,
                $1:stateinternal::NUMBER AS stateinternal,
                $1:fixedid::NUMBER AS fixedid,
                $1:countryinternal::NUMBER AS countryinternal,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:employeecountinternal::NUMBER AS employeecountinternal,
                $1:isvalidated_icare::BOOLEAN AS isvalidated_icare,
                $1:validuntilinternal::TIMESTAMP_TZ AS validuntilinternal,
                $1:taxlocation::NUMBER AS taxlocation,
                $1:locationcontactid_icare::NUMBER AS locationcontactid_icare,
                $1:createuserid::NUMBER AS createuserid,
                $1:accountlocation::NUMBER AS accountlocation,
                CAST($1:cityinternaldenorm::TEXT AS VARCHAR(60)) AS cityinternaldenorm,
                $1:industrycodeid::NUMBER AS industrycodeid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:cityinternal::TEXT AS VARCHAR(60)) AS cityinternal,
                $1:changetype::NUMBER AS changetype,
                $1:addresstypeinternal::NUMBER AS addresstypeinternal,
                CAST($1:addressline1kanjiinternal::TEXT AS VARCHAR(60)) AS addressline1kanjiinternal,
                CAST($1:cedexbureauinternal::TEXT AS VARCHAR(2)) AS cedexbureauinternal,
                $1:basedonid::NUMBER AS basedonid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:locationnum::NUMBER AS locationnum,
                CAST($1:postalcodeinternaldenorm::TEXT AS VARCHAR(60)) AS postalcodeinternaldenorm,
                $1:cedexinternal::BOOLEAN AS cedexinternal,
                $1:buildingautonumberseq::NUMBER AS buildingautonumberseq,
                CAST($1:postalcodeinternal::TEXT AS VARCHAR(60)) AS postalcodeinternal,
                CAST($1:descriptioninternal::TEXT AS VARCHAR(255)) AS descriptioninternal,
                $1:branchid::NUMBER AS branchid,
                $1:fireprotectclass::NUMBER AS fireprotectclass,
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
            FROM {{ source('gwpc', 'pc_policylocation') }}
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
                                'publicid',
                        'citykanjiinternaldenorm',
                        'addressline1internal',
                        'countyinternal',
                        'addressline2internal',
                        'addressline3internal',
                        'createtime',
                        'citykanjiinternal',
                        'isratedlocation_icare',
                        'addressline2kanjiinternal',
                        'stateinternal',
                        'fixedid',
                        'countryinternal',
                        'effectivedate',
                        'updatetime',
                        'id',
                        'expirationdate',
                        'employeecountinternal',
                        'isvalidated_icare',
                        'validuntilinternal',
                        'taxlocation',
                        'locationcontactid_icare',
                        'createuserid',
                        'accountlocation',
                        'cityinternaldenorm',
                        'industrycodeid',
                        'archivepartition',
                        'beanversion',
                        'cityinternal',
                        'changetype',
                        'addresstypeinternal',
                        'addressline1kanjiinternal',
                        'cedexbureauinternal',
                        'basedonid',
                        'updateuserid',
                        'locationnum',
                        'postalcodeinternaldenorm',
                        'cedexinternal',
                        'buildingautonumberseq',
                        'postalcodeinternal',
                        'descriptioninternal',
                        'branchid',
                        'fireprotectclass'
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
        