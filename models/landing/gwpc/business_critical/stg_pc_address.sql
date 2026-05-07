{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pc_address.
                                                address_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "business_critical", "pc_address"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:LocationName::TEXT AS VARCHAR(255)) AS locationname,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BatchGeocode::BOOLEAN AS batchgeocode,
                data_payload:Active::BOOLEAN AS active,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:AddressLine1::TEXT AS VARCHAR(60)) AS addressline1,
                CAST(data_payload:AddressLine2::TEXT AS VARCHAR(60)) AS addressline2,
                CAST(data_payload:County::TEXT AS VARCHAR(500)) AS county,
                CAST(data_payload:AddressLine3::TEXT AS VARCHAR(60)) AS addressline3,
                CAST(data_payload:CityKanji::TEXT AS VARCHAR(60)) AS citykanji,
                CAST(data_payload:SpatialPoint::TEXT AS VARCHAR(16777216)) AS spatialpoint,
                CAST(data_payload:AddressLine2Kanji::TEXT AS VARCHAR(60)) AS addressline2kanji,
                CAST(data_payload:PhoneExtension::TEXT AS VARCHAR(60)) AS phoneextension,
                data_payload:State::NUMBER AS state,
                CAST(data_payload:AddressBookUID::TEXT AS VARCHAR(64)) AS addressbookuid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Country::NUMBER AS country,
                data_payload:ID::NUMBER AS id,
                data_payload:IsValidated_icare::BOOLEAN AS isvalidated_icare,
                data_payload:EmployeeCount::NUMBER AS employeecount,
                CAST(data_payload:LocationCode::TEXT AS VARCHAR(255)) AS locationcode,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:ValidUntil::NUMBER/1000) AS validuntil,
                data_payload:PhoneCountry::NUMBER AS phonecountry,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:CityDenorm::TEXT AS VARCHAR(60)) AS citydenorm,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:City::TEXT AS VARCHAR(60)) AS city,
                TO_TIMESTAMP_TZ(data_payload:LastUpdateTime::NUMBER/1000) AS lastupdatetime,
                data_payload:Account::NUMBER AS account,
                CAST(data_payload:Phone::TEXT AS VARCHAR(30)) AS phone,
                data_payload:AddressType::NUMBER AS addresstype,
                CAST(data_payload:AddressLine1Kanji::TEXT AS VARCHAR(60)) AS addressline1kanji,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:CEDEXBureau::TEXT AS VARCHAR(2)) AS cedexbureau,
                data_payload:GeocodeStatus::NUMBER AS geocodestatus,
                data_payload:LocationNum::NUMBER AS locationnum,
                data_payload:CEDEX::BOOLEAN AS cedex,
                CAST(data_payload:PostalCodeDenorm::TEXT AS VARCHAR(60)) AS postalcodedenorm,
                CAST(data_payload:PostalCode::TEXT AS VARCHAR(60)) AS postalcode,
                data_payload:Referenced::BOOLEAN AS referenced,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:LinkedAddress::NUMBER AS linkedaddress,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
                TO_TIMESTAMP_TZ(data_payload:TemporaryLastUpdateTime::NUMBER/1000) AS temporarylastupdatetime,
                data_payload:NonSpecific::BOOLEAN AS nonspecific,
                data_payload:ObfuscatedInternal::BOOLEAN AS obfuscatedinternal,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
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
            FROM {{ source('gwpc', 'pc_address') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:locationname::TEXT AS VARCHAR(255)) AS locationname,
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:batchgeocode::BOOLEAN AS batchgeocode,
                $1:active::BOOLEAN AS active,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:addressline1::TEXT AS VARCHAR(60)) AS addressline1,
                CAST($1:addressline2::TEXT AS VARCHAR(60)) AS addressline2,
                CAST($1:county::TEXT AS VARCHAR(500)) AS county,
                CAST($1:addressline3::TEXT AS VARCHAR(60)) AS addressline3,
                CAST($1:citykanji::TEXT AS VARCHAR(60)) AS citykanji,
                CAST($1:spatialpoint::TEXT AS VARCHAR(16777216)) AS spatialpoint,
                CAST($1:addressline2kanji::TEXT AS VARCHAR(60)) AS addressline2kanji,
                CAST($1:phoneextension::TEXT AS VARCHAR(60)) AS phoneextension,
                $1:state::NUMBER AS state,
                CAST($1:addressbookuid::TEXT AS VARCHAR(64)) AS addressbookuid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:country::NUMBER AS country,
                $1:id::NUMBER AS id,
                $1:isvalidated_icare::BOOLEAN AS isvalidated_icare,
                $1:employeecount::NUMBER AS employeecount,
                CAST($1:locationcode::TEXT AS VARCHAR(255)) AS locationcode,
                $1:createuserid::NUMBER AS createuserid,
                $1:validuntil::TIMESTAMP_TZ AS validuntil,
                $1:phonecountry::NUMBER AS phonecountry,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:citydenorm::TEXT AS VARCHAR(60)) AS citydenorm,
                $1:retired::NUMBER AS retired,
                CAST($1:city::TEXT AS VARCHAR(60)) AS city,
                $1:lastupdatetime::TIMESTAMP_TZ AS lastupdatetime,
                $1:account::NUMBER AS account,
                CAST($1:phone::TEXT AS VARCHAR(30)) AS phone,
                $1:addresstype::NUMBER AS addresstype,
                CAST($1:addressline1kanji::TEXT AS VARCHAR(60)) AS addressline1kanji,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:cedexbureau::TEXT AS VARCHAR(2)) AS cedexbureau,
                $1:geocodestatus::NUMBER AS geocodestatus,
                $1:locationnum::NUMBER AS locationnum,
                $1:cedex::BOOLEAN AS cedex,
                CAST($1:postalcodedenorm::TEXT AS VARCHAR(60)) AS postalcodedenorm,
                CAST($1:postalcode::TEXT AS VARCHAR(60)) AS postalcode,
                $1:referenced::BOOLEAN AS referenced,
                $1:subtype::NUMBER AS subtype,
                $1:linkedaddress::NUMBER AS linkedaddress,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
                $1:temporarylastupdatetime::TIMESTAMP_TZ AS temporarylastupdatetime,
                $1:nonspecific::BOOLEAN AS nonspecific,
                $1:obfuscatedinternal::BOOLEAN AS obfuscatedinternal,
                $1:archivepartition::NUMBER AS archivepartition,
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
            FROM {{ source('gwpc', 'pc_address') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS address_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'locationname',
                        'loadcommandid',
                        'publicid',
                        'batchgeocode',
                        'active',
                        'createtime',
                        'addressline1',
                        'addressline2',
                        'county',
                        'addressline3',
                        'citykanji',
                        'spatialpoint',
                        'addressline2kanji',
                        'phoneextension',
                        'state',
                        'addressbookuid',
                        'updatetime',
                        'country',
                        'isvalidated_icare',
                        'employeecount',
                        'locationcode',
                        'createuserid',
                        'validuntil',
                        'phonecountry',
                        'beanversion',
                        'citydenorm',
                        'retired',
                        'city',
                        'lastupdatetime',
                        'account',
                        'phone',
                        'addresstype',
                        'addressline1kanji',
                        'updateuserid',
                        'cedexbureau',
                        'geocodestatus',
                        'locationnum',
                        'cedex',
                        'postalcodedenorm',
                        'postalcode',
                        'referenced',
                        'subtype',
                        'linkedaddress',
                        'description',
                        'temporarylastupdatetime',
                        'nonspecific',
                        'obfuscatedinternal',
                        'archivepartition'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}