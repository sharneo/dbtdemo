
{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Current View for the Curated Layer for the Table  
                                                pctl_groupregversion_ext . This view contains only the Latest Record Data in the Table
                                                This View is an Initial One and will need to be revisited. 
                                                1 and 0 Represent Initial Initialisation from CDA
-#}

{{ config(
    materialized='view',
    tags=["daily", "curated","curated_view", "hourly", "curated_pc_view"]
) }}

SELECT
    {{ dbt_utils.star(from=ref('pctl_groupregversion_ext')  ) }}
FROM {{ ref('pctl_groupregversion_ext') }}
