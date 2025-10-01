{{
  config({    
    "materialized": "table",
    "alias": "L2_gold_clinical_trial_gap_analysis",
    "database": "playground",
    "schema": "playground_schema_11083_gd4"
  })
}}

WITH max_cost_by_diagnosis AS (

  SELECT * 
  
  FROM {{ source('prophecy_tmp_source__L2_clinical_trial_gap_analysis', 'prophecy_tmp__mbh9lpvn__L2_clinical_trial_gap_analysis__max_cost_by_diagnosis') }}

),

null_nct_ids AS (

  SELECT * 
  
  FROM max_cost_by_diagnosis AS in0
  
  WHERE size(nctIds) = 0

)

SELECT *

FROM null_nct_ids
