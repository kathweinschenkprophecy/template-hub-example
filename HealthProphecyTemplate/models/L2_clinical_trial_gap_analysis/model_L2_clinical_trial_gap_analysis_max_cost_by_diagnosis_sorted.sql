{{
  config({    
    "materialized": "table",
    "alias": "prophecy_tmp__mbh9lpvn__L2_clinical_trial_gap_analysis__max_cost_by_diagnosis_sorted",
    "database": "product_dev",
    "schema": "default"
  })
}}

WITH max_cost_by_diagnosis AS (

  SELECT * 
  
  FROM {{ source('prophecy_tmp_source__L2_clinical_trial_gap_analysis', 'prophecy_tmp__mbh9lpvn__L2_clinical_trial_gap_analysis__max_cost_by_diagnosis') }}

),

max_cost_by_diagnosis_sorted AS (

  SELECT * 
  
  FROM max_cost_by_diagnosis AS in0
  
  ORDER BY total_cost DESC

)

SELECT *

FROM max_cost_by_diagnosis_sorted
