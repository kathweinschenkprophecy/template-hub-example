{{
  config({    
    "materialized": "table",
    "alias": "prophecy_tmp__mbh9lpvn__L2_clinical_trial_gap_analysis__max_cost_by_diagnosis",
    "database": "product_dev",
    "schema": "default"
  })
}}

WITH total_cost_by_reason_code AS (

  SELECT * 
  
  FROM {{ source('prophecy_tmp_source__L2_clinical_trial_gap_analysis', 'prophecy_tmp__mbh9lpvn__L2_clinical_trial_gap_analysis__total_cost_by_reason_code') }}

),

clean_null_diagnosis AS (

  {{
    DatabricksSqlBasics.DataCleansing(
      'total_cost_by_reason_code', 
      [
        { "name": "snomed_diagnosis", "dataType": "String" }, 
        { "name": "total_cost", "dataType": "Double" }
      ], 
      'Keep original', 
      ['snomed_diagnosis'], 
      true, 
      'No Diagnosis', 
      false, 
      0, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false
    )
  }}

),

L0_bronze_clinical_study_details AS (

  SELECT * 
  
  FROM {{ source('playground.playground_schema_11083_gd4', 'l0_bronze_clinical_study_details') }}

),

diagnosis_clinical_trials AS (

  SELECT 
    in0.snomed_diagnosis AS snomed_diagnosis,
    in0.total_cost AS total_cost,
    in1.nctId AS nctId,
    in1.briefTitle AS briefTitle
  
  FROM clean_null_diagnosis AS in0
  LEFT JOIN L0_bronze_clinical_study_details AS in1
     ON in1.snomedCode = in0.snomed_diagnosis

),

max_cost_by_diagnosis AS (

  SELECT 
    snomed_diagnosis AS snomed_diagnosis,
    total_cost AS total_cost,
    COLLECT_SET(nctId) AS nctIds
  
  FROM diagnosis_clinical_trials AS in0
  
  GROUP BY 
    snomed_diagnosis, total_cost

)

SELECT *

FROM max_cost_by_diagnosis
