{{
  config({    
    "materialized": "table",
    "alias": "l0_bronze_clinical_study_details",
    "database": "playground",
    "schema": "playground_schema_11083_gd4"
  })
}}

WITH L0_raw_clinical_trials AS (

  SELECT * 
  
  FROM {{ source('analyst_samples.healthcare', 'L0_raw_clinical_trials') }}

),

filter_malformed_json AS (

  SELECT * 
  
  FROM L0_raw_clinical_trials AS in0
  
  WHERE _corrupt_record IS NULL

),

clinical_trial_conditions_interventions AS (

  SELECT 
    protocolSection.identificationModule.nctId AS nctId,
    protocolSection.identificationModule.briefTitle AS briefTitle,
    protocolSection.designModule.studyType AS studyType,
    protocolSection.statusModule.overallStatus AS overallStatus,
    condition.col AS condition,
    intervention.col AS intervention,
    snomedCode AS snomedCode,
    conditionDescription AS conditionDescription
  
  FROM filter_malformed_json AS in0, 
  LATERAL explode_outer(protocolSection.conditionsModule.conditions) AS condition, 
  LATERAL explode_outer(protocolSection.armsInterventionsModule.interventions) AS intervention

),

cleanse_clinical_trial_data AS (

  {{
    DatabricksSqlBasics.DataCleansing(
      'clinical_trial_conditions_interventions', 
      [
        { "name": "nctId", "dataType": "String" }, 
        { "name": "briefTitle", "dataType": "String" }, 
        { "name": "studyType", "dataType": "String" }, 
        { "name": "overallStatus", "dataType": "String" }, 
        { "name": "condition", "dataType": "String" }, 
        { "name": "intervention", "dataType": "Struct" }, 
        { "name": "snomedCode", "dataType": "String" }, 
        { "name": "conditionDescription", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['conditionDescription'], 
      false, 
      'NA', 
      false, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false
    )
  }}

)

SELECT *

FROM cleanse_clinical_trial_data
