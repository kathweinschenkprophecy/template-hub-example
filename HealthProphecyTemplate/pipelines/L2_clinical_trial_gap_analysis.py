Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)

with DAG(Schedule = Schedule):
    L0_raw_encounters = Task(
        task_id = "L0_raw_encounters", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "L0_raw_encounters", "sourceType" : "Table", "sourceName" : "analyst_samples.healthcare", "alias" : ""}
    )
    model_L2_clinical_trial_gap_analysis_total_cost_sorted = Task(
        task_id = "model_L2_clinical_trial_gap_analysis_total_cost_sorted", 
        component = "Model", 
        modelName = "model_L2_clinical_trial_gap_analysis_total_cost_sorted"
    )
    L0_bronze_clinical_study_details = Task(
        task_id = "L0_bronze_clinical_study_details", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {
          "name": "l0_bronze_clinical_study_details", 
          "sourceType": "Table", 
          "sourceName": "playground.playground_schema_11083_gd4", 
          "alias": "", 
          "additionalProperties": None
        }
    )
    model_L2_clinical_trial_gap_analysis_max_cost_by_diagnosis = Task(
        task_id = "model_L2_clinical_trial_gap_analysis_max_cost_by_diagnosis", 
        component = "Model", 
        modelName = "model_L2_clinical_trial_gap_analysis_max_cost_by_diagnosis"
    )
    model_L2_clinical_trial_gap_analysis_null_nct_ids = Task(
        task_id = "model_L2_clinical_trial_gap_analysis_null_nct_ids", 
        component = "Model", 
        modelName = "model_L2_clinical_trial_gap_analysis_null_nct_ids"
    )
    model_L2_clinical_trial_gap_analysis_max_cost_by_diagnosis_sorted = Task(
        task_id = "model_L2_clinical_trial_gap_analysis_max_cost_by_diagnosis_sorted", 
        component = "Model", 
        modelName = "model_L2_clinical_trial_gap_analysis_max_cost_by_diagnosis_sorted"
    )
    model_L2_clinical_trial_gap_analysis_total_cost_by_reason_code = Task(
        task_id = "model_L2_clinical_trial_gap_analysis_total_cost_by_reason_code", 
        component = "Model", 
        modelName = "model_L2_clinical_trial_gap_analysis_total_cost_by_reason_code"
    )
    L0_bronze_clinical_study_details.out >> model_L2_clinical_trial_gap_analysis_max_cost_by_diagnosis.in_1
    L0_raw_encounters.out >> model_L2_clinical_trial_gap_analysis_total_cost_by_reason_code.in_0
    (
        model_L2_clinical_trial_gap_analysis_max_cost_by_diagnosis.out_0
        >> [model_L2_clinical_trial_gap_analysis_max_cost_by_diagnosis_sorted.in_0,
              model_L2_clinical_trial_gap_analysis_null_nct_ids.in_0]
    )
    (
        model_L2_clinical_trial_gap_analysis_total_cost_by_reason_code.out_0
        >> [model_L2_clinical_trial_gap_analysis_total_cost_sorted.in_0,
              model_L2_clinical_trial_gap_analysis_max_cost_by_diagnosis.in_0]
    )
