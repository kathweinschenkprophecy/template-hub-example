Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)

with DAG(Schedule = Schedule):
    L0_raw_clinical_trials = Task(
        task_id = "L0_raw_clinical_trials", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {
          "name": "L0_raw_clinical_trials", 
          "sourceType": "Table", 
          "sourceName": "analyst_samples.healthcare", 
          "alias": ""
        }
    )
    model_L0_raw_clinical_trials_cleanse_clinical_trial_data = Task(
        task_id = "model_L0_raw_clinical_trials_cleanse_clinical_trial_data", 
        component = "Model", 
        modelName = "model_L0_raw_clinical_trials_cleanse_clinical_trial_data"
    )
    L0_raw_clinical_trials.out >> model_L0_raw_clinical_trials_cleanse_clinical_trial_data.in_0
