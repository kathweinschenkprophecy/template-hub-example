Config = {"executionDate" : Date("'2025-01-01'")}
Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)

with DAG(Config = Config, Schedule = Schedule):
    L0_raw_encounters = Task(
        task_id = "L0_raw_encounters", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "L0_raw_encounters", "sourceType" : "Table", "sourceName" : "analyst_samples.healthcare", "alias" : ""}
    )
    model_L2_gold_daily_billing_report_patient_billings_by_provider = Task(
        task_id = "model_L2_gold_daily_billing_report_patient_billings_by_provider", 
        component = "Model", 
        modelName = "model_L2_gold_daily_billing_report_patient_billings_by_provider"
    )
    L0_raw_encounters.out >> model_L2_gold_daily_billing_report_patient_billings_by_provider.in_0
