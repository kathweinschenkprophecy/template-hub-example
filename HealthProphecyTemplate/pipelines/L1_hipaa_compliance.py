Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)

with DAG(Schedule = Schedule):
    model_L1_hipaa_compliance_clean_geo = Task(
        task_id = "model_L1_hipaa_compliance_clean_geo", 
        component = "Model", 
        modelName = "model_L1_hipaa_compliance_clean_geo"
    )
    L0_raw_patients = Task(
        task_id = "L0_raw_patients", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "L0_raw_patients", "sourceType" : "Table", "sourceName" : "analyst_samples.healthcare", "alias" : ""}
    )
    L0_raw_patients.out >> model_L1_hipaa_compliance_clean_geo.in_0
