def get_recent_job(spark_job_operations, job_id):
    job_schedule = spark_job_operations.batch_client.job_schedule.get(job_id)
    return spark_job_operations.batch_client.job.get(job_schedule.execution_info.recent_job.id)
