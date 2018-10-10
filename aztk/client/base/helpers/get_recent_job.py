# Note: this only works with jobs, not clusts
# cluster impl is planned to change to job schedule
def get_recent_job(core_job_operations, id):
    job_schedule = core_job_operations.batch_client.job_schedule.get(id)
    return core_job_operations.batch_client.job.get(job_schedule.execution_info.recent_job.id)
