from aztk.utils import batch_error_manager


# Note: this only works with jobs, not clusters
# cluster impl is planned to change to job schedule
def get_recent_job(core_job_operations, id):
    with batch_error_manager():
        job_schedule = core_job_operations.batch_client.job_schedule.get(id)
        return core_job_operations.batch_client.job.get(job_schedule.execution_info.recent_job.id)
