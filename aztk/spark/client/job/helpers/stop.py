from aztk.utils import batch_error_manager


def _stop(core_job_operations, job_id):
    # terminate currently running job and tasks
    recent_run_job = core_job_operations.get_recent_job(job_id)
    core_job_operations.batch_client.job.terminate(recent_run_job.id)
    # terminate job_schedule
    core_job_operations.batch_client.job_schedule.terminate(job_id)


def stop(self, job_id):
    with batch_error_manager():
        return _stop(self, job_id)
