from aztk.utils import batch_error_manager


def _stop(core_job_operations, job_id):
    # terminate currently running job and tasks
    job = core_job_operations.batch_client.job.get(job_id)
    core_job_operations.batch_client.job.terminate(job.id)


def stop(self, job_id):
    with batch_error_manager():
        return _stop(self, job_id)
