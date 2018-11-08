from aztk.utils import batch_error_manager


def delete(core_job_operations, job_id: str, keep_logs: bool = False):
    with batch_error_manager():
        return core_job_operations.delete_batch_resources(job_id, keep_logs)
