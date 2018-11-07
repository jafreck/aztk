from azure.batch.models import BatchErrorException


def stop_app(core_job_operations, job_id, application_name):
    job = core_job_operations.batch_client.job.get(job_id)

    # stop batch task
    try:
        core_job_operations.batch_client.task.terminate(job_id=job.id, task_id=application_name)
        return True
    except BatchErrorException:
        return False
