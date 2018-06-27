import azure.batch.models as batch_models
import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.spark import models
from aztk.utils import helpers

from .list_applications import list_applications
from .get_recent_job import get_recent_job


def _get_application_log(spark_client, job_id, application_name):
    # TODO: change where the logs are uploaded so they aren't overwritten on scheduled runs
    #           current: job_id, application_name/output.log
    #           new: job_id, recent_run_job.id/application_name/output.log
    recent_run_job = get_recent_job(spark_client, job_id)
    try:
        task = spark_client.batch_client.task.get(job_id=recent_run_job.id, task_id=application_name)
    except batch_models.batch_error.BatchErrorException as e:
        # see if the application is written to metadata of pool
        applications = list_applications(spark_client, job_id)

        for application in applications:
            if applications[application] is None and application == application_name:
                raise error.AztkError("The application {0} has not yet been created.".format(application))
        raise error.AztkError("The application {0} does not exist".format(application_name))
    else:
        if task.state in (batch_models.TaskState.active, batch_models.TaskState.running,
                          batch_models.TaskState.preparing):
            raise error.AztkError("The application {0} has not yet finished executing.".format(application_name))

        return spark_client.cluster.get_application_log(job_id, application_name)


def get_job_application_log(spark_job_client, job_id, application_name):
    try:
        return _get_application_log(spark_job_client, job_id, application_name)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))