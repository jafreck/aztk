import azure.batch.models as batch_models
from azure.batch.models import BatchErrorException

from aztk import error
from aztk.spark import models
from aztk.utils import helpers


def _get_application(core_operations, job_id, application_name):
    try:
        return core_operations.get_task(id=job_id, task_id=application_name)
    except batch_models.BatchErrorException:
        raise error.AztkError(
            "The Spark application {0} is still being provisioned or does not exist.".format(application_name))


def get_application(core_operations, job_id, application_name):
    from aztk.utils import batch_error_manager
    with batch_error_manager():
        return models.Application(_get_application(core_operations, job_id, application_name))
