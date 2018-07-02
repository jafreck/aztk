import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.utils import helpers


def get_application_status(spark_cluster_operations, cluster_id: str, app_name: str):
    try:
        task = spark_cluster_operations.batch_client.task.get(cluster_id, app_name)
        return task.state._value_
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))
