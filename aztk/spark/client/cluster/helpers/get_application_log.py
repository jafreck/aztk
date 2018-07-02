from azure.batch.models import batch_error

from aztk import error
from aztk.utils import helpers


def get_application_log(spark_cluster_operations, cluster_id: str, application_name: str, tail=False, current_bytes: int = 0):
    try:
        return super(type(spark_cluster_operations), spark_cluster_operations).get_application_log(cluster_id, application_name, tail, current_bytes)
    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))
