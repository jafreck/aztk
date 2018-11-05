from azure.batch.models import BatchErrorException

from aztk import error
from aztk.utils import helpers


def wait_for_application_to_complete(core_cluster_operations, id, application_name):
    from aztk.utils import batch_error_manager
    with batch_error_manager():
        return core_cluster_operations.wait(id, application_name)
