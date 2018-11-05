from aztk.spark.models import ApplicationState
from aztk.utils import batch_error_manager


def get_application_state(core_cluster_operations, cluster_id: str, app_name: str):
    with batch_error_manager():
        return ApplicationState(core_cluster_operations.get_task_state(cluster_id, app_name).value)
