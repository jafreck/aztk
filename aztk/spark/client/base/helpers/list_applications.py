from aztk.spark import models
from aztk.utils import batch_error_manager


def list_applications(core_operations, id):
    with batch_error_manager():
        return models.Application(core_operations.list_tasks(id))
