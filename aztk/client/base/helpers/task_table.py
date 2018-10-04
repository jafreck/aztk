from azure.common import AzureMissingResourceHttpError

from aztk.models import Task
from aztk.utils import BackOffPolicy, helpers, retry


@retry(
    retry_count=4,
    retry_interval=1,
    backoff_policy=BackOffPolicy.exponential,
    exceptions=(AzureMissingResourceHttpError))
def create_task_table(table_service, id):
    """Create the task table that tracks spark app execution
    Returns:
        `bool`: True if creation is successful
    """
    return table_service.create_table(helpers.convert_id_to_table_id(id), fail_on_exist=True)


@retry(
    retry_count=4,
    retry_interval=1,
    backoff_policy=BackOffPolicy.exponential,
    exceptions=(AzureMissingResourceHttpError))
def list_task_table_entries(table_service, id):
    task_rows = [task_row for task_row in table_service.query_entities(helpers.convert_id_to_table_id(id))]
    # TODO: return generic Task object, which $software will interpret as necessary
    return task_rows


@retry(
    retry_count=4,
    retry_interval=1,
    backoff_policy=BackOffPolicy.exponential,
    exceptions=(AzureMissingResourceHttpError))
def get_task_from_table(table_service, id, task_id):
    entity = table_service.get_entity(helpers.convert_id_to_table_id(id), id, task_id)
    # TODO: enable logger
    # print("Running get_task_from_table: {}".format(entity))
    return entity


@retry(
    retry_count=4,
    retry_interval=1,
    backoff_policy=BackOffPolicy.exponential,
    exceptions=(AzureMissingResourceHttpError))
def insert_task_into_task_table(table_service, id, task):
    return table_service.insert_entity(helpers.convert_id_to_table_id(id), task)


@retry(
    retry_count=4,
    retry_interval=1,
    backoff_policy=BackOffPolicy.exponential,
    exceptions=(AzureMissingResourceHttpError))
def delete_task_table(table_service, id):
    return table_service.delete_table(helpers.convert_id_to_table_id(id))
