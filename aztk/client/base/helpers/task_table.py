from azure.common import AzureMissingResourceHttpError

from aztk.models import Task
from aztk.utils import BackOffPolicy, helpers, retry


def __convert_entity_to_task(entity):
    print(entity)
    return Task(
        id=entity.get("RowKey", None),
        node_id=entity.get("node_id", None),
        state=entity.get("state", None),
        command_line=entity.get("command_line", None),
        exit_code=entity.get("exit_code", None),
        start_time=entity.get("start_time", None),
        end_time=entity.get("end_time", None),
        failure_info=entity.get("failure_info", None),
    )


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
    tasks = [
        __convert_entity_to_task(task_row)
        for task_row in table_service.query_entities(helpers.convert_id_to_table_id(id))
    ]
    return tasks


@retry(
    retry_count=4,
    retry_interval=1,
    backoff_policy=BackOffPolicy.exponential,
    exceptions=(AzureMissingResourceHttpError))
def get_task_from_table(table_service, id, task_id):
    entity = table_service.get_entity(helpers.convert_id_to_table_id(id), id, task_id)
    # TODO: enable logger
    # print("Running get_task_from_table: {}".format(entity))
    return __convert_entity_to_task(entity)


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
