from azure.common import AzureConflictHttpError, AzureMissingResourceHttpError
# pylint: disable=import-error,no-name-in-module
from azure.cosmosdb.table.models import Entity

from aztk.error import AztkError
from aztk.models import Task, TaskState
from aztk.utils import BackOffPolicy, helpers, retry, try_func


def __convert_entity_to_task(entity):
    return Task(
        id=entity.get("RowKey", None),
        node_id=entity.get("node_id", None),
        state=TaskState(entity.get("state", None)),
        state_transition_time=entity.get("state_transition_time", None),
        command_line=entity.get("command_line", None),
        exit_code=entity.get("exit_code", None),
        start_time=entity.get("start_time", None),
        end_time=entity.get("end_time", None),
        failure_info=entity.get("failure_info", None),
    )


def __convert_task_to_entity(partition_key, task):
    return Entity(
        PartitionKey=partition_key,
        RowKey=task.id,
        node_id=task.node_id,
        state=task.state.value,
        state_transition_time=task.state_transition_time,
        command_line=task.command_line,
        exit_code=task.exit_code,
        start_time=task.start_time,
        end_time=task.end_time,
        failure_info=task.failure_info,
    )


@try_func(exception_formatter=None, raise_exception=AztkError, catch_exceptions=(AzureConflictHttpError))
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
@try_func(exception_formatter=None, raise_exception=AztkError, catch_exceptions=(AzureConflictHttpError))
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
@try_func(exception_formatter=None, raise_exception=AztkError, catch_exceptions=(AzureConflictHttpError))
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
@try_func(exception_formatter=None, raise_exception=AztkError, catch_exceptions=(AzureConflictHttpError))
def insert_task_into_task_table(table_service, id, task):
    return table_service.insert_entity(helpers.convert_id_to_table_id(id), __convert_task_to_entity(id, task))


@retry(
    retry_count=4,
    retry_interval=1,
    backoff_policy=BackOffPolicy.exponential,
    exceptions=(AzureMissingResourceHttpError))
@try_func(exception_formatter=None, raise_exception=AztkError, catch_exceptions=(AzureConflictHttpError))
def update_task_in_task_table(table_service, id, task):
    return table_service.update_entity(helpers.convert_id_to_table_id(id), __convert_task_to_entity(id, task))


@retry(
    retry_count=4,
    retry_interval=1,
    backoff_policy=BackOffPolicy.exponential,
    exceptions=(AzureMissingResourceHttpError))
@try_func(exception_formatter=None, raise_exception=AztkError, catch_exceptions=(AzureConflictHttpError))
def delete_task_table(table_service, id):
    return table_service.delete_table(helpers.convert_id_to_table_id(id))
