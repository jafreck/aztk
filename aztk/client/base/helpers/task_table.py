from aztk.models import Task


def create_task_table(table_service, table_id):
    """Create the task table that tracks spark app execution
    Returns:
        `bool`: True if creation is successful
    """
    return table_service.create_table(table_id, fail_on_exist=True)


def get_task_table_entries(table_service, table_id):
    task_rows = [task_row for task_row in table_service.query_entities(table_id)]
    # TODO: return generic Task object, which $software will interpret as necessary
    return task_rows


def insert_task_into_task_table(table_service, table_id, task):
    return table_service.insert_entity(table_id, task)


def delete_task_table(table_service, table_id):
    return table_service.delete_table(table_id)
