class TaskData():
    def __init__(self, table_service, table_id):
        self.table_service = table_service
        self.table_id = table_id

    def create_task_table(self):
        """Create the task table that tracks spark app execution
        Returns:
            `bool`: True if creation is successful
        """
        return self.table_service.create_table(self.table_service, fail_on_exist=True)

    def get_task_table_entries(self, table_id):
        pass

    def insert_task_into_task_table(self, table_id, task):
        pass

    def delete_task_table(self, table_id):
        pass
