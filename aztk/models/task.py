from datetime import datetime

from aztk.core.models import Model, fields


class Task(Model):
    id = fields.String()
    node_id = fields.String()
    state = fields.String(default=None)
    command_line = fields.String(default=None)
    exit_code = fields.Integer(default=None)
    start_time = fields.Datetime(datetime, default=None)
    end_time = fields.Datetime(datetime, default=None)
    failure_info = fields.String(default=None)

    def convert_batch_task_to_aztk_task(self, batch_task):
        self.id = batch_task.id
        self.state = batch_task.state
        self.command_line = batch_task.command_line
        self.exit_code = batch_task.execution_info.exit_code
        self.start_time = batch_task.execution_info.start_time
        self.end_time = batch_task.execution_info.end_time
        self.failure_info = batch_task.execution_info.failure_info.message

        return self
