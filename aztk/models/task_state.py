from enum import Enum


class TaskState(Enum):
    Active = "active"
    Running = "running"
    Completed = "completed"
    Failed = "failed"
    Preparing = "preparing"
