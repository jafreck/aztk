import sys
import os
import yaml
import subprocess
import datetime
from typing import List
import azure.storage.blob as blob
import azure.batch.models as batch_models
from command_builder import CommandBuilder
from core import config




def schedule_tasks(tasks_path):

    '''
        Handle the request to submit a task
    '''
    batch_client = config.batch_client
    blob_client = config.blob_client
    
    for task_definition in tasks_path:
        with open(task_definition, 'r') as stream:
            try:
                task = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        print(type(task))

        # resource_files = []

        # for resource_file_definition in task['resource_files']:
        #     resource_files.append(
        #         batch_modelsresource_file.ResourceFile(
        #             blob_source=resource_file_definition['blob_source']
        #             file_path=resource_file_definition['file_path']
        #         )
        #     )
        # # define batch_models.TaskAddParameter
        # task = batch_models.TaskAddParameter(
        #     id=task['id'],
        #     command_line=task['command_line'],
        #     resource_files=resource_files,
        #     user_identity=batch_models.UserIdentity(
        #         auto_user=batch_models.AutoUserSpecification(
        #             scope=batch_models.AutoUserScope.task,
        #             elevation_level=batch_models.ElevationLevel.admin))
        # )

        # schedule the task
        batch_client.task.add(job_id=os.environ['AZ_BATCH_JOB_ID'], task=task)


    

if __name__ == "__main__":
    tasks_path = []
    for file in os.listdir(os.environ['AZ_BATCH_TASK_WORKING_DIR']):
        if file.endswith(".yaml"):
            tasks_path.append(os.path.join(os.environ['AZ_BATCH_TASK_WORKING_DIR'], file))

    schedule_tasks(tasks_path)