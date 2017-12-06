from typing import List
import os
import yaml
import datetime
import azure.batch.models as batch_models
from aztk_sdk.utils import constants, helpers
from aztk_sdk.utils.command_builder import CommandBuilder

'''
    Job Submission helper methods
'''
def __app_cmd():
    # TODO: convert to CommandBuilder
    return 'sudo docker exec -i -e AZ_BATCH_TASK_WORKING_DIR=$AZ_BATCH_TASK_WORKING_DIR -e AZ_BATCH_JOB_ID=$AZ_BATCH_JOB_ID spark /bin/bash >> output.log 2>&1 -c "python \$DOCKER_WORKING_DIR/job_submission.py"'

def generate_task(spark_client, job, application_tasks):
    time_stamp = str(datetime.datetime.utcnow()).replace(' ', '_')
    # Upload dependent JARS
    resource_files = []
    for application, task in application_tasks:
        task_definition_resource_file = helpers.upload_text_to_container(container_name=job.id,
                                                                          application_name=application.name + '.yaml',
                                                                          time_stamp=time_stamp,
                                                                          file_path=application.name + '.yaml',
                                                                          content=yaml.dump(task),
                                                                          blob_client=spark_client.blob_client)
        resource_files.append(task_definition_resource_file)

    task_cmd = __app_cmd()

    # Create task
    task = batch_models.JobManagerTask(
        id=job.id,
        command_line=helpers.wrap_commands_in_shell([task_cmd]),
        resource_files=resource_files,
        kill_job_on_completion=False, 
        allow_low_priority_node=True,
        user_identity=batch_models.UserIdentity(
            auto_user=batch_models.AutoUserSpecification(
                scope=batch_models.AutoUserScope.task,
                elevation_level=batch_models.ElevationLevel.admin))
    )

    return task


def list_jobs(spark_client):
    pass


def list_applications(spark_client, job):
    pass


def submit(spark_client, job):
    pass


def stop(spark_client, job):
    pass


def delete(spark_client, job):
    pass


def get_app(spark_client, job, app):
    pass


def get_app_logs(spark_client, job):
    pass


def stop_app(spark_client, job, app):
    pass
