from typing import List
import os
import yaml
import datetime
import time
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
    # Upload dependent JARS
    resource_files = []
    for application, task in application_tasks:
        task_definition_resource_file = helpers.upload_text_to_container(container_name=job.id,
                                                                          application_name=application.name + '.yaml',
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


def __get_recent_job(spark_client, job_id):
    job_schedule = spark_client.batch_client.job_schedule.get(job_id)
    return spark_client.batch_client.job.get(job_schedule.execution_info.recent_job.id)


def list_applications(spark_client, job_id):
    job = spark_client.get_job(job_id)

    # get tasks from Batch job
    return [application.id for application in spark_client.batch_client.task.list(job.recent_run_id) if application.id != job_id]


# def disable(spark_client, job_id):
#     # disable the currently running job from the job schedule if exists
#     recent_run_job = __get_recent_job(spark_client, job_id)
#     if recent_run_job.id and recent_run_job.state == batch_models.JobState.active:
#         spark_client.batch_client.job.disable(job_id=recent_run_job.id, disable_tasks=batch_models.DisableJobOption.requeue)
   
#     # disable the job_schedule
#     spark_client.batch_client.job_schedule.disable(job_id)


# def enable(spark_client, job_id):
#     # disable the currently running job from the job schedule if exists
#     recent_run_job = __get_recent_job(spark_client, job_id)
#     if recent_run_job.id and recent_run_job.state == batch_models.JobState.active:
#         spark_client.batch_client.job.enable(job_id=recent_run_job.id)
   
#     # disable the job_schedule
#     spark_client.batch_client.job_schedule.enable(job_id)


def stop(spark_client, job_id):
    # terminate currently running job and tasks
    recent_run_job = __get_recent_job(spark_client, job_id)
    spark_client.batch_client.job.terminate(recent_run_job.id)
    # terminate job_schedule
    spark_client.batch_client.job_schedule.terminate(job_id)


def delete(spark_client, job_id):
    recent_run_job = __get_recent_job(spark_client, job_id)
    spark_client.batch_client.job.terminate(recent_run_job.id)

    # delete job_schedule
    spark_client.batch_client.job_schedule.delete(job_id)


def get_application(spark_client, job_id, app_id):
    # info about the app
    recent_run_job = __get_recent_job(spark_client, job_id)
    return spark_client.batch_client.task.get(job_id=recent_run_job.id, task_id=app_id)


def get_application_log(spark_client, job_id, app_id):
    # TODO: change where the logs are uploaded so they aren't overwritten on scheduled runs
    #           current: job_id, app_id/output.log
    #           new: job_id, recent_run_job.id/app_id/output.log

    # recent_run_job = __get_recent_job(spark_client, job_id)
    # return spark_client.get_application_log(recent_run_job.id.replace(":", "-"), app_id)
    return spark_client.get_application_log(job_id, app_id)


def stop_app(spark_client, job_id, app_id):
    recent_run_job = __get_recent_job(spark_client, job_id)

    # TODO: stop spark job on node -- ssh in, stop ?

    # stop batch task
    spark_client.batch_client.task.stop(job_id=recent_run_job.id, task_id=app_id)

def wait_until_job_finished(spark_client, job_id):
    job_state = spark_client.batch_client.job_schedule.get(job_id).state

    while job_state != batch_models.JobScheduleState.completed:
        time.sleep(3)
        job_state = spark_client.batch_client.job_schedule.get(job_id).state
