import datetime
import os
import time
from typing import List

import azure.batch.models as batch_models
import yaml

import aztk.error as error
from aztk.utils import constants, helpers
from aztk.utils.command_builder import CommandBuilder


'''
    Job Submission helper methods
'''
def __app_cmd():
    docker_exec = CommandBuilder("sudo docker exec")
    docker_exec.add_argument("-i")
    docker_exec.add_option("-e", "AZ_BATCH_TASK_WORKING_DIR=$AZ_BATCH_TASK_WORKING_DIR")
    docker_exec.add_option("-e", "AZ_BATCH_JOB_ID=$AZ_BATCH_JOB_ID")
    docker_exec.add_argument("hadoop /bin/bash >> output.log 2>&1 -c \"" \
                             "source ~/.bashrc; " \
                             "export PYTHONPATH=$PYTHONPATH:\$AZTK_WORKING_DIR; " \
                             "cd \$AZ_BATCH_TASK_WORKING_DIR; " \
                             "\$AZTK_WORKING_DIR/.aztk-env/.venv/bin/python \$AZTK_WORKING_DIR/aztk/hadoop/node_scripts/job_submission.py\"")
    return docker_exec.to_str()


def generate_task(hadoop_client, job, application_tasks):
    resource_files = []
    for application, task in application_tasks:
        task_definition_resource_file = helpers.upload_text_to_container(container_name=job.id,
                                                                         application_name=application.name + '.yaml',
                                                                         file_path=application.name + '.yaml',
                                                                         content=yaml.dump(task),
                                                                         blob_client=hadoop_client.blob_client)
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


def __get_recent_job(hadoop_client, job_id):
    job_schedule = hadoop_client.batch_client.job_schedule.get(job_id)
    return hadoop_client.batch_client.job.get(job_schedule.execution_info.recent_job.id)


def list_jobs(hadoop_client):
    return [cloud_job_schedule for cloud_job_schedule in hadoop_client.batch_client.job_schedule.list()]


def list_applications(hadoop_client, job_id):
    recent_run_job = __get_recent_job(hadoop_client, job_id)
    # get application names from Batch job metadata
    applications = {}
    for metadata_item in recent_run_job.metadata:
        if metadata_item.name == "applications":
            for app_name in metadata_item.value.split('\n'):
                applications[app_name] = None

    # get tasks from Batch job
    for task in hadoop_client.batch_client.task.list(recent_run_job.id):
        if task.id != job_id:
            applications[task.id] = task

    return applications


def get_job(hadoop_client, job_id):
    job = hadoop_client.batch_client.job_schedule.get(job_id)
    job_apps = [app for app in
                hadoop_client.batch_client.task.list(job_id=job.execution_info.recent_job.id) if app.id != job_id]
    recent_run_job = __get_recent_job(hadoop_client, job_id)
    pool_prefix = recent_run_job.pool_info.auto_pool_specification.auto_pool_id_prefix
    pool = nodes = None
    for cloud_pool in hadoop_client.batch_client.pool.list():
        if pool_prefix in cloud_pool.id:
            pool = cloud_pool
            break
    if pool:
        nodes = hadoop_client.batch_client.compute_node.list(pool_id=pool.id)
    return job, job_apps, pool, nodes


def disable(hadoop_client, job_id):
    # disable the currently running job from the job schedule if exists
    recent_run_job = __get_recent_job(hadoop_client, job_id)
    if recent_run_job.id and recent_run_job.state == batch_models.JobState.active:
        hadoop_client.batch_client.job.disable(job_id=recent_run_job.id,
                                              disable_tasks=batch_models.DisableJobOption.requeue)

    # disable the job_schedule
    hadoop_client.batch_client.job_schedule.disable(job_id)


def enable(hadoop_client, job_id):
    # disable the currently running job from the job schedule if exists
    recent_run_job = __get_recent_job(hadoop_client, job_id)
    if recent_run_job.id and recent_run_job.state == batch_models.JobState.active:
        hadoop_client.batch_client.job.enable(job_id=recent_run_job.id)

    # disable the job_schedule
    hadoop_client.batch_client.job_schedule.enable(job_id)


def stop(hadoop_client, job_id):
    # terminate currently running job and tasks
    recent_run_job = __get_recent_job(hadoop_client, job_id)
    hadoop_client.batch_client.job.terminate(recent_run_job.id)
    # terminate job_schedule
    hadoop_client.batch_client.job_schedule.terminate(job_id)


def delete(hadoop_client, job_id, keep_logs: bool = False):
    recent_run_job = __get_recent_job(hadoop_client, job_id)
    deleted_job_or_job_schedule = False
    # delete job
    try:
        hadoop_client.batch_client.job.delete(recent_run_job.id)
        deleted_job_or_job_schedule = True
    except batch_models.batch_error.BatchErrorException:
        pass
    # delete job_schedule
    try:
        hadoop_client.batch_client.job_schedule.delete(job_id)
        deleted_job_or_job_schedule = True
    except batch_models.batch_error.BatchErrorException:
        pass

    # delete storage container
    if keep_logs:
        cluster_data = hadoop_client._get_cluster_data(job_id)
        cluster_data.delete_container(job_id)

    return deleted_job_or_job_schedule


def get_application(hadoop_client, job_id, application_name):
    # info about the app
    recent_run_job = __get_recent_job(hadoop_client, job_id)
    try:
        return hadoop_client.batch_client.task.get(job_id=recent_run_job.id, task_id=application_name)
    except batch_models.batch_error.BatchErrorException:
        raise error.AztkError("The hadoop application {0} is still being provisioned or does not exist.".format(application_name))


def get_application_log(hadoop_client, job_id, application_name):
    # TODO: change where the logs are uploaded so they aren't overwritten on scheduled runs
    #           current: job_id, application_name/output.log
    #           new: job_id, recent_run_job.id/application_name/output.log
    recent_run_job = __get_recent_job(hadoop_client, job_id)
    try:
        task = hadoop_client.batch_client.task.get(job_id=recent_run_job.id, task_id=application_name)
    except batch_models.batch_error.BatchErrorException as e:
        print(e)
        # see if the application is written to metadata of pool
        applications = list_applications(hadoop_client, job_id)
        print(applications)
        for application in applications:
            if applications[application] is None and application == application_name:
                raise error.AztkError("The application {0} has not yet been created.".format(application))
        raise error.AztkError("The application {0} does not exist".format(application_name))
    else:
        if task.state in (batch_models.TaskState.active, batch_models.TaskState.running, batch_models.TaskState.preparing):
            raise error.AztkError("The application {0} has not yet finished executing.".format(application_name))

        return hadoop_client.get_application_log(job_id, application_name)


def stop_app(hadoop_client, job_id, application_name):
    recent_run_job = __get_recent_job(hadoop_client, job_id)

    # stop batch task
    try:
        hadoop_client.batch_client.task.terminate(job_id=recent_run_job.id, task_id=application_name)
        return True
    except batch_models.batch_error.BatchErrorException:
        return False

def wait_until_job_finished(hadoop_client, job_id):
    job_state = hadoop_client.batch_client.job_schedule.get(job_id).state

    while job_state != batch_models.JobScheduleState.completed:
        time.sleep(3)
        job_state = hadoop_client.batch_client.job_schedule.get(job_id).state
