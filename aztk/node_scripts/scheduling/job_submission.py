import os

import azure.batch.models as batch_models
import yaml

import common
import scheduling_target
from aztk.node_scripts.core import config
from aztk.node_scripts.install.pick_master import get_master_node_id


def read_tasks():
    tasks_path = []
    for file in os.listdir(os.environ["AZ_BATCH_TASK_WORKING_DIR"]):
        if file.endswith(".yaml"):
            tasks_path.append(os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], file))

    tasks = []
    for task_definition in tasks_path:
        with open(task_definition, "r", encoding="UTF-8") as stream:
            try:
                task = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
    return tasks


def affinitize_task_to_master(batch_client, cluster_id, task):
    pool = batch_client.pool.get(config.pool_id)
    master_node_id = get_master_node_id(pool)
    master_node = batch_client.compute_node.get(pool_id=cluster_id, node_id=master_node_id)
    task.affinity_info = batch_models.AffinityInformation(affinity_id=master_node.affinity_id)
    return task


def schedule_tasks(tasks):
    """
        Handle the request to submit a task
    """
    batch_client = config.batch_client

    for task in tasks:
        # affinitize task to master
        task = affinitize_task_to_master(batch_client, os.environ["AZ_BATCH_POOL_ID"], task)
        # schedule the task
        batch_client.task.add(job_id=os.environ["AZ_BATCH_JOB_ID"], task=task)


def get_scheduling_target():
    pass


def schedule_with_target(tasks):
    for task in tasks:
        # this may be running on the "wrong" node
        # need to detect scheduling_target for task
        # then need to generate sas url and node_run on "correct node"
        scheduling_target.download_task_resource_files(task.id, task.resource_files)

        application = common.load_application(os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], "application.yaml"))


if __name__ == "__main__":
    tasks = read_tasks() # this assumes resources already downloaded...  
    
    import sys
    scheduling_target sys.argv.get(1)
    if scheduling_target == "master":
        schedule_with_target(tasks)
    else:
        schedule_tasks(tasks)
