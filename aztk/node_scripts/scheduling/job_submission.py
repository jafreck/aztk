import os
import sys

import azure.batch.models as batch_models
import yaml

import common
import scheduling_target
from aztk.node_scripts.core import config
from aztk.node_scripts.install.pick_master import get_master_node_id
from aztk.utils import constants


def read_downloaded_tasks():
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


def select_scheduling_target_node(spark_cluster_operations, cluster_id, scheduling_target):
    # for now, limit to only targeting master
    cluster = spark_cluster_operations.get(cluster_id)
    if not cluster.master_node_id:
        return None
    return cluster.master_node_id


def schedule_with_target(scheduling_target, task_sas_urls):
    for task_sas_url in task_sas_urls:
        # this may be running on the "wrong" node
        # need to detect scheduling_target for task
        # then need to node_run on "correct node"
        # this means paramiko MUST be installed on the node

        # want to actually call a spark_client.cluster.submit
        task = common.download_task_definition(task_sas_url)
        task_working_dir = "/mnt/aztk/startup/tasks/workitems/{}".format(task.id)

        task_cmd = (
            r"source ~/.bashrc; "
            r"mkdir -p {0};"
            r"export PYTHONPATH=$PYTHONPATH:$AZTK_WORKING_DIR; "
            r"export AZ_BATCH_TASK_WORKING_DIR={0};"
            r"export STORAGE_LOGS_CONTAINER={1};"
            r"cd $AZ_BATCH_TASK_WORKING_DIR; "
            r'nohup $AZTK_WORKING_DIR/.aztk-env/.venv/bin/python $AZTK_WORKING_DIR/aztk/node_scripts/scheduling/submit.py "{2}" >> {3} 2>&1'.
            format(task_working_dir, config.pool_id, task.blob_source, constants.SPARK_SUBMIT_LOGS_FILE))
        node_id = select_scheduling_target_node(config.spark_client.cluster, config.pool_id, scheduling_target)
        node_run_output = config.spark_client.cluster.node_run(config.pool_id, node_id, task_cmd, timeout=120)


if __name__ == "__main__":
    scheduling_target = sys.argv.get(1)
    if scheduling_target:
        task_sas_urls = [task_sas_url for task_sas_url in sys.argv[2:]]
        schedule_with_target(scheduling_target, task_sas_urls)
    else:
        tasks = read_downloaded_tasks()
        schedule_tasks(tasks)
