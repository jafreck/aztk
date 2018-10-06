import logging
import os
import subprocess
import sys
import time
import uuid
from typing import List

import requests
import yaml
# pylint: disable=import-error,no-name-in-module
from azure.cosmosdb.table.models import Entity

import common
from aztk import error
from aztk.models import TaskState
from aztk.node_scripts.core import config
from aztk.node_scripts.scheduling import scheduling_target
from aztk.utils import helpers
from aztk.utils.command_builder import CommandBuilder

# limit azure.storage logging
logging.getLogger("azure.storage").setLevel(logging.CRITICAL)
"""
Submit helper methods
"""


def __app_submit_cmd(application):
    spark_home = os.environ["SPARK_HOME"]
    with open(os.path.join(spark_home, "conf", "master")) as f:
        master_ip = f.read().rstrip()

    # set file paths to correct path on container
    files_path = os.environ["AZ_BATCH_TASK_WORKING_DIR"]
    jars = [os.path.join(files_path, os.path.basename(jar)) for jar in application.jars]
    py_files = [os.path.join(files_path, os.path.basename(py_file)) for py_file in application.py_files]
    files = [os.path.join(files_path, os.path.basename(f)) for f in application.files]

    # 2>&1 redirect stdout and stderr to be in the same file
    spark_submit_cmd = CommandBuilder("{0}/bin/spark-submit".format(spark_home))
    spark_submit_cmd.add_option("--master", "spark://{0}:7077".format(master_ip))
    spark_submit_cmd.add_option("--name", application.name)
    spark_submit_cmd.add_option("--class", application.main_class)
    spark_submit_cmd.add_option("--jars", jars and ",".join(jars))
    spark_submit_cmd.add_option("--py-files", py_files and ",".join(py_files))
    spark_submit_cmd.add_option("--files", files and ",".join(files))
    spark_submit_cmd.add_option("--driver-java-options", application.driver_java_options)
    spark_submit_cmd.add_option("--driver-library-path", application.driver_library_path)
    spark_submit_cmd.add_option("--driver-class-path", application.driver_class_path)
    spark_submit_cmd.add_option("--driver-memory", application.driver_memory)
    spark_submit_cmd.add_option("--executor-memory", application.executor_memory)
    if application.driver_cores:
        spark_submit_cmd.add_option("--driver-cores", str(application.driver_cores))
    if application.executor_cores:
        spark_submit_cmd.add_option("--executor-cores", str(application.executor_cores))

    spark_submit_cmd.add_argument(
        os.path.expandvars(application.application) + " " +
        " ".join(["'" + str(app_arg) + "'" for app_arg in (application.application_args or [])]))

    with open("spark-submit.txt", mode="w", encoding="UTF-8") as stream:
        stream.write(spark_submit_cmd.to_str())

    return spark_submit_cmd


def receive_submit_request(application_file_path):
    """
        Handle the request to submit a task
    """
    blob_client = config.blob_client
    application = common.load_application(application_file_path)

    cmd = __app_submit_cmd(application)

    return_code = subprocess.call(cmd.to_str(), shell=True)
    common.upload_log(blob_client, application)
    return return_code


def ssh_submit(task_sas_url):
    task = common.download_task_definition(task_sas_url)
    scheduling_target.download_task_resource_files(task.id, task.resource_files)

    application = common.load_application(os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], "application.yaml"))

    cmd = __app_submit_cmd(application)

    # update task table before running
    insert_task_into_task_table(config.pool_id, task)
    exit_code = -1
    try:
        exit_code = subprocess.call(cmd.to_str(), shell=True)
        common.upload_log(config.blob_client, application)
        #TODO: separate logging and applicaiton output
        print("completed application, updating storage table")
        mark_task_complete(config.pool_id, task.id, exit_code)
    except Exception as e:
        #TODO: separate logging and applicaiton output
        print("application failed, updating storage table")
        mark_task_failure(config.pool_id, task.id, exit_code, str(e))

    return exit_code


def insert_task_into_task_table(cluster_id, task):
    entity = Entity()
    entity.PartitionKey = config.pool_id
    entity.RowKey = task.id
    entity.node_id = os.environ.get("AZ_BATCH_NODE_ID", None)
    entity.state = TaskState.Running.value
    entity.command_line = task.command_line
    entity.start_time = time.time()
    entity.end_time = None
    entity.exit_code = None
    entity.failure_info = None

    config.spark_client.cluster._core_cluster_operations.insert_task_into_task_table(cluster_id, entity)


def get_table_entity(cluster_id, task_id):
    return config.spark_client.cluster._core_cluster_operations.get_task_from_table(cluster_id, task_id)


def mark_task_complete(cluster_id, task_id, exit_code):
    entity = get_table_entity(cluster_id, task_id)
    entity.end_time = time.time()
    entity.exit_code = exit_code
    entity.state = TaskState.Completed.value
    config.spark_client.table_service.insert_or_replace_entity(helpers.convert_id_to_table_id(cluster_id), entity)
    entity = get_table_entity(cluster_id, task_id)
    print(entity)


def mark_task_failure(cluster_id, task_id, exit_code, failure_info):
    entity = get_table_entity(cluster_id, task_id)
    entity.end_time = time.time()
    entity.exit_code = exit_code
    entity.state = TaskState.Completed.value
    entity.failure_info = failure_info
    config.spark_client.table_service.insert_or_replace_entity(helpers.convert_id_to_table_id(cluster_id), entity)
    entity = get_table_entity(cluster_id, task_id)
    print(entity)


if __name__ == "__main__":
    return_code = 1

    if len(sys.argv) == 2:
        serialized_task_sas_url = sys.argv[1]

        try:
            return_code = ssh_submit(serialized_task_sas_url)
        except Exception as e:
            import traceback
            print()
            common.upload_error_log(traceback.format_exc() + str(e),
                                    os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], "application.yaml"))
    else:
        try:
            return_code = receive_submit_request(
                os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], "application.yaml"))
        except Exception as e:
            common.upload_error_log(str(e), os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], "application.yaml"))

        # force batch task exit code to match spark exit code
        # TODO: make this valuable to ssh_submit
        sys.exit(return_code)
