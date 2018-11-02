import logging
import os
import subprocess
import sys

from aztk.node_scripts.core import config, log
from aztk.node_scripts.scheduling import common, scheduling_target
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

    log.info("Spark submit cmd: %s", spark_submit_cmd.to_str())
    return spark_submit_cmd


def receive_submit_request(application_file_path):
    """
        Handle the request to submit a task
    """
    blob_client = config.blob_client
    application = common.load_application(application_file_path)

    cmd = __app_submit_cmd(application)
    exit_code = -1
    try:
        exit_code = common.run_command(config.spark_client, cmd.to_str(), application.name)
    except Exception as e:
        common.upload_error_log(str(e), os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], "application.yaml"))
    return exit_code


def ssh_submit(task_sas_url):
    task_definition = common.download_task_definition(task_sas_url)
    scheduling_target.download_task_resource_files(task_definition.id, task_definition.resource_files)

    application = common.load_application(os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], "application.yaml"))

    cmd = __app_submit_cmd(application)

    exit_code = -1
    aztk_cluster_id = os.environ.get("AZTK_CLUSTER_ID")
    try:
        # update task table before running
        task = scheduling_target.insert_task_into_task_table(aztk_cluster_id, task_definition)
        # run task and upload log
        exit_code = common.run_command(config.spark_client, cmd.to_str(), application.name)
        log("completed application, updating storage table")
        scheduling_target.mark_task_complete(aztk_cluster_id, task.id, exit_code)
    except Exception as e:
        log("application failed, updating storage table")
        import traceback
        scheduling_target.mark_task_failure(aztk_cluster_id, task_definition.id, exit_code, traceback.format_exc())

    return exit_code


if __name__ == "__main__":
    exit_code = 1

    if len(sys.argv) == 2:
        serialized_task_sas_url = sys.argv[1]

        try:
            exit_code = ssh_submit(serialized_task_sas_url)
        except Exception as e:
            import traceback
            common.upload_error_log(traceback.format_exc() + str(e),
                                    os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], "application.yaml"))
    else:
        exit_code = receive_submit_request(os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], "application.yaml"))
        log.info("Exit code: %s", str(exit_code))
        sys.exit(exit_code)
