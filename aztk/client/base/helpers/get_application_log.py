import time

import azure
import azure.batch.models as batch_models
from azure.batch.models import BatchErrorException

from aztk import error, models
from aztk.models import Task, TaskState
from aztk.utils import batch_error_manager, constants, helpers

output_file = constants.TASK_WORKING_DIR + "/" + constants.SPARK_SUBMIT_LOGS_FILE


def __check_task_node_exist(batch_client, cluster_id: str, task: Task) -> bool:
    try:
        batch_client.compute_node.get(cluster_id, task.node_id)
        return True
    except BatchErrorException:
        return False


def wait_for_task(base_operations, cluster_id, application_name):
    # TODO: ensure get_task_state not None or throw
    task = base_operations.get_task(cluster_id, application_name)
    while task.state not in [TaskState.Completed, TaskState.Failed]:
        time.sleep(3)
        # TODO: enable logger
        # log.debug("{} {}: application not yet complete".format(cluster_id, application_name))
        task = base_operations.get_task(cluster_id, application_name)
    return task


def __get_output_file_properties(batch_client, cluster_id: str, application_name: str):
    while True:
        try:
            file = helpers.get_file_properties(cluster_id, application_name, output_file, batch_client)
            return file
        except BatchErrorException as e:
            if e.response.status_code == 404:
                # TODO: log
                time.sleep(5)
                continue
            else:
                raise e


def get_log_from_storage(block_blob_service, container_name, application_name, task):
    """
        Args:
            block_blob_service (:obj:`azure.storage.blob.BlockBlobService`):  Client used to interact with the Azure Storage
                Blob service.
            container_name (:obj:`str`): the name of the Azure Blob storage container to get data from
            application_name (:obj:`str`): the name of the application to get logs for
            task (:obj:`aztk.models.Task`): the aztk task for for this application
    """
    try:
        blob = block_blob_service.get_blob_to_text(container_name,
                                                   application_name + "/" + constants.SPARK_SUBMIT_LOGS_FILE)
    except azure.common.AzureMissingResourceHttpError:
        raise error.AztkError("Logs not found in your storage account. They were either deleted or never existed.")

    return models.ApplicationLog(
        name=application_name,
        cluster_id=container_name,
        application_state=task.state,
        log=blob.content,
        total_bytes=blob.properties.content_length,
        exit_code=task.exit_code,
    )


def wait_for_scheduling_target_task(base_operations, cluster_id, application_name):
    application_state = base_operations.get_task_state(cluster_id, application_name)
    while TaskState(application_state) not in [TaskState.Completed, TaskState.Failed]:
        time.sleep(3)
        print("Application {}: State {}".format(TaskState(application_state), application_name))
        # TODO: enable logger
        # log.debug("{} {}: application not yet complete".format(cluster_id, application_name))
        application_state = base_operations.get_task_state(cluster_id, application_name)
    return base_operations.get_task(cluster_id, application_name)


def get_log(base_operations, cluster_id: str, application_name: str, tail=False, current_bytes: int = 0):
    job_id = cluster_id
    task_id = application_name
    cluster_configuration = base_operations.get_cluster_configuration(cluster_id)

    task = wait_for_task(base_operations, cluster_id, application_name)
    if cluster_configuration.scheduling_target is not models.SchedulingTarget.Any:
        return get_log_from_storage(base_operations.block_blob_service, cluster_id, application_name, task)
    else:
        if not __check_task_node_exist(base_operations.batch_client, cluster_id, task):
            return get_log_from_storage(base_operations.block_blob_service, cluster_id, application_name, task)

    file = __get_output_file_properties(base_operations.batch_client, cluster_id, application_name)
    target_bytes = file.content_length

    if target_bytes != current_bytes:
        ocp_range = None

        if tail:
            ocp_range = "bytes={0}-{1}".format(current_bytes, target_bytes - 1)

        stream = base_operations.batch_client.file.get_from_task(
            job_id, task_id, output_file, batch_models.FileGetFromTaskOptions(ocp_range=ocp_range))
        content = helpers.read_stream_as_string(stream)

        return models.ApplicationLog(
            name=application_name,
            cluster_id=cluster_id,
            application_state=task.state,
            log=content,
            total_bytes=target_bytes,
            exit_code=task.exit_code,
        )
    else:
        return models.ApplicationLog(
            name=application_name,
            cluster_id=cluster_id,
            application_state=task.state,
            log="",
            total_bytes=target_bytes,
            exit_code=task.exit_code,
        )


def get_application_log(base_operations, cluster_id: str, application_name: str, tail=False, current_bytes: int = 0):
    with batch_error_manager():
        return get_log(base_operations, cluster_id, application_name, tail, current_bytes)
