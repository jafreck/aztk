import tempfile
import time

import azure
import azure.batch.models as batch_models

from aztk import error, models
from aztk.models import Task, TaskState
from aztk.utils import batch_error_manager, constants


def convert_application_name_to_blob_path(application_name):
    return application_name + "/" + constants.SPARK_SUBMIT_LOGS_FILE


def wait_for_batch_task(base_operations, cluster_id: str, application_name: str) -> Task:
    """
        Wait for the batch task to leave the waiting state into running(or completed if it was fast enough)
    """

    while True:
        task_state = base_operations.get_task_state(cluster_id, application_name)

        if task_state in [batch_models.TaskState.active, batch_models.TaskState.preparing]:
            # TODO: log
            time.sleep(5)
        else:
            return base_operations.get_batch_task(id=cluster_id, task_id=application_name)


def wait_for_scheduling_target_task(base_operations, cluster_id, application_name):
    # TODO: ensure get_task_state not None or throw
    task = base_operations.get_task(cluster_id, application_name)
    while task.state not in [TaskState.Completed, TaskState.Failed, TaskState.Running]:
        time.sleep(3)
        # TODO: enable logger
        # log.debug("{} {}: application not yet complete".format(cluster_id, application_name))
        task = base_operations.get_task(cluster_id, application_name)
    return task


def wait_for_task(base_operations, cluster_id: str, application_name: str, cluster_configuration):
    if cluster_configuration.scheduling_target is not models.SchedulingTarget.Any:
        task = wait_for_scheduling_target_task(base_operations, cluster_id, application_name)
    else:
        task = wait_for_batch_task(base_operations, cluster_id, application_name)
    return task


def get_blob_from_storage(block_blob_client, container_name, application_name, stream, start_range, end_range=None):
    print(block_blob_client, container_name, application_name, stream, start_range, end_range)
    previous = 0

    def download_callback(current, total):
        nonlocal previous
        stream.seek(previous)
        print("({}/{})".format(previous, current))
        # print(stream.read().decode('utf-8'))    # SDK SHOULDN'T PRINT
        previous = current

    try:
        blob = block_blob_client.get_blob_to_stream(
            container_name,
            convert_application_name_to_blob_path(application_name),
            stream,
            progress_callback=download_callback,
            start_range=start_range,
            end_range=end_range)
        stream.seek(0)
        return blob
    except azure.common.AzureMissingResourceHttpError:
        raise
        raise error.AztkError("Logs not found in your storage account. They were either deleted or never existed.")
    except azure.common.AzureHttpError as e:
        if e.error_code in ["InvalidRange"]:
            # the blob has no data, should not throw here
            raise error.AztkError("The application {} log has no data yet.".format(application_name))
        raise


def get_log_from_storage(blob_client, container_name, application_name, task, current_bytes):
    stream = tempfile.TemporaryFile()
    blob = get_blob_from_storage(blob_client.create_block_blob_service(), container_name, application_name, stream,
                                 current_bytes)
    return models.ApplicationLog(
        name=application_name,
        cluster_id=container_name,
        application_state=task.state,
        log=stream,
        total_bytes=blob.properties.content_length,
        exit_code=task.exit_code,
    )


def stream_log_from_storage(base_operations, container_name, application_name, task):
    """
        Args:
            base_operations (:obj:`aztk.client.base.BaseOperations`):  Base aztk client
            container_name (:obj:`str`): the name of the Azure Blob storage container to get data from
            application_name (:obj:`str`): the name of the application to get logs for
            task (:obj:`aztk.models.Task`): the aztk task for for this application
    """
    stream = tempfile.TemporaryFile()
    last_read_byte = 0

    block_blob_client = base_operations.blob_client.create_block_blob_service()
    blob = get_blob_from_storage(
        block_blob_client,
        container_name,
        application_name,
        stream,
        start_range=last_read_byte,
        end_range=last_read_byte + constants.STREAMING_DOWNLOAD_CHUNK_SIZE,
    )

    while task.state not in [TaskState.Completed, TaskState.Failed]:
        print(container_name, task.id)
        task = base_operations.get_task(container_name, task.id)
        last_read_byte = blob.properties.content_length
        blob = get_blob_from_storage(
            block_blob_client,
            container_name,
            application_name,
            stream,
            start_range=last_read_byte,
        )

    stream.seek(0)

    return models.ApplicationLog(
        name=application_name,
        cluster_id=container_name,
        application_state=task.state,
        log=stream,
        total_bytes=blob.properties.content_length,
        exit_code=task.exit_code,
    )


def get_log(base_operations, cluster_id: str, application_name: str, tail=False, current_bytes: int = 0):
    cluster_configuration = base_operations.get_cluster_configuration(cluster_id)
    task = wait_for_task(base_operations, cluster_id, application_name, cluster_configuration)

    return get_log_from_storage(base_operations.blob_client, cluster_id, application_name, task, current_bytes)


def stream_log(base_operations, cluster_id: str, application_name: str):
    cluster_configuration = base_operations.get_cluster_configuration(cluster_id)
    task = wait_for_task(base_operations, cluster_id, application_name, cluster_configuration)
    return stream_log_from_storage(base_operations, cluster_id, application_name, task)


def get_application_log(base_operations, cluster_id: str, application_name: str, tail=False, current_bytes: int = 0):
    with batch_error_manager():
        # return get_log(base_operations, cluster_id, application_name, tail, current_bytes)
        return stream_log(base_operations, cluster_id, application_name)
