import datetime
import os
import shlex
import subprocess
import sys

import azure.batch.models as batch_models
import azure.storage.blob as blob
import requests
import yaml
from azure.storage.common import CloudStorageAccount

from aztk.node_scripts.core import config
from aztk.node_scripts.scheduling import scheduling_target


def load_application(application_file_path):
    """
        Read and parse the application from file
    """
    with open(application_file_path, encoding="UTF-8") as f:
        application = yaml.load(f)
    return application


def upload_log(blob_client, application):
    """
        upload output.log to storage account
    """
    log_file = os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], os.environ["SPARK_SUBMIT_LOGS_FILE"])
    upload_file_to_container(
        container_name=os.environ["STORAGE_LOGS_CONTAINER"],
        application_name=application.name,
        file_path=log_file,
        blob_client=blob_client,
        use_full_path=False,
    )


def upload_file_to_container(container_name,
                             application_name,
                             file_path,
                             blob_client=None,
                             use_full_path=False,
                             node_path=None) -> batch_models.ResourceFile:
    """
    Uploads a local file to an Azure Blob storage container.
    :param blob_client: A blob service client.
    :type blob_client: `azure.storage.common.CloudStorageAccount`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :param str node_path: Path on the local node. By default will be the same as file_path
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    block_blob_client = blob_client.create_block_blob_service()
    file_path = file_path
    blob_name = None
    if use_full_path:
        blob_name = file_path.strip("/")
    else:
        blob_name = os.path.basename(file_path)
        blob_path = application_name + "/" + blob_name

    if not node_path:
        node_path = blob_name

    block_blob_client.create_container(container_name, fail_on_exist=False)

    block_blob_client.create_blob_from_path(container_name, blob_path, file_path)

    sas_token = block_blob_client.generate_blob_shared_access_signature(
        container_name,
        blob_path,
        permission=blob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(days=7),
    )

    sas_url = block_blob_client.make_blob_url(container_name, blob_path, sas_token=sas_token)

    return batch_models.ResourceFile(file_path=node_path, blob_source=sas_url)


def upload_error_log(error, application_file_path):
    application = load_application(application_file_path)
    blob_client = config.blob_client

    error_log_path = os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], "error.log")
    with open(error_log_path, "w", encoding="UTF-8") as error_log:
        error_log.write(error)

    upload_file_to_container(
        container_name=os.environ["STORAGE_LOGS_CONTAINER"],
        application_name=application.name,
        file_path=os.path.realpath(error_log.name),
        blob_client=blob_client,
        use_full_path=False,
    )
    upload_log(blob_client, application)


def download_task_definition(task_sas_url):
    response = scheduling_target.http_request_wrapper(requests.get, task_sas_url, timeout=10)
    yaml_serialized_task = response.content
    return yaml.load(yaml_serialized_task)


def stream_upload_to_storage(
        blob_client: CloudStorageAccount,
        stream,
        application_name,
):
    """
    Args:
        blob_client (`azure.storage.common.CloudStorageAccount`)
        stream (`obj:IOBase`): opened stream to upload as the blob content
    """
    from azure.storage.blob.appendblobservice import AppendBlobService
    AppendBlobService.MAX_BLOCK_SIZE = 10 * 1024

    append_blob_client = blob_client.create_append_blob_service()

    append_blob_client.create_blob(
        container_name=os.environ["STORAGE_LOGS_CONTAINER"],
        blob_name=application_name + "/output.log",
        if_none_match="*",
    )
    append_blob_client.append_blob_from_stream(
        container_name=os.environ["STORAGE_LOGS_CONTAINER"],
        blob_name=application_name + "/output.log",
        stream=stream,
    )


def run_command(spark_client, command, application_name):
    print("running common.py run_command")
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stream_upload_to_storage(spark_client.blob_client, process.stdout, application_name)
    rc = process.poll()
    return rc
