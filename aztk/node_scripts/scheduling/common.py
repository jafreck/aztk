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


def upload_log(block_blob_service, application):
    """
        upload output.log to storage account
    """
    log_file = os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], os.environ["SPARK_SUBMIT_LOGS_FILE"])
    upload_file_to_container(
        container_name=os.environ["STORAGE_LOGS_CONTAINER"],
        application_name=application.name,
        file_path=log_file,
        block_blob_service=block_blob_service,
        use_full_path=False,
    )


def upload_file_to_container(container_name,
                             application_name,
                             file_path,
                             block_blob_service=None,
                             use_full_path=False,
                             node_path=None) -> batch_models.ResourceFile:
    """
    Uploads a local file to an Azure Blob storage container.
    :param block_blob_service: A blob service client.
    :type block_blob_service: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :param str node_path: Path on the local node. By default will be the same as file_path
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    file_path = file_path
    blob_name = None
    if use_full_path:
        blob_name = file_path.strip("/")
    else:
        blob_name = os.path.basename(file_path)
        blob_path = application_name + "/" + blob_name

    if not node_path:
        node_path = blob_name

    block_blob_service.create_container(container_name, fail_on_exist=False)

    block_blob_service.create_blob_from_path(container_name, blob_path, file_path)

    sas_token = block_blob_service.generate_blob_shared_access_signature(
        container_name,
        blob_path,
        permission=blob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(days=7),
    )

    sas_url = block_blob_service.make_blob_url(container_name, blob_path, sas_token=sas_token)

    return batch_models.ResourceFile(file_path=node_path, blob_source=sas_url)


def upload_error_log(error, application_file_path):
    application = load_application(application_file_path)

    error_log_path = os.path.join(os.environ["AZ_BATCH_TASK_WORKING_DIR"], "error.log")
    with open(error_log_path, "w", encoding="UTF-8") as error_log:
        error_log.write(error)

    upload_file_to_container(
        container_name=os.environ["STORAGE_LOGS_CONTAINER"],
        application_name=application.name,
        file_path=os.path.realpath(error_log.name),
        block_blob_service=config.block_blob_service,
        use_full_path=False,
    )
    upload_log(config.block_blob_service, application)


def download_task_definition(task_sas_url):
    response = scheduling_target.http_request_wrapper(requests.get, task_sas_url, timeout=10)
    yaml_serialized_task = response.content
    return yaml.load(yaml_serialized_task)


def run_command(command, cluster_id, application_name):
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stream_upload_to_file_share(cluster_id, application_name, process.stdout)
    rc = process.wait()
    return rc


def create_file_share(cluster_id, application_id, quota=5120, fail_on_exist=True):
    config.file_service.create_share(share_name=cluster_id + application_id, quota=quota, fail_on_exist=fail_on_exist)


def stream_upload_to_file_share(cluster_id, application_name, stream):
    create_file_share(cluster_id, application_name)
    config.file_service.create_file_from_stream(
        share_name=cluster_id + application_name,
        directory_name="",
        file_name="output.log",
        stream=stream,
        max_connections=4,
    )
