from typing import List

import azure.batch.models as batch_models

from aztk.client.base import BaseOperations as CoreBaseOperations
from aztk.spark import models

from .helpers import generate_cluster_start_task, generate_application_task, get_application_log


class SparkBaseOperations(CoreBaseOperations):
    #TODO: make this private or otherwise not public
    def generate_cluster_start_task(self,
                                    zip_resource_file: batch_models.ResourceFile,
                                    id: str,
                                    gpu_enabled: bool,
                                    docker_repo: str = None,
                                    file_shares: List[models.FileShare] = None,
                                    plugins: List[models.PluginConfiguration] = None,
                                    mixed_mode: bool = False,
                                    worker_on_master: bool = True):
        """Generate the Azure Batch Start Task to provision a Spark cluster.

        Args:
            zip_resource_file (:obj:`azure.batch.models.ResourceFile`): a single zip file of all necessary data
                to upload to the cluster.
            id (:obj:`str`): the id of the cluster.
            gpu_enabled (:obj:`bool`): if True, the cluster is GPU enabled.
            docker_repo (:obj:`str`, optional): the docker repository and tag that identifies the docker image to use.
                If None, the default Docker image will be used. Defaults to None.
            file_shares (:obj:`aztk.spark.models.FileShare`, optional): a list of FileShares to mount on the cluster.
                Defaults to None.
            plugins (:obj:`aztk.spark.models.PluginConfiguration`, optional): a list of plugins to set up on the cluster.
                Defaults to None.
            mixed_mode (:obj:`bool`, optional): If True, the cluster is configured to use both dedicated and low priority VMs.
                Defaults to False.
            worker_on_master (:obj:`bool`, optional): If True, the cluster is configured to provision a Spark worker
                on the VM that runs the Spark master. Defaults to True.

        Returns:
            azure.batch.models.StartTask: the StartTask definition to provision the cluster.
        """
        return generate_cluster_start_task.generate_cluster_start_task(self, zip_resource_file, id, gpu_enabled,
                                                                       docker_repo, file_shares, plugins, mixed_mode,
                                                                       worker_on_master)

    def generate_application_task(self, container_id, application, remote=False):
        """Generate the Azure Batch Start Task to provision a Spark cluster.

        Args:
            container_id (:obj:`str`): the id of the container to run the application in
            application (:obj:`aztk.spark.models.ApplicationConfiguration): the Application Definition
            remote (:obj:`bool`): If True, the application file will not be uploaded, it is assumed to be reachable
                by the cluster already. This is useful when your application is stored in a mounted Azure File Share
                and not the client. Defaults to False.

        Returns:
            azure.batch.models.TaskAddParameter: the Task definition for the Application.
        """
        return generate_application_task.generate_application_task(self, container_id, application, remote)

    def get_application_log(self, id: str, application_name: str, tail=False, current_bytes: int = 0):
        """Get the log for a running or completed application

        Args:
            id (:obj:`str`): the id of the cluster to run the command on.
            application_name (:obj:`str`): str
            tail (:obj:`bool`, optional): If True, get the remaining bytes after current_bytes. Otherwise, the whole log will be retrieved.
                Only use this if streaming the log as it is being written. Defaults to False.
            current_bytes (:obj:`int`): Specifies the last seen byte, so only the bytes after current_bytes are retrieved.
                Only useful is streaming the log as it is being written. Only used if tail is True.

        Returns:
            aztk.spark.models.ApplicationLog: a model representing the output of the application.
        """
        return get_application_log.get_application_log(SparkBaseOperations, self, id, application_name, tail, current_bytes)
