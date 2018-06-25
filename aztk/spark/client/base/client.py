from typing import List

import azure.batch.models as batch_models

from aztk.client.base import BaseClient as CoreBaseClient
from aztk.spark import models

from .helpers import generate_cluster_start_task, generate_application_task


class SparkBaseClient(CoreBaseClient):
    def __generate_cluster_start_task(self,
                                      zip_resource_file: batch_models.ResourceFile,
                                      cluster_id: str,
                                      gpu_enabled: bool,
                                      docker_repo: str = None,
                                      file_shares: List[models.FileShare] = None,
                                      plugins: List[models.PluginConfiguration] = None,
                                      mixed_mode: bool = False,
                                      worker_on_master: bool = True):
        return generate_cluster_start_task.generate_cluster_start_task(self, zip_resource_file, cluster_id, gpu_enabled,
                                                                       docker_repo, file_shares, plugins, mixed_mode,
                                                                       worker_on_master)

    def __generate_application_task(self, container_id, application, remote=False):
        return generate_application_task.generate_application_task(self, container_id, application, remote)
