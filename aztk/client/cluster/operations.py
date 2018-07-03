from aztk.client.base import BaseOperations
from aztk.client.base.base_operations import BaseOperations
from aztk.models import ClusterConfiguration

from .helpers import copy, create, delete, get, list


class CoreClusterOperations(BaseOperations):
    def create(self, cluster_configuration: ClusterConfiguration, software_metadata_key: str, start_task,
               vm_image_model):
        return create.create_pool_and_job(self, cluster_configuration, software_metadata_key, start_task, vm_image_model)

    def get(self, cluster_id: str):
        return get.get_pool_details(self, cluster_id)

    def copy(self,
             cluster_id,
             source_path,
             destination_path=None,
             container_name=None,
             internal=False,
             get=False,
             timeout=None):
        return copy.cluster_copy(self, cluster_id, source_path, destination_path, container_name, internal, get,
                                 timeout)

    def delete(self, pool_id: str, keep_logs: bool = False):
        return delete.delete_pool_and_job(self, pool_id, keep_logs)

    def list(self, software_metadata_key):
        return list.list_clusters(self, software_metadata_key)
