import yaml
import logging
import azure.common
from azure.storage.blob import BlockBlobService


class ClusterData:
    """
    Class handling the management of data for a cluster
    """

    def __init__(self, blob_client: BlockBlobService, cluster_id: str):
        self.blob_client = blob_client
        self.cluster_id = cluster_id
        self._ensure_container()

    def save_cluster_config(self, cluster_config):
        blob_path = "config.yaml"
        content = yaml.dump(cluster_config)
        container_name = cluster_config.cluster_id
        self.blob_client.create_blob_from_text(container_name, blob_path,
                                               content)

    def read_cluster_config(self):
        blob_path = "config.yaml"
        try:
            result = self.blob_client.get_blob_to_text(self.cluster_id,
                                                       blob_path)
            return yaml.load(result.content)
        except azure.common.AzureMissingResourceHttpError:
            logging.warn(
                "Cluster %s doesn't have cluster configuration in storage",
                self.cluster_id)
        except yaml.YAMLError:
            logging.warn(
                "Cluster %s contains invalid cluster configuration in blob",
                self.cluster_id)

    def _ensure_container(self):
        self.blob_client.create_container(self.cluster_id, fail_on_exist=False)
