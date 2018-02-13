import yaml
import logging
import azure.common
from azure.storage.blob import BlockBlobService
from .node_data import NodeData
from .blob_data import BlobData

class ClusterData:
    """
    Class handling the management of data for a cluster
    """
    # ALl data related to cluster(config, metadata, etc.) should be under this folder
    CLUSTER_DIR = "cluster"

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

    def upload_file(self, blob_path: str, local_path: str) -> BlobData:
        self.blob_client.create_blob_from_path(self.cluster_id, blob_path, local_path)
        return BlobData(self.blob_client, self.cluster_id, blob_path)

    def upload_cluster_file(self, blob_path: str, local_path: str) -> BlobData:
        return self.upload_file(self.CLUSTER_DIR + "/" + blob_path, local_path)

    def upload_node_data(self, node_data: NodeData) -> BlobData:
        return self.upload_cluster_file("node-scripts.zip", node_data.zip_path)

    def _ensure_container(self):
        self.blob_client.create_container(self.cluster_id, fail_on_exist=False)
