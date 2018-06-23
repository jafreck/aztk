from aztk.spark.client.cluster.client import Client as cluster_client

from .helpers import create
from aztk.spark import models

class Client(cluster_client):

    def create(self, cluster_configuration: models.ClusterConfiguration, wait: bool = False):
        return create.create_cluster(self, cluster_configuration, wait)
