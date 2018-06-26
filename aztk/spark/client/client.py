from aztk.client import CoreClient
from aztk.spark import models
from aztk.spark.client.cluster import ClusterOperations
from aztk.spark.client.job import JobOperations
from aztk.utils import azure_api



class Client(CoreClient):
    def __init__(self, secrets_configuration: models.SecretsConfiguration):
        context = self.get_context(secrets_configuration)
        self.cluster = ClusterOperations(context)
        self.job = JobOperations(context)

    def get_context(self, secrets_configuration: models.SecretsConfiguration):
        self.secrets_configuration = secrets_configuration

        azure_api.validate_secrets(secrets_configuration)
        self.batch_client = azure_api.make_batch_client(secrets_configuration)
        self.blob_client = azure_api.make_blob_client(secrets_configuration)
        context = {
            'batch_client': self.batch_client,
            'blob_client': self.blob_client,
            'secrets_configuration': secrets_configuration,
        }
        return context
