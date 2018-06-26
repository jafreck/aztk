import aztk.models as models
from aztk.client.cluster import CoreClusterOperations
from aztk.client.job import CoreJobOperations
from aztk.utils import azure_api


class CoreClient:
    def __init__(self, secrets_configuration: models.SecretsConfiguration):
        context = self.get_context(secrets_configuration)
        self.cluster = CoreClusterOperations(context)
        self.job = CoreJobOperations(context)

    def get_context(self, secrets_configuration: models.SecretsConfiguration):
        self.secrets_configuration = secrets_configuration

        azure_api.validate_secrets(secrets_configuration)
        self.batch_client = azure_api.make_batch_client(secrets_configuration)
        self.blob_client = azure_api.make_blob_client(secrets_configuration)
        context = {
            'batch_client': self.batch_client,
            'blob_client': self.blob_client,
            'secrets_configuration': self.secrets_configuration,
        }
        return context
