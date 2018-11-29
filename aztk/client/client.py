from aztk import models
from aztk.utils import azure_api


class CoreClient:
    """The base AZTK client that all other clients inherit from.

    **This client should not be used directly. Only software specific clients
    should be used.**

    """

    def __init__(self):
        self.secrets_configuration = None
        self.batch_client = None
        self.cloud_storage_account = None
        self.table_service = None

    def _get_context(self, secrets_configuration: models.SecretsConfiguration):
        self.secrets_configuration = secrets_configuration

        azure_api.validate_secrets(secrets_configuration)
        self.batch_client = azure_api.make_batch_client(secrets_configuration)
        self.cloud_storage_account = azure_api.make_cloud_storage_account(secrets_configuration)
        self.block_blob_service = self.cloud_storage_account.create_block_blob_service()
        self.table_service = azure_api.make_table_service(secrets_configuration)
        context = {
            "batch_client": self.batch_client,
            "cloud_storage_account": self.cloud_storage_account,
            "block_blob_service": self.block_blob_service,
            "table_service": self.table_service,
            "secrets_configuration": self.secrets_configuration,
        }
        return context
