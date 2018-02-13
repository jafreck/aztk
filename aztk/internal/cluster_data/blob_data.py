import azure.batch.models as batch_models
import datetime
from azure.storage.blob import BlockBlobService, BlobPermissions

class BlobData:
    """

    """
    def __init__(self, blob_client: BlockBlobService, container: str, blob: str):
        self.container = container
        self.blob = blob
        self.blob_client = blob_client


    def as_resource_file(self, dest: str = None) -> batch_models.ResourceFile:
        sas_token = self.blob_client.generate_blob_shared_access_signature(
            self.container,
            self.blob,
            permission=BlobPermissions.READ,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(days=365))

        sas_url = self.blob_client.make_blob_url(
            self.container, self.blob, sas_token=sas_token)

        return batch_models.ResourceFile(file_path=dest or self.blob, blob_source=sas_url)
