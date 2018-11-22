import datetime

import azure.batch.models as batch_models
from azure.storage.blob import BlobPermissions, BlockBlobService


class BlobData:
    """
    Object mapping to a blob entry. Can generate resource files for batch
    """

    def __init__(self, block_blob_service: BlockBlobService, container: str, blob: str):
        self.container = container
        self.blob = blob
        self.dest = blob
        self.block_blob_service = block_blob_service

    def to_resource_file(self, dest: str = None) -> batch_models.ResourceFile:
        sas_token = self.block_blob_service.generate_blob_shared_access_signature(
            self.container,
            self.blob,
            permission=BlobPermissions.READ,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(days=365),
        )

        sas_url = self.block_blob_service.make_blob_url(self.container, self.blob, sas_token=sas_token)

        return batch_models.ResourceFile(file_path=dest or self.dest, blob_source=sas_url)
