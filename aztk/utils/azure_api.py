import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.storage.blob as blob
import re
from aztk import error
from aztk.version import __version__
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage import CloudStorageAccount
from typing import Optional


RESOURCE_ID_PATTERN = re.compile('^/subscriptions/(?P<subscription>[^/]+)'
                                 '/resourceGroups/(?P<resourcegroup>[^/]+)'
                                 '/providers/[^/]+'
                                 '/[^/]+Accounts/(?P<account>[^/]+)$')

class BatchConfig:
    def __init__(self, service_url: Optional[str]=None, account_key: Optional[str]=None, account_name: Optional[str]=None,
                 tenant_id: Optional[str]=None, client_id: Optional[str]=None, credential: Optional[str]=None,
                 resource_id: Optional[str]=None):
        self.service_url = service_url
        self.account_key = account_key
        self.account_name = account_name

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.credential = credential
        self.resource_id = resource_id


class BlobConfig:
    def __init__(self, account_key: Optional[str]=None, account_name: Optional[str]=None, account_suffix: Optional[str]=None,
                 tenant_id: Optional[str]=None, client_id: Optional[str]=None, credential: Optional[str]=None,
                 resource_id: Optional[str]=None):
        self.account_key = account_key
        self.account_name = account_name
        self.account_suffix = account_suffix

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.credential = credential
        self.resource_id = resource_id


def _validate_batch_config(batch_config: BatchConfig):
    if batch_config.resource_id is None:
        if batch_config.account_name is None or batch_config.account_key is None or batch_config.service_url is None:
            raise error.AzureApiInitError("Neither servicePrincipal nor sharedKey configured for Batch in secrets.yaml config")
    else:
        if not RESOURCE_ID_PATTERN.match(batch_config.resource_id):
            raise error.AzureApiInitError("servicePrincipal.batchaccountresourceid is not in expected format")
        if batch_config.tenant_id is None:
            raise error.AzureApiInitError("Missing tenant_id in servicePrincipal auth for Batch in secrets.yaml config")
        if batch_config.client_id is None:
            raise error.AzureApiInitError("Missing client_id in servicePrincipal auth for Batch in secrets.yaml config")
        if batch_config.credential is None:
            raise error.AzureApiInitError("Missing credential in servicePrincipal auth for Batch in secrets.yaml config")


def make_batch_client(batch_config: BatchConfig):
    """
        Creates a batch client object
        :param str batch_account_key: batch account key
        :param str batch_account_name: batch account name
        :param str batch_service_url: batch service url
    """
    # Validate the given config
    _validate_batch_config(batch_config)

    if batch_config.resource_id is None:
        # Set up SharedKeyCredentials
        base_url = batch_config.service_url
        credentials = batch_auth.SharedKeyCredentials(
            batch_config.account_name,
            batch_config.account_key)
    else:
        # Set up ServicePrincipalCredentials
        credentials = ServicePrincipalCredentials(
            client_id=batch_config.client_id,
            secret=batch_config.credential,
            tenant=batch_config.tenant_id,
            resource='https://management.core.windows.net/')
        m = RESOURCE_ID_PATTERN.match(batch_config.resource_id)
        batch_client = BatchManagementClient(credentials, m.group('subscription'))
        account = batch_client.batch_account.get(m.group('resourcegroup'), m.group('account'))
        base_url = 'https://%s/' % account.account_endpoint
        credentials = ServicePrincipalCredentials(
            client_id=batch_config.client_id,
            secret=batch_config.credential,
            tenant=batch_config.tenant_id,
            resource='https://batch.core.windows.net/')

    # Set up Batch Client
    batch_client = batch.BatchServiceClient(
        credentials,
        base_url=base_url)

    # Set retry policy
    batch_client.config.retry_policy.retries = 5
    batch_client.config.add_user_agent('aztk/{}'.format(__version__))

    return batch_client


def _validate_blob_config(blob_config: BlobConfig):
    if blob_config.resource_id is None:
        if blob_config.account_name is None or blob_config.account_key is None or blob_config.account_suffix is None:
            raise error.AzureApiInitError("Neither servicePrincipal nor sharedKey configured for Storage in secrets.yaml config")
    else:
        if not RESOURCE_ID_PATTERN.match(blob_config.resource_id):
            raise error.AzureApiInitError("servicePrincipal.storageaccountresourceid is not in expected format")
        if blob_config.tenant_id is None:
            raise error.AzureApiInitError("Missing tenant_id in servicePrincipal auth for Storage in secrets.yaml config")
        if blob_config.client_id is None:
            raise error.AzureApiInitError("Missing client_id in servicePrincipal auth for Storage in secrets.yaml config")
        if blob_config.credential is None:
            raise error.AzureApiInitError("Missing credential in servicePrincipal auth for Storage in secrets.yaml config")


def make_blob_client(blob_config: BlobConfig):
    """
        Creates a blob client object
        :param str storage_account_key: storage account key
        :param str storage_account_name: storage account name
        :param str storage_account_suffix: storage account suffix
    """
    # Validate Blob config
    _validate_blob_config(blob_config)

    if blob_config.resource_id is None:
        # Set up SharedKeyCredentials
        blob_client = blob.BlockBlobService(
            account_name=blob_config.account_name,
            account_key=blob_config.account_key,
            endpoint_suffix=blob_config.account_suffix)
    else:
        # Set up ServicePrincipalCredentials
        credentials = ServicePrincipalCredentials(
            client_id=blob_config.client_id,
            secret=blob_config.credential,
            tenant=blob_config.tenant_id,
            resource='https://management.core.windows.net/')
        m = RESOURCE_ID_PATTERN.match(blob_config.resource_id)
        accountname = m.group('account')
        subscription = m.group('subscription')
        resourcegroup = m.group('resourcegroup')
        mgmt_client = StorageManagementClient(credentials, subscription)
        key = mgmt_client.storage_accounts.list_keys(resource_group_name=resourcegroup, account_name=accountname).keys[0].value
        storage_client = CloudStorageAccount(accountname, key)
        blob_client = storage_client.create_block_blob_service()

    return blob_client
