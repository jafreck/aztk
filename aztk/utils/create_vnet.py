import azure.mgmt.network.models as network_models
from azure.mgmt.batch import BatchManagementClient
from azure.common.credentials import ServicePrincipalCredentials

import aztk.utils.azure_api as azure_api
from aztk.utils import constants


def create_vnet(tenant_id, client_id, credential, resource_id, vnet_id="aztk-default", subnet_id="aztk-default"):
    network_client = azure_api.make_network_client(
        azure_api.NetworkConfig(
            tenant_id=tenant_id,
            client_id=client_id,
            credential=credential,
            resource_id=resource_id
        )
    )

    match = constants.RESOURCE_ID_PATTERN.match(resource_id)

    credentials = ServicePrincipalCredentials(
            client_id=client_id,
            secret=credential,
            tenant=tenant_id,
            resource='https://management.core.windows.net/')
    batch_mgmt_client = BatchManagementClient(credentials, match.group('subscription'))
    location = batch_mgmt_client.batch_account.get(match.group("resourcegroup"), match.group('account')).location

    network_client.virtual_networks.create_or_update(
        resource_group_name=match.group("resourcegroup"),
        virtual_network_name=vnet_id,
        parameters=network_models.VirtualNetwork(
            location=location,
            address_space=network_models.AddressSpace(["10.0.0.0/24"])
        )
    )
    
    network_client.subnets.create_or_update(
        resource_group_name=match.group("resourcegroup"),
        virtual_network_name=vnet_id,
        subnet_name=subnet_id,
        subnet_parameters=network_models.Subnet(address_prefix='10.0.0.0/24'))
    
    return network_client.subnets.get(resource_group_name=match.group("resourcegroup"),
                                      virtual_network_name=vnet_id,
                                      subnet_name=subnet_id).id
