import aztk.utils.azure_api as azure_api
import azure.mgmt.network.models as network_models
from aztk.utils import constants


def create_vnet(tenant_id, client_id, credential, resource_id, vnet_id="aztk-default", subnet_id="aztk-default"):
    print(tenant_id, client_id, credential, resource_id, vnet_id, subnet_id)
    network_client = azure_api.make_network_client(
        azure_api.NetworkConfig(
            tenant_id=tenant_id,
            client_id=client_id,
            credential=credential,
            resource_id=resource_id
        )
    )

    match = constants.RESOURCE_ID_PATTERN.match(resource_id)

    network_client.virtual_networks.create_or_update(
        resource_group_name=match.group("resourcegroup"), virtual_network_name=vnet_id, parameters=network_models.VirtualNetwork()
    )
    
    network_client.subnets.create_or_update(
        resource_group_name=match.group("resourcegroup"),
        virtual_network_name=vnet_id,
        subnet_name=subnet_id,
        subnet_parameters=network_models.Subnet())
