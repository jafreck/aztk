"""
    This is the code that all nodes will run in their start task to try to allocate the master
"""
import azure.batch.batch_service_client as batch
import azure.batch.models as batchmodels
from azure.batch.models import BatchErrorException
from msrest.exceptions import ClientRequestError

import aztk.models
from aztk.node_scripts.core import config, log

MASTER_NODE_METADATA_KEY = "_spark_master_node"


class CannotAllocateMasterError(Exception):
    pass


def try_assign_self_as_master(client: batch.BatchServiceClient, cluster: aztk.models.Cluster):
    current_metadata = cluster.pool.metadata or []
    new_metadata = current_metadata + [{"name": MASTER_NODE_METADATA_KEY, "value": config.node_id}]

    try:
        client.pool.patch(
            config.pool_id,
            batchmodels.PoolPatchParameter(metadata=new_metadata),
            batchmodels.PoolPatchOptions(if_match=cluster.pool.e_tag),
        )
        return True
    except (BatchErrorException, ClientRequestError):
        log.info("Couldn't assign itself as master the pool because the pool was modified since last get.")
        return False


def find_master(client: batch.BatchServiceClient) -> bool:
    """
        Try to set a master for the cluster. If the node is dedicated it will try to assign itself if none already claimed it.
        :returns bool: If the node is the master it returns true otherwise returns false
    """
    # If not dedicated the node cannot be a master
    # TODO enable when inter node communication is working with low pri and dedicated together.
    # if not config.is_dedicated:
    # return False

    for i in range(0, 5):
        cluster = config.spark_client.cluster.get(config.cluster_id)

        if cluster.master_node_id:
            if cluster.master_node_id == config.node_id:
                log.info("Node is already the master '{0}'".format(cluster.master_node_id))
                return True
            else:
                log.info("Pool already has a master '{0}'. This node will be a worker".format(cluster.master_node_id))
                return False
        else:
            log.info("Pool has no master. Trying to assign itself! ({0}/5)".format(i + 1))
            result = try_assign_self_as_master(client, cluster)

            if result:
                log.info("Assignment was successful! Node {0} is the new master.".format(config.node_id))
                return True

    raise CannotAllocateMasterError("Unable to assign node as a master in 5 tries")
