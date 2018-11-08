import azure.batch.models as batchmodels

from aztk.node_scripts.core import config


def get_pool() -> batchmodels.CloudPool:
    return config.batch_client.pool.get(config.pool_id)


def get_node(node_id: str) -> batchmodels.ComputeNode:
    return config.batch_client.compute_node.get(config.pool_id, node_id)


def get_master_node_id(cluster_id) -> str:
    cluster = config.spark_client.cluster.get(cluster_id)
    if cluster.master_node_id:
        return cluster.master_node_id
