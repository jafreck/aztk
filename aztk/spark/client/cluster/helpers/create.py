from typing import List

import azure.batch.models as batch_models
import azure.batch.models.batch_error as batch_error

from aztk import error
from aztk.internal.cluster_data import NodeData
from aztk.spark import models
from aztk.spark.utils import util
from aztk.utils import constants, helpers

POOL_ADMIN_USER_IDENTITY = batch_models.UserIdentity(
    auto_user=batch_models.AutoUserSpecification(
        scope=batch_models.AutoUserScope.pool, elevation_level=batch_models.ElevationLevel.admin))

def _default_scheduling_target(vm_count: int):
    if vm_count == 0:
        return models.SchedulingTarget.Any
    else:
        return models.SchedulingTarget.Dedicated


def _apply_default_for_cluster_config(configuration: models.ClusterConfiguration):
    cluster_conf = models.ClusterConfiguration()
    cluster_conf.merge(configuration)
    if cluster_conf.scheduling_target is None:
        cluster_conf.scheduling_target = _default_scheduling_target(cluster_conf.size)
    return cluster_conf


def create_cluster(spark_cluster_client, cluster_conf: models.ClusterConfiguration, wait: bool = False):
    """
    Create a new aztk spark cluster

    Args:
        cluster_conf(aztk.spark.models.models.ClusterConfiguration): Configuration for the the cluster to be created
        wait(bool): If you should wait for the cluster to be ready before returning

    Returns:
        aztk.spark.models.Cluster
    """
    cluster_conf = _apply_default_for_cluster_config(cluster_conf)
    cluster_conf.validate()

    cluster_data = spark_cluster_client._get_cluster_data(cluster_conf.cluster_id)
    try:
        zip_resource_files = None
        node_data = NodeData(cluster_conf).add_core().done()
        zip_resource_files = cluster_data.upload_node_data(node_data).to_resource_file()

        start_task = spark_cluster_client.__generate_cluster_start_task(spark_cluster_client, zip_resource_files, cluster_conf.cluster_id,
                                                 cluster_conf.gpu_enabled(), cluster_conf.get_docker_repo(),
                                                 cluster_conf.file_shares, cluster_conf.plugins,
                                                 cluster_conf.mixed_mode(), cluster_conf.worker_on_master)

        software_metadata_key = "spark"

        vm_image = models.VmImage(publisher='Canonical', offer='UbuntuServer', sku='16.04')

        cluster = spark_cluster_client.create_pool_and_job(cluster_conf, software_metadata_key, start_task, vm_image)

        # Wait for the master to be ready
        if wait:
            util.wait_for_master_to_be_ready(spark_cluster_client, cluster.id)
            cluster = spark_cluster_client.get_cluster(cluster.id)

        return cluster

    except batch_error.BatchErrorException as e:
        raise error.AztkError(helpers.format_batch_exception(e))
