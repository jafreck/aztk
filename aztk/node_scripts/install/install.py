import os

from aztk.internal import cluster_data
from aztk.models.plugins import PluginTarget
from aztk.node_scripts import wait_until_master_selected
from aztk.node_scripts.core import config, log
from aztk.node_scripts.install import (create_user, pick_master, plugins, spark, spark_container)


def read_cluster_config():
    data = cluster_data.ClusterData(config.block_blob_service, config.cluster_id)
    cluster_config = data.read_cluster_config()
    log.info("Got cluster config: %s", cluster_config)
    return cluster_config


def setup_host(docker_repo: str, docker_run_options: str):
    """
    Code to be run on the node (NOT in a container)
    :param docker_repo: location of the Docker image to use
    :param docker_run_options: additional command-line options to pass to docker run
    """
    create_user.create_user(batch_client=config.batch_client)
    if os.environ["AZ_BATCH_NODE_IS_DEDICATED"] == "true" or os.environ["AZTK_MIXED_MODE"] == "false":
        is_master = pick_master.find_master(config.batch_client)
    else:
        is_master = False
        wait_until_master_selected.main()

    is_worker = not is_master or os.environ.get("AZTK_WORKER_ON_MASTER") == "true"

    cluster = config.spark_client.cluster.get(id=config.cluster_id)
    master_node = config.batch_client.compute_node.get(config.pool_id, cluster.master_node_id)

    os.environ["AZTK_IS_MASTER"] = "true" if is_master else "false"
    os.environ["AZTK_IS_WORKER"] = "true" if is_worker else "false"

    os.environ["AZTK_MASTER_IP"] = master_node.ip_address

    cluster_conf = read_cluster_config()

    # TODO pass azure file shares
    spark_container.start_spark_container(
        docker_repo=docker_repo,
        docker_run_options=docker_run_options,
        gpu_enabled=os.environ.get("AZTK_GPU_ENABLED") == "true",
        plugins=cluster_conf.plugins,
    )
    plugins.setup_plugins(target=PluginTarget.Host, is_master=is_master, is_worker=is_worker)


def setup_spark_container():
    """
    Code run in the main spark container
    """
    is_master = os.environ.get("AZTK_IS_MASTER") == "true"
    is_worker = os.environ.get("AZTK_IS_WORKER") == "true"
    log.info("Setting spark container. Master: %s, Worker: %s", is_master, is_worker)

    log.info("Copying spark setup config")
    spark.setup_conf()
    log.info("Done copying spark setup config")

    spark.setup_connection()

    if is_master:
        spark.start_spark_master()

    if is_worker:
        spark.start_spark_worker()

    plugins.setup_plugins(target=PluginTarget.SparkContainer, is_master=is_master, is_worker=is_worker)

    # TODO: this is a good candidate for a lock.
    #       this function holds lock until completion,
    #       poller wait to aquire lock
    open("/tmp/setup_complete", "a").close()
