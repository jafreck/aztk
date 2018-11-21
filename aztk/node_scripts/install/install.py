import os

from aztk.internal import cluster_data
from aztk.models.plugins import PluginTarget
from aztk.node_scripts import wait_until_master_selected
from aztk.node_scripts.core import config
from aztk.node_scripts.install import (create_user, pick_master, plugins, spark, spark_container)


def read_cluster_config():
    data = cluster_data.ClusterData(config.blob_client, config.cluster_id)
    cluster_config = data.read_cluster_config()
    print("Got cluster config", cluster_config)
    return cluster_config


def setup_host(docker_repo: str, docker_run_options: str):
    """
    Code to be run on the node (NOT in a container)
    :param docker_repo: location of the Docker image to use
    :param docker_run_options: additional command-line options to pass to docker run
    """
    client = config.batch_client

    create_user.create_user(batch_client=client)
    if config.is_dedicated or config.mixed_mode == "false":
        is_master = pick_master.find_master(client)
    else:
        is_master = False
        wait_until_master_selected.main()

    is_worker = not is_master or config.worker_on_master
    master_node_id = pick_master.get_master_node_id(config.batch_client.pool.get(config.pool_id))
    master_node = config.batch_client.compute_node.get(config.pool_id, master_node_id)

    os.environ["AZTK_IS_MASTER"] = "true" if is_master else "false"
    os.environ["AZTK_IS_WORKER"] = "true" if is_worker else "false"

    os.environ["AZTK_MASTER_IP"] = master_node.ip_address

    cluster_conf = read_cluster_config()

    # TODO pass azure file shares
    spark_container.start_spark_container(
        docker_repo=docker_repo,
        docker_run_options=docker_run_options,
        gpu_enabled=config.gpu_enabled,
        plugins=cluster_conf.plugins,
    )
    plugins.setup_plugins(target=PluginTarget.Host, is_master=config.is_master, is_worker=config.is_worker)


def setup_spark_container():
    """
    Code run in the main spark container
    """

    print("Setting spark container. Master: ", config.is_master, ", Worker: ", config.is_worker)

    print("Copying spark setup config")
    spark.setup_conf()
    print("Done copying spark setup config")

    spark.setup_connection()

    if config.is_master:
        spark.start_spark_master()

    if config.is_worker:
        spark.start_spark_worker()

    plugins.setup_plugins(target=PluginTarget.SparkContainer, is_master=config.is_master, is_worker=config.is_worker)

    open("/tmp/setup_complete", "a").close()
