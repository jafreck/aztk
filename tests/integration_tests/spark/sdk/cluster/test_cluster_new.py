import os
import subprocess
import time
from datetime import datetime
from zipfile import ZipFile

import azure.batch.models as batch_models
import pytest
from azure.batch.models import BatchErrorException

import aztk.spark
from aztk.error import AztkError
from aztk.utils import constants
from aztk_cli import config
from tests.integration_tests.spark.sdk.get_client import get_spark_client, get_test_suffix

base_cluster_id = get_test_suffix("cluster")
spark_client = get_spark_client()


def clean_up_cluster(cluster_id):
    try:
        spark_client.cluster.delete(cluster_id=cluster_id)
    except (BatchErrorException, AztkError):
        # pass in the event that the cluster does not exist
        pass


def ensure_spark_master(cluster_id):
    results = spark_client.cluster.run(cluster_id,
                "if $AZTK_IS_MASTER ; then $SPARK_HOME/sbin/spark-daemon.sh status org.apache.spark.deploy.master.Master 1 ;" \
                " else echo AZTK_IS_MASTER is false ; fi")
    for _, result in results:
        if isinstance(result, Exception):
            raise result
        print(result[0])
        assert result[0] in ["org.apache.spark.deploy.master.Master is running.", "AZTK_IS_MASTER is false"]


def ensure_spark_worker(cluster_id):
    results = spark_client.cluster.run(cluster_id,
                "if $AZTK_IS_WORKER ; then $SPARK_HOME/sbin/spark-daemon.sh status org.apache.spark.deploy.worker.Worker 1 ;" \
                " else echo AZTK_IS_WORKER is false ; fi")
    for _, result in results:
        if isinstance(result, Exception):
            raise result
        assert result[0] in ["org.apache.spark.deploy.worker.Worker is running.", "AZTK_IS_WORKER is false"]


def ensure_spark_processes(cluster_id):
    ensure_spark_master(cluster_id)
    ensure_spark_worker(cluster_id)


def wait_for_all_nodes(cluster_id, nodes):
    while True:
        for node in nodes:
            if node.state not in [batch_models.ComputeNodeState.idle, batch_models.ComputeNodeState.running]:
                break
        else:
            nodes = spark_client.cluster.get(cluster_id).nodes
            continue
        break


def test_create_cluster():
    test_id = "test-create-"
    # TODO: make Cluster Configuration more robust, test each value
    cluster_configuration = aztk.spark.models.ClusterConfiguration(
        cluster_id=test_id + base_cluster_id,
        vm_count=2,
        vm_low_pri_count=0,
        vm_size="standard_f2",
        subnet_id=None,
        custom_scripts=None,
        file_shares=None,
        toolkit=aztk.spark.models.SparkToolkit(version="2.3.0"),
        spark_configuration=None)
    try:
        cluster = spark_client.cluster.create(cluster_configuration, wait=True)

        assert cluster.pool is not None
        assert cluster.nodes is not None
        assert cluster.id == cluster_configuration.cluster_id
        assert cluster.vm_size == "standard_f2"
        assert cluster.current_dedicated_nodes == 2
        assert cluster.gpu_enabled is False
        assert cluster.master_node_id is not None
        assert cluster.current_low_pri_nodes == 0

    except (AztkError, BatchErrorException) as e:
        assert False

    finally:
        clean_up_cluster(cluster_configuration.cluster_id)
