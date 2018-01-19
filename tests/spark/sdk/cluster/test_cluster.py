import subprocess
import aztk.spark
from cli import config
from datetime import datetime
import pytest
import azure.batch.models as batch_models
from azure.batch.models import BatchErrorException
from aztk.error import AztkError

dt = datetime.now()
time = dt.microsecond
cluster_id = "test{}".format(time)

# load secrets
# note: this assumes secrets are set up in .aztk/secrets
spark_client = aztk.spark.Client(config.load_aztk_screts())



def test_create_cluster():

    # TODO: make Cluster Configuration more robust, test each value
    cluster_configuration = aztk.spark.models.ClusterConfiguration(
        cluster_id=cluster_id,
        vm_count=2,
        vm_low_pri_count=0,
        vm_size="standard_f2",
        subnet_id=None,
        custom_scripts=None,
        file_shares=None,
        docker_repo=None,
        spark_configuration=None
    )
    try:
        cluster = spark_client.create_cluster(cluster_configuration, wait=True)
    except (AztkError, BatchErrorException) as e:
        assert False

    assert cluster.pool is not None
    assert cluster.nodes is not None
    assert cluster.id == cluster_id
    assert cluster.vm_size == "standard_f2"
    assert cluster.current_dedicated_nodes == 2
    assert cluster.gpu_enabled is False
    assert cluster.master_node_id is not None
    assert cluster.current_low_pri_nodes == 0

def test_get_cluster():
    try:
        cluster = spark_client.get_cluster(cluster_id=cluster_id)
    except (AztkError, BatchErrorException) as e:
        assert False

    assert cluster.pool is not None
    assert cluster.nodes is not None
    assert cluster.id == cluster_id
    assert cluster.vm_size == "standard_f2"
    assert cluster.current_dedicated_nodes == 2
    assert cluster.gpu_enabled is False
    assert cluster.master_node_id is not None
    assert cluster.current_low_pri_nodes == 0

def test_list_clusters():
    try:
        clusters = spark_client.list_clusters()
    except (AztkError, BatchErrorException) as e:
        assert False
    assert cluster_id in [cluster.id for cluster in clusters]

def test_get_remote_login_settings():
    try:
        cluster = spark_client.get_cluster(cluster_id=cluster_id)
        rls = spark_client.get_remote_login_settings(cluster_id=cluster.id, node_id=cluster.master_node_id)
    except (AztkError, BatchErrorException) as e:
        assert False

    assert rls.ip_address is not None
    assert rls.port is not None

def test_submit():
    try:
        application_configuration = aztk.spark.models.ApplicationConfiguration(
            name="pipy100",
            application="examples/src/main/python/pi.py",
            application_args=[100],
            main_class=None,
            jars=[],
            py_files=[],
            files=[],
            driver_java_options=None,
            driver_class_path=None,
            driver_memory=None,
            driver_cores=None,
            executor_memory=None,
            executor_cores=None,
            max_retry_count=None
        )
        spark_client.submit(cluster_id=cluster_id, application=application_configuration, wait=True)
    except (AztkError, BatchErrorException) as e:
        assert False
    
    assert True

def test_get_application_log():
    try:
        application_log = spark_client.get_application_log(cluster_id=cluster_id,
                                                           application_name="pipy100",
                                                           tail=False,
                                                           current_bytes=0)
    except (AztkError, BatchErrorException) as e:
        assert False

    assert application_log.exit_code == 0
    assert application_log.name == "pipy100"
    assert application_log.state == "completed"
    assert application_log.log is not None
    assert application_log.total_bytes is not None

def test_create_user():
    pass

def test_get_application_status():
    pass

def test_delete_cluster():
    try:
        success = spark_client.delete_cluster(cluster_id=cluster_id)
    except (AztkError, BatchErrorException) as e:
        assert False

    assert success is True